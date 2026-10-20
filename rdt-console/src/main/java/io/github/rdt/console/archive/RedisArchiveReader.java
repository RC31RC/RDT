package io.github.rdt.console.archive;

import io.github.rdt.queue.FailedMessage;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.KeysScanOptions;
import org.redisson.client.codec.StringCodec;

/** Read-only SCAN/HSCAN over the current RDT key schema, with no management-index pruning. */
public final class RedisArchiveReader implements RdtArchiveJob.Source {
    public static final int BATCH_SIZE = 500;
    // Redisson 4.7 MapCache packs values as dLc0 and stores entry expiry in two sorted sets.
    // Using getMapCache() would register an eviction task; normal reads can also refresh idle TTL.
    private static final String SCAN_MAP_CACHE = """
            local page = redis.call('hscan', KEYS[1], ARGV[1], 'count', ARGV[2])
            local result = {page[1]}
            local time = redis.call('time')
            local now = time[1] * 1000 + math.floor(time[2] / 1000)
            for i = 1, #page[2], 2 do
                local field = page[2][i]
                local idle, value = struct.unpack('dLc0', page[2][i + 1])
                local ttl = redis.call('zscore', KEYS[2], field)
                local idleTtl = false
                if idle ~= 0 then idleTtl = redis.call('zscore', KEYS[3], field) end
                if (ttl == false or tonumber(ttl) > now)
                        and (idleTtl == false or tonumber(idleTtl) > now) then
                    table.insert(result, field)
                    table.insert(result, value)
                end
            end
            return result
            """;
    private final RedissonClient client;
    private final JsonCodec json;
    private final Set<String> namespaces;
    private final Duration dispatchedTimeout;

    public RedisArchiveReader(RedissonClient client, JsonCodec json, List<String> namespaces,
                              Duration dispatchedTimeout) {
        this.client = client;
        this.json = json;
        this.namespaces = Set.copyOf(namespaces);
        this.dispatchedTimeout = dispatchedTimeout;
    }

    public long read(Instant startedAt, Consumer<List<String>> destination) {
        Batch batch = new Batch(destination);
        scan("dead-letters", (key, namespace, name) -> scanCache(key, (field, value) -> {
            FailedMessage failure = json.read(value, FailedMessage.class);
            batch.add(new ArchiveRecord(1, startedAt, "DEAD_LETTER", namespace, key, field, failure));
        }));
        scan("schedule-executions", (key, namespace, jobId) -> {
            KeySpace keys = new KeySpace(namespace);
            JobExecution current = currentExecution(keys.key("schedule-state", jobId));
            scanHistory(key, keys.key("schedule-execution-expiry", jobId), (field, value) -> {
                JobExecution execution = json.read(value, JobExecution.class);
                // Current state is authoritative; its separate scan also covers a crash before history persistence.
                if (current == null || !current.executionId().equals(execution.executionId())) {
                    addExecution(batch, startedAt, namespace, key, execution);
                }
            });
        });
        scan("schedule-state", (key, namespace, jobId) -> {
            JobExecution execution = currentExecution(key);
            if (execution != null) addExecution(batch, startedAt, namespace, key, execution);
        });
        batch.flush();
        return batch.count;
    }

    private JobExecution currentExecution(String key) {
        String raw = client.<String, String>getMap(key, StringCodec.INSTANCE).get("execution");
        return raw == null ? null : json.read(raw, JobExecution.class);
    }

    private void addExecution(Batch batch, Instant at, String namespace, String key, JobExecution execution) {
        String kind;
        if ("FAILED".equals(execution.status())) {
            kind = "FAILED_EXECUTION";
        } else if ("DISPATCHED".equals(execution.status())
                && execution.updatedAt() < at.minus(dispatchedTimeout).toEpochMilli()) {
            kind = "STALLED_EXECUTION";
        } else {
            return;
        }
        // Dispatch tokens authorize a worker attempt; diagnosis does not need to export them.
        var data = new ArchivedExecution(execution.executionId(), execution.jobId(), execution.scheduledAt(),
                execution.attempt(), execution.status(), execution.instanceId(), execution.runId(),
                execution.updatedAt(), execution.retryAt(), execution.detail());
        batch.add(new ArchiveRecord(1, at, kind, namespace, key, execution.executionId(), data));
    }

    private void scan(String feature, KeyConsumer consumer) {
        if (namespaces.isEmpty()) {
            scanPattern("rdt:*:" + feature + ":*", feature, consumer);
        } else {
            for (String namespace : namespaces) {
                scanPattern(new KeySpace(namespace).pattern(feature), feature, consumer);
            }
        }
    }

    private void scanPattern(String pattern, String feature, KeyConsumer consumer) {
        var scan = KeysScanOptions.defaults().pattern(pattern).chunkSize(BATCH_SIZE);
        for (String key : client.getKeys().getKeys(scan)) {
            checkInterrupted();
            String[] parts = key.split(":", 5);
            if (parts.length != 5 || !parts[0].equals("rdt")) continue;
            String namespace = RdtSupport.decodeComponent(parts[1]).orElse(null);
            if (namespace == null || (!namespaces.isEmpty() && !namespaces.contains(namespace))) continue;
            KeySpace keys = new KeySpace(namespace);
            String name = keys.nameFromKey(feature, key).orElse(null);
            if (name != null && keys.key(feature, name).equals(key)) consumer.accept(key, namespace, name);
        }
    }

    private void scanHistory(String key, String expiry, RecordConsumer consumer) {
        String cursor = "0";
        do {
            checkInterrupted();
            List<Object> page = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY, """
                    local page = redis.call('HSCAN', KEYS[1], ARGV[1], 'COUNT', ARGV[2])
                    local result = {page[1]}
                    local t = redis.call('TIME'); local now = t[1]*1000+math.floor(t[2]/1000)
                    for i = 1, #page[2], 2 do
                      local expires = redis.call('ZSCORE', KEYS[2], page[2][i])
                      if expires and tonumber(expires) > now then
                        table.insert(result, page[2][i]); table.insert(result, page[2][i+1])
                      end
                    end
                    return result
                    """, RScript.ReturnType.LIST, List.of(key, expiry), cursor, BATCH_SIZE);
            cursor = (String) page.getFirst();
            for (int i = 1; i < page.size(); i += 2)
                consumer.accept((String) page.get(i), (String) page.get(i+1));
        } while (!"0".equals(cursor));
    }

    private void scanCache(String key, RecordConsumer consumer) {
        String cursor = "0";
        do {
            checkInterrupted();
            List<Object> page = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY,
                    SCAN_MAP_CACHE, RScript.ReturnType.LIST,
                    List.of(key, "redisson__timeout__set:" + key, "redisson__idle__set:" + key),
                    cursor, BATCH_SIZE);
            cursor = (String) page.getFirst();
            for (int i = 1; i < page.size(); i += 2) {
                consumer.accept((String) page.get(i), (String) page.get(i + 1));
            }
        } while (!"0".equals(cursor));
    }

    private static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Archive was interrupted");
    }

    private final class Batch {
        private final Consumer<List<String>> destination;
        private final List<String> records = new ArrayList<>(BATCH_SIZE);
        private long count;
        private Batch(Consumer<List<String>> destination) { this.destination = destination; }
        private void add(ArchiveRecord record) {
            checkInterrupted();
            records.add(json.write(record));
            if (records.size() == BATCH_SIZE) flush();
        }
        private void flush() {
            if (records.isEmpty()) return;
            destination.accept(List.copyOf(records));
            count += records.size();
            records.clear();
        }
    }

    @FunctionalInterface
    private interface KeyConsumer { void accept(String key, String namespace, String name); }
    @FunctionalInterface
    private interface RecordConsumer { void accept(String field, String value); }

    private record ArchiveRecord(int version, Instant archivedAt, String kind, String namespace,
                                 String redisKey, String recordId, Object data) { }
    private record ArchivedExecution(String executionId, String jobId, long scheduledAt, int attempt,
                                     String status, String instanceId, String runId, long updatedAt,
                                     long retryAt, String detail) { }
}
