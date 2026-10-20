package io.github.rdt.redisson.queue;

import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.support.KeySpace;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded server-side scans; no message bodies cross the wire during cleanup. */
final class StreamRetention {
    private static final Logger log = LoggerFactory.getLogger(StreamRetention.class);
    private static final int SWEEP_BATCH_SIZE = 64;
    private final QueuePolicy policy;
    private final RedissonClient client;
    private final KeySpace keys;
    private final Map<String, CleanupRequest> dirty = new ConcurrentHashMap<>();
    private record CleanupRequest(RStream<String, String> stream, boolean acknowledged) {}
    private static final String SWEEP = """
            if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
            local function less(a, b)
              local am, ax = string.match(a, '(%d+)%-(%d+)')
              local bm, bx = string.match(b, '(%d+)%-(%d+)')
              if #am ~= #bm then return #am < #bm end
              if am ~= bm then return am < bm end
              if #ax ~= #bx then return #ax < #bx end
              return ax < bx
            end
            local cursor = redis.call('GET', KEYS[2]) or '-'
            local rows = redis.call('XRANGE', KEYS[1], cursor, '+', 'COUNT', 64)
            if #rows == 0 then redis.call('DEL', KEYS[2]); return 0 end
            local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
            if #groups == 0 then redis.call('DEL', KEYS[2]); return -1 end
            local watermarks = {}
            for _, fields in ipairs(groups) do
              local name; local last
              for i = 1, #fields, 2 do
                if fields[i] == 'name' then name = fields[i+1] end
                if fields[i] == 'last-delivered-id' then last = fields[i+1] end
              end
              local pendingIds = {}
              if not less(last, rows[1][1]) then
                local pending = redis.call('XPENDING', KEYS[1], name,
                    rows[1][1], rows[#rows][1], #rows)
                for _, entry in ipairs(pending) do pendingIds[entry[1]] = true end
              end
              table.insert(watermarks, {last, pendingIds})
            end
            local size = redis.call('XLEN', KEYS[1])
            local t = redis.call('TIME'); local now = t[1]*1000+math.floor(t[2]/1000)
            local remaining = math.max(0, size-tonumber(ARGV[2]))
            local released = 0
            local removed = 0
            for _, row in ipairs(rows) do
              local id = row[1]
              local eligible = ARGV[1] == 'true'
                or (tonumber(ARGV[2]) > 0 and remaining > 0)
                or (tonumber(ARGV[3]) > 0 and tonumber(string.match(id, '^(%d+)')) < now-tonumber(ARGV[3]))
              if eligible then
                for _, group in ipairs(watermarks) do
                  if less(group[1], id) or group[2][id] then
                    eligible = false; break
                  end
                end
              end
              if eligible then
                for i = 1, #row[2], 2 do
                  if row[2][i] == 'message' then released = released + string.len(row[2][i+1]) end
                end
                redis.call('XDEL', KEYS[1], id)
                remaining = remaining - 1
                removed = removed + 1
              end
            end
            if released > 0 then
              local bytes = tonumber(redis.call('GET', KEYS[3]) or '0')
              if bytes <= released then redis.call('DEL', KEYS[3])
              else redis.call('DECRBY', KEYS[3], released) end
            end
            if removed > 0 then redis.call('HINCRBY', KEYS[4], 'stream', -removed) end
            if #rows < 64 then redis.call('DEL', KEYS[2])
            else redis.call('SET', KEYS[2], '('..rows[#rows][1], 'PX', 3600000) end
            return #rows
            """;

    StreamRetention(QueuePolicy policy, RedissonClient client, KeySpace keys) {
        this.policy = policy;
        this.client = client;
        this.keys = keys;
    }

    /** ACK and diagnostics are constant-work; group-wide deletion checks run in bounded maintenance batches. */
    void acknowledge(QueuePolicy effective, RStream<String, String> stream, String group,
                     String id, List<String> diagnostics) {
        String destination = keys.nameFromKey("stream", stream.getName()).orElseThrow();
        List<Object> scriptKeys = new ArrayList<>();
        scriptKeys.add(stream.getName());
        scriptKeys.add(keys.key("queue-capacity", destination));
        scriptKeys.add(keys.key("queue-capacity-state", destination));
        scriptKeys.addAll(diagnostics);
        Number result = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
                redis.call('ZREM', KEYS[4], ARGV[2])
                for i = 5, #KEYS do redis.call('DEL', KEYS[i]) end
                if ARGV[3] ~= 'true' then return 0 end
                return acked
                """, RScript.ReturnType.LONG, scriptKeys,
                group, id, Boolean.toString(effective.autoDeleteAfterAck()));
        if (result.longValue() > 0) markAcknowledged(effective, stream);
        if (!effective.autoDeleteAfterAck()) trimStream(stream, destination);
    }

    void markAcknowledged(QueuePolicy effective, RStream<String, String> stream) {
        if (effective.autoDeleteAfterAck()) dirty.put(stream.getName(), new CleanupRequest(stream, true));
    }

    void sweepAcknowledged() {
        dirty.forEach((name, request) -> sweep(name, request));
    }

    void trimStream(RStream<String, String> stream, String destination) {
        if (policy.autoDeleteAfterAck() || policy.streamMaxLength() > 0 || !policy.streamRetention().isZero())
            dirty.compute(stream.getName(), (name, previous) -> new CleanupRequest(stream, previous != null && previous.acknowledged()));
    }

    private void sweep(String name, CleanupRequest request) {
        RStream<String, String> stream = request.stream();
        String destination = keys.nameFromKey("stream", stream.getName()).orElseThrow();
        long deadline = System.nanoTime() + 100_000_000L;
        try {
            // Atomic cursor advancement permits cooperative cleanup on multiple instances.
            for (int batch = 0; batch < 64; batch++) {
                Number scanned = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,
                        SWEEP, RScript.ReturnType.LONG,
                        List.of(stream.getName(), keys.key("stream-cleanup-cursor", destination),
                                keys.key("queue-capacity", destination),
                                keys.key("queue-capacity-state", destination)),
                        Boolean.toString(request.acknowledged()),
                        policy.streamMaxLength(), policy.streamRetention().toMillis());
                if (scanned.intValue() < 0 || (scanned.intValue() == 0 && stream.size() == 0))
                    dirty.remove(name, request);
                if (scanned.intValue() < SWEEP_BATCH_SIZE || System.nanoTime() >= deadline) break;
            }
        } catch (RuntimeException error) {
            log.warn("Stream cleanup failed stream={}", stream.getName(), error);
        }
    }
}
