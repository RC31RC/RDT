package io.github.rdt.console.archive;

import io.github.rdt.exception.RdtException;
import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.Message;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class RedisArchiveReaderIT {
    @TempDir
    Path directory;
    private final JsonCodec json = new JsonCodec();
    private final Instant at = Instant.parse("2026-10-19T02:00:00Z");
    private final KeySpace keys = new KeySpace("archive-it:" + UUID.randomUUID());
    private final KeySpace other = new KeySpace(keys.namespace() + ":other");

    @Test
    void archivesOnlyAnomaliesInBoundedBatchesWithoutChangingRedisOrIdleExpiry() throws IOException {
        RedissonClient writer = client(false);
        try {
            seed(writer);
        } finally {
            writer.shutdown(); // Stop writer-owned MapCache eviction before the read-only assertions.
        }
        RedissonClient client = client(true);
        try {
            Map<String, String> before = fingerprints(client);
            var reader = new RedisArchiveReader(client, json, List.of(keys.namespace()), Duration.ofMinutes(10));
            var sizes = new ArrayList<Integer>();
            var records = new ArrayList<Map<String, Object>>();
            var sink = new LocalFileArchiveSink(directory, LocalDate.of(2026, 10, 19), 30);
            long count;
            try {
                count = reader.read(at, batch -> {
                    sizes.add(batch.size());
                    sink.write(batch);
                    for (String value : batch) records.add(json.read(value, Map.class));
                });
                sink.close();
            } catch (RuntimeException failure) {
                sink.abort();
                throw failure;
            }
            assertThat(count).isEqualTo(1208);
            assertThat(sizes).containsExactly(500, 500, 208);
            assertThat(records).allSatisfy(record -> {
                assertThat(record.get("namespace")).isEqualTo(keys.namespace());
                assertThat(record.get("version")).isEqualTo(1);
                assertThat(record.get("data")).isInstanceOf(Map.class);
            });
            assertThat(records.stream().filter(record -> record.get("kind").equals("DEAD_LETTER"))).hasSize(1203);
            assertThat(records.stream().filter(record -> !record.get("kind").equals("DEAD_LETTER"))
                    .map(record -> record.get("recordId")))
                    .containsExactlyInAnyOrder("failed", "stalled", "current-only", "duplicate", "failed-only");
            assertThat(records.toString()).doesNotContain("worker-secret-token", "expired-ttl", "expired-idle", "stale-history");
            assertThat(fingerprints(client)).isEqualTo(before);
            Path target = directory.resolve("rdt-archive-2026-10-19.jsonl.gz");
            try (var input = new GZIPInputStream(Files.newInputStream(target))) {
                assertThat(new String(input.readAllBytes(), StandardCharsets.UTF_8).lines().count()).isEqualTo(count);
            }
            var all = new ArrayList<Map<String, Object>>();
            new RedisArchiveReader(client, json, List.of(), Duration.ofMinutes(10))
                    .read(at, batch -> batch.forEach(value -> all.add(json.read(value, Map.class))));
            assertThat(all).anySatisfy(record -> assertThat(record.get("namespace")).isEqualTo(other.namespace()));
            assertThat(all.toString()).doesNotContain("invalid-key-marker");
        } finally {
            client.shutdown();
            cleanup();
        }
    }

    @Test
    void malformedRecordFailsTheRunWithoutPublishingOrPurgingOldFiles() throws IOException {
        RedissonClient writer = client(false);
        writer.<String, String>getMapCache(keys.key("dead-letters", "bad"), StringCodec.INSTANCE).put("bad", "{invalid");
        writer.shutdown();
        RedissonClient client = client(true);
        try {
            Path old = Files.writeString(directory.resolve("rdt-archive-2025-01-01.jsonl.gz"), "preserve");
            var reader = new RedisArchiveReader(client, json, List.of(keys.namespace()), Duration.ofMinutes(10));
            assertThatThrownBy(() -> reader.read(at, batch -> { })).isInstanceOf(RdtException.class)
                    .hasMessageContaining("Cannot decode payload");
            var factory = new RdtArchiveConfiguration().localFileArchiveSinkFactory(new RdtArchiveProperties(
                    "local", directory, "0 0 2 * * ?", ZoneOffset.UTC, Duration.ofMinutes(10), 30));
            new RdtArchiveJob(reader, factory, Clock.fixed(at, ZoneOffset.UTC)).archive();
            try (var paths = Files.list(directory)) { assertThat(paths.toList()).containsExactly(old); }
            assertThat(client.getKeys().countExists(keys.key("dead-letters", "bad"))).isEqualTo(1);
        } finally {
            client.shutdown();
            cleanup();
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "rdt.it.archive.readonly-user", matches = ".+")
    void archiveWorksWithAclThatDeniesAllRedisWrites() {
        RedissonClient writer = client(false);
        seedSmall(writer);
        writer.shutdown();
        RedissonClient client = client(true);
        try {
            assertThatThrownBy(() -> client.getBucket(keys.key("forbidden", "write")).set("denied"))
                    .hasMessageContaining("NOPERM");
            var records = new ArrayList<String>();
            long count = new RedisArchiveReader(client, json, List.of(keys.namespace()), Duration.ofMinutes(10))
                    .read(at, records::addAll);
            assertThat(count).isEqualTo(3);
            assertThat(records).hasSize(3);
        } finally {
            client.shutdown();
            cleanup();
        }
    }

    private void seed(RedissonClient client) {
        String main = keys.key("dead-letters", "main");
        var cache = client.<String, String>getMapCache(main, StringCodec.INSTANCE);
        for (int i = 0; i < 702; i++) {
            cache.fastPut("m" + i, failure("m" + i), 1, TimeUnit.DAYS, 1, TimeUnit.DAYS);
        }
        for (int i = 0; i < 501; i++) {
            client.<String, String>getMapCache(keys.key("dead-letters", "topic" + i), StringCodec.INSTANCE)
                    .fastPut("m", failure("topic" + i), 1, TimeUnit.DAYS);
        }
        cache.fastPut("expired-ttl", failure("expired-ttl"), 1, TimeUnit.DAYS);
        cache.fastPut("expired-idle", failure("expired-idle"), 1, TimeUnit.DAYS, 1, TimeUnit.DAYS);
        client.getScoredSortedSet("redisson__timeout__set:" + main, StringCodec.INSTANCE).add(1, "expired-ttl");
        client.getScoredSortedSet("redisson__idle__set:" + main, StringCodec.INSTANCE).add(1, "expired-idle");
        long now = at.toEpochMilli();
        history(client, "history", "failed", "FAILED", now, "failure");
        history(client, "history", "stalled", "DISPATCHED", now - 600001, "stuck");
        history(client, "history", "boundary", "DISPATCHED", now - 600000, "excluded");
        history(client, "history", "recent", "DISPATCHED", now - 599999, "excluded");
        for (String status : List.of("RUNNING", "SUCCEEDED", "CANCELLED", "READY", "WAITING")) {
            history(client, "history", status, status, now - 3600000, "excluded");
        }
        state(client, "only", "current-only", "DISPATCHED", now - 600001, "current");
        state(client, "failed-only", "failed-only", "FAILED", now, "current");
        history(client, "duplicate", "duplicate", "FAILED", now, "stale-history");
        state(client, "duplicate", "duplicate", "FAILED", now + 1, "authoritative");
        history(client, "recovered", "recovered", "FAILED", now, "stale-history");
        state(client, "recovered", "recovered", "SUCCEEDED", now + 1, "recovered");
        client.<String, String>getMapCache(other.key("dead-letters", "other"), StringCodec.INSTANCE)
                .put("other", failure("other"));
        String fake = keys.key("dead-letters", "fake").replace("{rdt-", "{invalid-");
        client.<String, String>getMap(fake, StringCodec.INSTANCE).put("bad", "invalid-key-marker");
        client.<String, String>getMap(keys.key("queue", "unrelated"), StringCodec.INSTANCE).put("bad", "invalid-key-marker");
    }

    private void seedSmall(RedissonClient client) {
        client.<String, String>getMapCache(keys.key("dead-letters", "small"), StringCodec.INSTANCE)
                .fastPut("small", failure("small"), 1, TimeUnit.DAYS, 1, TimeUnit.DAYS);
        history(client, "small", "failed", "FAILED", at.toEpochMilli(), "failed");
        state(client, "small", "stuck", "DISPATCHED", at.toEpochMilli() - 600001, "stuck");
    }

    private String failure(String id) {
        return json.write(FailedMessage.stream("destination", "group", Message.create(Map.of("id", id, "text", "quote\"\n"), json),
                3, "downstream unavailable", at.toString()));
    }

    private void history(RedissonClient client, String job, String id, String status, long updated, String detail) {
        client.<String, String>getMap(keys.key("schedule-executions", job), StringCodec.INSTANCE)
                .fastPut(id, execution(job, id, status, updated, detail));
        client.<String>getScoredSortedSet(keys.key("schedule-execution-expiry", job), StringCodec.INSTANCE)
                .add(System.currentTimeMillis() + Duration.ofDays(1).toMillis(), id);
    }
    private void state(RedissonClient client, String job, String id, String status, long updated, String detail) {
        client.<String, String>getMap(keys.key("schedule-state", job), StringCodec.INSTANCE)
                .put("execution", execution(job, id, status, updated, detail));
    }
    private String execution(String job, String id, String status, long updated, String detail) {
        return json.write(new JobExecution(id, job, updated, 1, status, "worker", "run", "worker-secret-token", updated, 0, detail, 0));
    }

    private Map<String, String> fingerprints(RedissonClient client) {
        Map<String, String> result = new HashMap<>();
        for (String key : client.getKeys().getKeysByPattern("*rdt:" + RdtSupport.component(keys.namespace()) + ":*", 500)) {
            String value = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY,
                    "return redis.sha1hex(redis.call('dump', KEYS[1]) or '')", RScript.ReturnType.VALUE, List.of(key));
            result.put(key, value);
        }
        return result;
    }

    private RedissonClient client(boolean readOnly) {
        Config config = new Config().setThreads(2).setNettyThreads(2);
        var server = config.useSingleServer().setAddress(System.getProperty("rdt.it.redis"))
                .setConnectionMinimumIdleSize(1).setConnectionPoolSize(4)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(1);
        if (readOnly && System.getProperty("rdt.it.archive.readonly-user") != null) {
            server.setUsername(System.getProperty("rdt.it.archive.readonly-user"))
                    .setPassword(System.getProperty("rdt.it.archive.readonly-password"));
        } else {
            server.setPassword(System.getenv("REDIS_PASSWORD"));
        }
        return Redisson.create(config);
    }

    private void cleanup() {
        RedissonClient client = client(false);
        try {
            for (KeySpace namespace : List.of(keys, other)) {
                client.getKeys().deleteByPattern("*rdt:" + RdtSupport.component(namespace.namespace()) + ":*");
            }
        } finally { client.shutdown(); }
    }
}
