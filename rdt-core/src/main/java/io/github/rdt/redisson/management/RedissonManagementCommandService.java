package io.github.rdt.redisson.management;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.exception.RdtQueueException;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.management.ManagementCommandService;
import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.KeysScanOptions;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamGroup;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.client.codec.StringCodec;

/** 基于既有 RDT 键协议执行受限管理命令。 */
public final class RedissonManagementCommandService implements ManagementCommandService {
    private static final String REDIS_TIME = """
            local t = redis.call('TIME')
            return t[1] * 1000 + math.floor(t[2] / 1000)
            """;

    private final RedissonClient client;
    private final KeySpace keys;
    private final DistributedLockTemplate locks;
    private final JsonCodec json;

    /** 创建带 JSON 死信重投能力的命令服务。 */
    public RedissonManagementCommandService(
            RedissonClient client, KeySpace keys, DistributedLockTemplate locks, JsonCodec json) {
        this.client = client;
        this.keys = keys;
        this.locks = locks;
        this.json = json;
    }

    @Override
    public boolean deleteTaskResults(String taskId) {
        RdtSupport.component(taskId);
        return RedisSupport.redis("delete task results", () -> client.getKeys().delete(
                keys.key("schedule-results", taskId), keys.key("management-index", "attempts:" + taskId),
                keys.key("management-expiry", "attempts:" + taskId),
                keys.key("management-index", "failures:" + taskId),
                keys.key("management-expiry", "failures:" + taskId)) > 0);
    }

    @Override
    public boolean triggerTask(String taskId) {
        RdtSupport.component(taskId);
        try {
            return locks.execute("schedule:" + taskId, LockOptions.defaults(), () -> {
                String key = keys.key("schedule-state", taskId);
                if (client.getKeys().countExists(key) == 0) {
                    return false;
                }
                RMap<String, String> state = client.getMap(key, StringCodec.INSTANCE);
                long now = redisTime();
                String raw = state.get("execution");
                JobExecution current = raw == null ? null : json.read(raw, JobExecution.class);
                if (current != null && !current.terminal()) return false;
                JobExecution execution = new JobExecution("manual-" + UUID.randomUUID(), taskId, now, 0,
                        "READY", "", "", "", now, now, "Manual trigger", 0);
                RScoredSortedSet<String> due = client.getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE);
                state.fastPut("execution", json.write(execution));
                due.add(now, taskId);
                return true;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RdtConfigurationException("Cannot request task trigger", e);
        }
    }

    @Override
    public boolean pauseTask(String taskId, boolean paused) {
        RdtSupport.component(taskId);
        RMap<String, String> state = client.getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE);
        if (!state.containsKey("definition")) return false;
        state.fastPut("paused", Boolean.toString(paused));
        if (!paused) client.<String>getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE).add(0, taskId);
        return true;
    }

    @Override
    public boolean drainInstance(String instanceId, boolean draining) {
        RdtSupport.component(instanceId);
        client.<String>getBucket(keys.key("runtime-draining", instanceId), StringCodec.INSTANCE)
                .set(Boolean.toString(draining));
        return true;
    }

    @Override
    public boolean retryExecution(String taskId, String executionId) {
        RdtSupport.component(taskId);
        try {
            return locks.execute("schedule:" + taskId, LockOptions.defaults(), () -> {
                RMap<String, String> state = client.getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE);
                String raw = state.get("execution");
                if (raw == null) return false;
                JobExecution current = json.read(raw, JobExecution.class);
                if (!current.executionId().equals(executionId) || !current.status().equals("FAILED")) return false;
                long now = redisTime();
                JobExecution retry = new JobExecution("manual-" + UUID.randomUUID(), taskId,
                        current.scheduledAt(), 0, "READY", "", "", "", now, now, "Retry of " + executionId, 0);
                state.fastPut("execution", json.write(retry));
                client.<String>getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE).add(now, taskId);
                return true;
            });
        } catch (RuntimeException error) { throw error; }
        catch (Exception error) { throw new RdtConfigurationException("Cannot retry execution", error); }
    }

    @Override
    public boolean deleteStreamMessage(String destination, String messageId) {
        RdtSupport.component(destination);
        StreamMessageId id = messageId(messageId);
        String key = keys.key("stream", destination);
        if (client.getKeys().countExists(key) == 0) {
            return false;
        }
        RStream<String, String> stream = client.getStream(key, StringCodec.INSTANCE);
        for (StreamGroup group : RedisSupport.redis("read stream groups", stream::listGroups)) {
            RedisSupport.redisRun("acknowledge stream message", () -> stream.ack(group.getName(), id));
        }
        return RedisSupport.redis("delete stream message", () -> stream.remove(id)) > 0;
    }

    @Override
    public boolean deleteDeadLetter(String destination, String group, String messageId, String broadcastInstance) {
        RdtSupport.component(destination);
        RdtSupport.component(group);
        RdtSupport.component(messageId);
        String key = keys.key("dead-letters", destination);
        String field = FailedMessage.field(group, messageId, broadcastInstance);
        RMap<String, String> deadLetters = deadLetters(key);
        boolean removed = RedisSupport.redis("delete dead letter", () -> deadLetters.fastRemove(field)) > 0;
        if (removed) removeDeadLetterIndex(destination, field);
        return removed;
    }

    @Override
    public boolean retryDeadLetter(String destination, String group, String messageId, String broadcastInstance) {
        RdtSupport.component(destination);
        RdtSupport.component(group);
        RdtSupport.component(messageId);
        String key = keys.key("dead-letters", destination);
        String field = FailedMessage.field(group, messageId, broadcastInstance);
        try {
            return locks.execute("dead-letter:" + destination + ":" + group + ":" + messageId, LockOptions.defaults(), () -> {
                String raw = RedisSupport.redis("read dead letter", () -> deadLetters(key).get(field));
                if (raw == null) {
                    return false;
                }
                FailedMessage failure = json.read(raw, FailedMessage.class);
                var limiter = client.getRateLimiter(keys.key("dlq-replay-rate",destination));
                limiter.trySetRate(RateType.OVERALL,10,Duration.ofSeconds(1));
                if (!limiter.tryAcquire()) throw new RdtQueueException("Dead-letter replay rate exceeded; retry later");
                var headers = new HashMap<>(failure.message().headers());
                headers.put("originalMessageId", failure.originalMessageId());
                // A manual replay is a new delivery. Preserve provenance while allowing Inbox to process it.
                var replay = new Message(UUID.randomUUID().toString(), failure.message().payload(),
                        Instant.now().toString(),1,failure.message().key(),null,headers);
                RedisSupport.redisRun("requeue dead letter", () -> {
                    if (failure.mode() == QueueMode.BROADCAST) {
                        client.getTopic(keys.key("topic",destination),StringCodec.INSTANCE).publish(json.write(replay));
                    } else client.<String,String>getStream(keys.key("stream",destination),StringCodec.INSTANCE)
                            .add(StreamAddArgs.entry("message",json.write(replay)));
                });
                boolean removed = RedisSupport.redis("delete retried dead letter", () -> deadLetters(key).fastRemove(field)) > 0;
                if (removed) removeDeadLetterIndex(destination, field);
                new ManagementIndex(client, keys).register("streams", destination);
                return removed;
            });
        } catch (Exception e) {
            if (e instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new RdtConfigurationException("Cannot retry dead letter", e);
        }
    }

    private void removeDeadLetterIndex(String destination, String field) {
        new ManagementIndex(client, keys).remove("dead-letters", json.write(new String[]{destination, field}));
    }

    private RMapCache<String, String> deadLetters(String key) {
        return client.getMapCache(key, StringCodec.INSTANCE);
    }

    private long redisTime() {
        return RedisSupport.redis("read Redis clock", () -> client.getScript(StringCodec.INSTANCE)
                .eval(RScript.Mode.READ_ONLY, REDIS_TIME, RScript.ReturnType.LONG, List.of()));
    }

    private long parseLong(String value, String key) {
        try {
            return Long.parseLong(value);
        } catch (RuntimeException e) {
            throw new RdtConfigurationException("Invalid schedule next value in Redis key " + key, e);
        }
    }

    private StreamMessageId messageId(String value) {
        RdtSupport.require(value != null && value.matches("[0-9]+-[0-9]+"),
                "Stream message ID must use the form milliseconds-sequence");
        String[] parts = value.split("-", -1);
        try {
            return new StreamMessageId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        } catch (NumberFormatException e) {
            throw new RdtConfigurationException("Invalid Stream message ID: " + value, e);
        }
    }

    @Override
    public int clearCache(boolean confirmed) {
        RdtSupport.require(confirmed, "Cache clearing requires explicit confirmation");
        return RedisSupport.redis("clear namespace cache", () -> {
            int deleted = 0;
            List<String> batch = new ArrayList<>(500);
            // Region namespaces are encoded as a whole. Decode before accepting a scanned key.
            var scan = KeysScanOptions.defaults().pattern("rdt:*:cache:*").chunkSize(500);
            for (String key : client.getKeys().getKeys(scan)) {
                String[] parts = key.split(":", 4);
                String namespace = RdtSupport.decodeComponent(parts[1]).orElse("");
                if (!namespace.equals(keys.namespace()) && !namespace.startsWith(keys.namespace() + ":region:")) continue;
                batch.add(key);
                if (batch.size() == 500) {
                    deleted += Math.toIntExact(client.getKeys().unlink(batch.toArray(String[]::new)));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) deleted += Math.toIntExact(client.getKeys().unlink(batch.toArray(String[]::new)));
            client.getTopic(keys.key("cache-events", "clear-all"), StringCodec.INSTANCE).publish("*");
            return deleted;
        });
    }
}
