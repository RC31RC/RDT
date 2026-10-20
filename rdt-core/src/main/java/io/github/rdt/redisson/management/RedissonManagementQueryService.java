package io.github.rdt.redisson.management;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.management.DeadLetterStats;
import io.github.rdt.management.DeadLetterView;
import io.github.rdt.management.ManagementOverview;
import io.github.rdt.management.ManagementPage;
import io.github.rdt.management.ManagementQueryService;
import io.github.rdt.management.StreamConsumerView;
import io.github.rdt.management.StreamGroupView;
import io.github.rdt.management.StreamMessageView;
import io.github.rdt.management.StreamView;
import io.github.rdt.management.TaskView;
import io.github.rdt.queue.FailedMessage;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.runtime.RuntimeInstance;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.schedule.TaskResult;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.redisson.api.RMap;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.PendingResult;
import org.redisson.api.stream.StreamConsumer;
import org.redisson.api.stream.StreamGroup;
import org.redisson.api.stream.StreamInfo;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.client.codec.StringCodec;

/**
 * 基于 RDT 键约定读取共享 Redis 状态。
 *
 * <p>资源通过有界共享索引发现，页面只读取当前请求页和按需历史。</p>
 */
public final class RedissonManagementQueryService implements ManagementQueryService {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;

    /**
     * 创建查询服务。
     *
     * @param client 共享 Redisson 客户端
     * @param keys 命名空间键空间
     * @param json JSON 编解码器
     */
    public RedissonManagementQueryService(RedissonClient client, KeySpace keys, JsonCodec json) {
        this.client = client;
        this.keys = keys;
        this.json = json;
    }

    @Override
    public Optional<StreamView> stream(String destination) {
        RdtSupport.component(destination);
        String key = keys.key("stream", destination);
        if (client.getKeys().countExists(key) == 0) {
            return Optional.empty();
        }
        return Optional.of(readStream(destination, key));
    }

    @Override
    public List<StreamMessageView> streamMessages(String destination, int limit) {
        RdtSupport.component(destination);
        RdtSupport.require(limit > 0 && limit <= 200, "Stream message limit must be between 1 and 200");
        String key = keys.key("stream", destination);
        if (client.getKeys().countExists(key) == 0) {
            return List.of();
        }
        RStream<String, String> stream = client.getStream(key, StringCodec.INSTANCE);
        Map<StreamMessageId, Map<String, String>> entries = RedisSupport.redis("read stream messages",
                () -> stream.rangeReversed(limit, StreamMessageId.MAX, StreamMessageId.MIN));
        List<StreamMessageView> messages = new ArrayList<>();
        entries.forEach((id, fields) -> messages.add(new StreamMessageView(id(id), fields)));
        return List.copyOf(messages);
    }

    private StreamView readStream(String destination, String key) {
        return readStream(destination, key, true);
    }

    private StreamView readStream(String destination, String key, boolean includeConsumers) {
        RStream<String, String> stream = client.getStream(key, StringCodec.INSTANCE);
        StreamInfo<String, String> info = RedisSupport.redis("read stream info", stream::getInfo);
        List<StreamGroupView> groups = new ArrayList<>();
        for (StreamGroup group : RedisSupport.redis("read stream groups", stream::listGroups)) {
            PendingResult pending = RedisSupport.redis("read stream pending", () -> stream.getPendingInfo(group.getName()));
            List<StreamConsumerView> consumers = new ArrayList<>();
            for (StreamConsumer consumer : !includeConsumers ? List.<StreamConsumer>of() : RedisSupport.redis("read stream consumers",
                    () -> stream.listConsumers(group.getName()))) {
                consumers.add(new StreamConsumerView(consumer.getName(), consumer.getPending(),
                        consumer.getIdleTime(), consumer.getInactive()));
            }
            consumers.sort(Comparator.comparing(StreamConsumerView::name));
            groups.add(new StreamGroupView(group.getName(), group.getConsumers(), group.getPending(),
                    group.getLag(), id(group.getLastDeliveredId()), pending == null ? 0 : pending.getTotal(),
                    id(pending == null ? null : pending.getLowestId()),
                    id(pending == null ? null : pending.getHighestId()), consumers));
        }
        groups.sort(Comparator.comparing(StreamGroupView::name));
        return new StreamView(destination, key, info.getLength(), groups, id(info.getLastGeneratedId()),
                queueDisplayName(destination));
    }

    @Override
    public Optional<TaskView> task(String taskId) {
        RdtSupport.component(taskId);
        String key = keys.key("schedule-state", taskId);
        if (client.getKeys().countExists(key) == 0) {
            return Optional.empty();
        }
        return readTask(taskId, key);
    }

    private Optional<TaskView> readTask(String taskId, String key) {
        Map<String, String> state = RedisSupport.redis("read task state", () ->
                client.<String, String>getMap(key, StringCodec.INSTANCE).readAllMap());
        if (state.isEmpty() || "true".equals(state.get("retired"))) return Optional.empty();
        String definition = state.get("definition");
        long next = parseLong(state.get("next"), "schedule next", key);
        JobExecution execution = state.get("execution") == null ? null : json.read(state.get("execution"), JobExecution.class);
        return Optional.of(new TaskView(taskId, key, definition, next, state.get("displayName"),
                "true".equals(state.get("paused")), execution == null ? "" : execution.status(),
                execution == null ? "" : execution.executionId(), state.getOrDefault("route", "HEALTH_FIRST")));
    }

    private void validatePage(int offset, int limit) {
        RdtSupport.require(offset >= 0 && offset <= 100_000 && limit >= 1 && limit <= 200, "Invalid management page");
    }

    @Override
    public ManagementPage<StreamView> streams(int offset, int limit) {
        validatePage(offset, limit);
        var index = new ManagementIndex(client, keys).resources("streams");
        List<StreamView> items = new ArrayList<>();
        for (String name : index.valueRange(offset, offset + limit - 1)) {
            String key = keys.key("stream", name);
            if (client.getKeys().countExists(key) > 0) items.add(readStream(name, key, false));
            else index.remove(name);
        }
        return new ManagementPage<>(items, offset, limit, index.size());
    }

    @Override
    public ManagementPage<TaskView> tasks(int offset, int limit) {
        validatePage(offset, limit);
        var index = new ManagementIndex(client, keys).resources("tasks");
        List<TaskView> items = new ArrayList<>();
        for (String id : index.valueRange(offset, offset + limit - 1)) {
            String key = keys.key("schedule-state", id);
            if (client.getKeys().countExists(key) > 0) readTask(id, key).ifPresent(items::add);
            else index.remove(id);
        }
        return new ManagementPage<>(items, offset, limit, index.size());
    }

    @Override
    public List<TaskResult> taskResults(String taskId, int offset, int limit) {
        RdtSupport.component(taskId);
        validatePage(offset, limit);
        var indexes = new ManagementIndex(client, keys);
        indexes.prune("attempts:" + taskId);
        var ids = indexes.resources("attempts:" + taskId).valueRangeReversed(offset, offset + limit - 1);
        if (ids.isEmpty()) return List.of();
        var values = client.<String, String>getMap(keys.key("schedule-results", taskId), StringCodec.INSTANCE)
                .getAll(new HashSet<>(ids));
        return ids.stream().map(values::get).filter(Objects::nonNull)
                .map(raw -> (TaskResult) json.read(raw, TaskResult.class)).toList();
    }

    @Override
    public ManagementPage<DeadLetterView> deadLetters(int offset, int limit) {
        validatePage(offset, limit);
        var indexes = new ManagementIndex(client, keys);
        indexes.prune("dead-letters");
        var index = indexes.resources("dead-letters");
        List<DeadLetterView> items = new ArrayList<>();
        for (String id : index.valueRangeReversed(offset, offset + limit - 1)) {
            String[] ref = json.read(id, String[].class);
            String raw = deadLetters(keys.key("dead-letters", ref[0])).get(ref[1]);
            if (raw == null) { index.remove(id); continue; }
            FailedMessage failure = json.read(raw, FailedMessage.class);
            items.add(new DeadLetterView(failure.destination(), failure.group(), failure.message().id(),
                    failure.message().payload(), failure.message().createdAt(), failure.failureCount(),
                    failure.error(), failure.failedAt(), queueDisplayName(failure.destination()),failure.mode(),failure.instanceId(),failure.originalMessageId(),failure.firstFailedAt()));
        }
        return new ManagementPage<>(items, offset, limit, index.size());
    }

    @Override
    public DeadLetterStats deadLetterStats(Duration recentWindow) {
        RdtSupport.positive(recentWindow, "Dead letter recent window");
        var indexes = new ManagementIndex(client, keys);
        indexes.prune("dead-letters");
        var index = indexes.resources("dead-letters");
        long now = System.currentTimeMillis();
        Double oldest = index.firstScore();
        return new DeadLetterStats(index.size(), index.count(now - recentWindow.toMillis(), true, now, true),
                oldest == null ? 0 : oldest.longValue());
    }

    @Override
    public ManagementOverview overview() {
        var indexes = new ManagementIndex(client, keys);
        long streamCount = indexes.resources("streams").size();
        long taskCount = indexes.resources("tasks").size();
        long length = 0, groups = 0, pending = 0, failures = 0;
        for (String name : indexes.resources("streams").valueRange(0, 199)) {
            var stream = client.<String, String>getStream(keys.key("stream", name), StringCodec.INSTANCE);
            if (!stream.isExists()) continue;
            length += stream.size();
            for (StreamGroup group : stream.listGroups()) { groups++; pending += group.getPending(); }
        }
        for (String id : indexes.resources("tasks").valueRange(0, 199)) {
            indexes.prune("failures:" + id);
            failures += indexes.resources("failures:" + id).size();
        }
        indexes.prune("dead-letters");
        return new ManagementOverview(keys.namespace(), streamCount, length, groups, pending, taskCount,
                failures, indexes.resources("dead-letters").size(), streamCount > 200 || taskCount > 200);
    }

    @Override
    public List<JobExecution> executions(String taskId, int offset, int limit) {
        RdtSupport.component(taskId);
        RdtSupport.require(offset >= 0 && limit >= 1 && limit <= 200, "Invalid execution page");
        var index = client.<String>getScoredSortedSet(keys.key("schedule-execution-index", taskId), StringCodec.INSTANCE);
        List<String> ids = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                local t = redis.call('TIME'); local now = t[1]*1000+math.floor(t[2]/1000)
                local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 4096)
                for _, id in ipairs(expired) do
                  redis.call('ZREM', KEYS[1], id); redis.call('ZREM', KEYS[2], id)
                  redis.call('HDEL', KEYS[3], id)
                end
                local rows = redis.call('ZREVRANGE', KEYS[1], ARGV[1], tonumber(ARGV[1])+tonumber(ARGV[2])-1)
                local result = {}
                for _, id in ipairs(rows) do
                  local expiry = redis.call('ZSCORE', KEYS[2], id)
                  if expiry and tonumber(expiry) > now then table.insert(result, id) end
                end
                return result
                """, RScript.ReturnType.LIST,
                List.of(index.getName(), keys.key("schedule-execution-expiry", taskId),
                        keys.key("schedule-executions", taskId)), offset, limit);
        if (ids.isEmpty()) return List.of();
        var values = client.<String, String>getMap(keys.key("schedule-executions", taskId), StringCodec.INSTANCE)
                .getAll(new HashSet<>(ids));
        return ids.stream().map(values::get).filter(Objects::nonNull)
                .map(raw -> (JobExecution) json.read(raw, JobExecution.class))
                .map(value -> new JobExecution(value.executionId(), value.jobId(), value.scheduledAt(), value.attempt(),
                        value.status(), value.instanceId(), value.runId(), null, value.updatedAt(), value.retryAt(), value.detail(), 0))
                .toList();
    }

    @Override
    public List<RuntimeInstance> instances() {
        List<RuntimeInstance> result = new ArrayList<>();
        var index = client.<String>getScoredSortedSet(keys.key("runtime-index", "instances"), StringCodec.INSTANCE);
        index.removeRangeByScore(0, true, System.currentTimeMillis(), false);
        for (String name : index.valueRange(System.currentTimeMillis(), true, Double.POSITIVE_INFINITY, true, 0, 256)) {
            String key = keys.key("runtime-instance", name);
            keys.nameFromKey("runtime-instance", key).ifPresent(ignoredName -> {
                String value = RedisSupport.redis("read runtime instance", () ->
                        client.<String>getBucket(key, StringCodec.INSTANCE).get());
                if (value == null) {
                    return;
                }
                int separator = value.indexOf('\n');
                if (separator < 0 || separator + 1 >= value.length()) {
                    return;
                }
                try {
                    RuntimeInstance view = json.read(value.substring(separator + 1), RuntimeInstance.class);
                    String owner = client.<String>getBucket(keys.key("owner", "node:" + view.instanceId()),
                            StringCodec.INSTANCE).get();
                    if (value.substring(0, separator).equals(owner)) result.add(view);
                } catch (RuntimeException ignored) {
                    // An invalid registry entry must not break the whole console snapshot.
                }
            });
        }
        result.sort(Comparator.comparing(RuntimeInstance::instanceId));
        return List.copyOf(result);
    }

    /** 从共享 Redis 读取，不能依赖业务 JVM 的本地消费者注册表。 */
    private String queueDisplayName(String destination) {
        return RedisSupport.redis("read queue display name", () -> client.<String, String>getMap(
                keys.key("queue-metadata", destination), StringCodec.INSTANCE).get("displayName"));
    }

    private RMap<String, String> deadLetters(String key) {
        return client.getMapCache(key, StringCodec.INSTANCE);
    }

    private long parseLong(String value, String field, String key) {
        try {
            return Long.parseLong(value);
        } catch (RuntimeException e) {
            throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "Invalid " + field + " in Redis key " + key, e);
        }
    }

    private String id(StreamMessageId value) {
        return value == null ? null : value.toString();
    }
}
