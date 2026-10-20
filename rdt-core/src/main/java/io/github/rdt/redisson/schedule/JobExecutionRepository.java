package io.github.rdt.redisson.schedule;

import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.schedule.TaskResult;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import org.redisson.api.RMap;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

final class JobExecutionRepository {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final Duration retention;
    private final int maxEntries;
    JobExecutionRepository(RedissonClient client,KeySpace keys,JsonCodec json,Duration retention,int maxEntries) {
        this.maxEntries = maxEntries;
        this.client=client;this.keys=keys;this.json=json;this.retention=retention;
    }
    JobExecution execution(RMap<String, String> state) {
        String raw = state.get("execution");
        return raw == null ? null : json.read(raw, JobExecution.class);
    }

    void archive(JobExecution execution) {
        store(keys.key("schedule-executions", execution.jobId()),
                keys.key("schedule-execution-index", execution.jobId()),
                keys.key("schedule-execution-expiry", execution.jobId()),
                keys.key("schedule-execution-failures", execution.jobId()),
                keys.key("schedule-execution-failure-expiry", execution.jobId()),
                execution.executionId(), json.write(execution), execution.updatedAt(), retention, false);
    }

    /** Hash bodies and all indexes are bounded together and expire without another writer. */
    private void store(String data, String index, String expiry, String failures, String failureExpiry,
                       String id, String value, long time, Duration ttl, boolean failed) {
        client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                local t = redis.call('TIME'); local now = t[1]*1000+math.floor(t[2]/1000)
                local function remove(id)
                  redis.call('HDEL', KEYS[1], id)
                  for i = 2, 5 do redis.call('ZREM', KEYS[i], id) end
                end
                local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 256)
                for _, id in ipairs(expired) do remove(id) end
                redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
                redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
                redis.call('ZADD', KEYS[3], now+tonumber(ARGV[4]), ARGV[1])
                if ARGV[6] == 'true' then
                  redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1])
                  redis.call('ZADD', KEYS[5], now+tonumber(ARGV[4]), ARGV[1])
                else redis.call('ZREM', KEYS[4], ARGV[1]); redis.call('ZREM', KEYS[5], ARGV[1]) end
                local excess = redis.call('ZCARD', KEYS[2])-tonumber(ARGV[5])
                if excess > 0 then
                  local old = redis.call('ZRANGE', KEYS[2], 0, excess-1)
                  for _, id in ipairs(old) do remove(id) end
                end
                local latest = redis.call('ZREVRANGE', KEYS[3], 0, 0, 'WITHSCORES')
                if #latest > 0 then
                  for i = 1, 5 do redis.call('PEXPIREAT', KEYS[i], math.floor(tonumber(latest[2]))) end
                end
                return 1
                """, RScript.ReturnType.LONG, List.of(data, index, expiry, failures, failureExpiry),
                id, value, time, ttl.toMillis(), maxEntries, Boolean.toString(failed));
    }

    boolean transition(RMap<String, String> state, JobExecution previous, JobExecution updated,
                               Long next, boolean requireLive) {
        Long changed = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                if redis.call('hget', KEYS[1], 'execution') ~= ARGV[1] then return 0 end
                local t = redis.call('TIME')
                local now = t[1] * 1000 + math.floor(t[2] / 1000)
                if ARGV[4] == 'true' and tonumber(ARGV[5]) <= now then return 0 end
                redis.call('hset', KEYS[1], 'execution', ARGV[2])
                if ARGV[3] ~= '' then redis.call('hset', KEYS[1], 'next', ARGV[3]) end
                return 1
                """, RScript.ReturnType.LONG, List.of(state.getName()), json.write(previous), json.write(updated),
                next == null ? "" : Long.toString(next), Boolean.toString(requireLive), previous.retryAt());
        if (!Long.valueOf(1).equals(changed)) return false;
        archive(updated);
        return true;
    }

    void save(String taskId, String occurrence, Duration resultTtl, TaskResult result) {
        RdtSupport.positive(resultTtl, "result ttl");
        store(keys.key("schedule-results", taskId),
                keys.key("management-index", "attempts:" + taskId),
                keys.key("management-expiry", "attempts:" + taskId),
                keys.key("management-index", "failures:" + taskId),
                keys.key("management-expiry", "failures:" + taskId),
                occurrence, json.write(result), System.currentTimeMillis(), resultTtl, !result.successful());
    }

    public List<TaskResult> results(String taskId,int offset,int limit) {
        RdtSupport.require(offset>=0 && limit>=1 && limit<=200,"Invalid task result page");
        var indexes=new ManagementIndex(client,keys);
        indexes.prune("attempts:"+taskId);
        var ids=indexes.resources("attempts:"+taskId).valueRangeReversed(offset,offset+limit-1);
        if(ids.isEmpty()) return List.of();
        var values=client.<String,String>getMap(keys.key("schedule-results",taskId),StringCodec.INSTANCE).getAll(new LinkedHashSet<>(ids));
        return ids.stream().map(values::get).filter(Objects::nonNull)
                .map(value -> json.<TaskResult>read(value,TaskResult.class)).toList();
    }

}
