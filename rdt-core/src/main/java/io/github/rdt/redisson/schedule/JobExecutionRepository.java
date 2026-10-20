package io.github.rdt.redisson.schedule;

import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.schedule.TaskResult;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RMap;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

final class JobExecutionRepository {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final Duration retention;
    JobExecutionRepository(RedissonClient client,KeySpace keys,JsonCodec json,Duration retention) {
        this.client=client;this.keys=keys;this.json=json;this.retention=retention;
    }
    JobExecution execution(RMap<String, String> state) {
        String raw = state.get("execution");
        return raw == null ? null : json.read(raw, JobExecution.class);
    }

    void archive(JobExecution execution) {
        client.<String, String>getMapCache(keys.key("schedule-executions", execution.jobId()), StringCodec.INSTANCE)
                .fastPut(execution.executionId(), json.write(execution), retention.toMillis(), TimeUnit.MILLISECONDS);
        var index = client.<String>getScoredSortedSet(keys.key("schedule-execution-index", execution.jobId()), StringCodec.INSTANCE);
        index.add(execution.updatedAt(), execution.executionId());
        index.removeRangeByScore(0, true, execution.updatedAt() - retention.toMillis(), false);
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
        RedisSupport.redisRun("save task result", () -> client.<String, String>getMapCache(
                keys.key("schedule-results", taskId), StringCodec.INSTANCE).fastPut(occurrence,
                json.write(result), resultTtl.toMillis(), TimeUnit.MILLISECONDS));
        var indexes = new ManagementIndex(client, keys);
        indexes.record("attempts:" + taskId, occurrence, System.currentTimeMillis(), resultTtl);
        if (!result.successful()) indexes.record("failures:" + taskId, occurrence, System.currentTimeMillis(), resultTtl);
    }

    public List<TaskResult> results(String taskId,int offset,int limit) {
        RdtSupport.require(offset>=0 && limit>=1 && limit<=200,"Invalid task result page");
        var indexes=new ManagementIndex(client,keys);
        indexes.prune("attempts:"+taskId);
        var ids=indexes.resources("attempts:"+taskId).valueRangeReversed(offset,offset+limit-1);
        if(ids.isEmpty()) return List.of();
        var values=client.<String,String>getMapCache(keys.key("schedule-results",taskId),StringCodec.INSTANCE).getAll(new java.util.LinkedHashSet<>(ids));
        return ids.stream().map(values::get).filter(java.util.Objects::nonNull)
                .map(value -> json.<TaskResult>read(value,TaskResult.class)).toList();
    }

}
