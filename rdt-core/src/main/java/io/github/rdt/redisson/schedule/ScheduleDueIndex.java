package io.github.rdt.redisson.schedule;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.util.ArrayList;
import java.util.List;
import org.redisson.api.RScript;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

final class ScheduleDueIndex {
    static final int LOCAL_SCAN_LIMIT = 256;
    record DuePage(List<String> ids, int scanned) {}
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    ScheduleDueIndex(RedissonClient client,KeySpace keys,JsonCodec json) { this.client=client;this.keys=keys;this.json=json; }
    RScoredSortedSet<String> dueIndex() {
        return client.getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE);
    }

    DuePage findDue(List<String> candidates, long now, int limit) {
        RdtSupport.require(candidates.size() <= LOCAL_SCAN_LIMIT && limit > 0, "Invalid local due page");
        List<Object> arguments = new ArrayList<>();
        arguments.add(now);
        arguments.add(limit);
        arguments.addAll(candidates);
        List<String> result = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY, """
                local result = {'0'}
                for i = 3, #ARGV do
                  result[1] = tostring(i-2)
                  local score = redis.call('ZSCORE', KEYS[1], ARGV[i])
                  if score and tonumber(score) <= tonumber(ARGV[1]) then
                    table.insert(result, ARGV[i])
                    if #result-1 >= tonumber(ARGV[2]) then break end
                  end
                end
                return result
                """, RScript.ReturnType.LIST, List.of(dueIndex().getName()), arguments.toArray());
        return new DuePage(List.copyOf(result.subList(1, result.size())), Integer.parseInt(result.getFirst()));
    }

    void refreshDueIndex(String taskId, RMap<String, String> state) {
        if ("true".equals(state.get("retired"))) { dueIndex().remove(taskId); return; }
        String value=state.get("next");
        Long next=value==null ? null : Long.valueOf(value);
        String raw=state.get("execution");
        JobExecution execution = raw==null ? null : json.read(raw,JobExecution.class);
        long dueAt = execution != null && !execution.terminal() ? execution.retryAt()
                : "true".equals(state.get("paused")) ? Long.MAX_VALUE
                : next == null ? Long.MAX_VALUE : next;
        RedisSupport.redisRun("update schedule due index", () -> dueIndex().add((double) dueAt, taskId));
        // State and the global index intentionally use different cluster slots. The terminal
        // tombstone is checked after publication too, closing the retire/publication race.
        if ("true".equals(state.get("retired"))) dueIndex().remove(taskId);
    }

}
