package io.github.rdt.redisson.schedule;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

final class ScheduleDueIndex {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    ScheduleDueIndex(RedissonClient client,KeySpace keys,JsonCodec json) { this.client=client;this.keys=keys;this.json=json; }
    RScoredSortedSet<String> dueIndex() {
        return client.getScoredSortedSet(keys.key("schedule-due", "tasks"), StringCodec.INSTANCE);
    }

    void refreshDueIndex(String taskId, RMap<String, String> state) {
        String value=state.get("next");
        Long next=value==null ? null : Long.valueOf(value);
        String raw=state.get("execution");
        JobExecution execution = raw==null ? null : json.read(raw,JobExecution.class);
        long dueAt = execution != null && !execution.terminal() ? execution.retryAt()
                : "true".equals(state.get("paused")) ? Long.MAX_VALUE
                : next == null ? Long.MAX_VALUE : next;
        RedisSupport.redisRun("update schedule due index", () -> dueIndex().add((double) dueAt, taskId));
    }

}
