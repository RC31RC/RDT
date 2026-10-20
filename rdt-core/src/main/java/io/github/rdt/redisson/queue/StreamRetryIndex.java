package io.github.rdt.redisson.queue;

import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.client.codec.StringCodec;

final class StreamRetryIndex {
    private final RedissonClient client;
    private final KeySpace keys;
    private volatile RdtMetrics metrics=RdtMetrics.NONE;
    void metrics(RdtMetrics value) { metrics=value; }
    StreamRetryIndex(RedissonClient client,KeySpace keys) { this.client=client;this.keys=keys; }
    StreamMessageId streamId(String id) {
        String[] parts = id.split("-", 2);
        return new StreamMessageId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
    }

    List<String> recoverablePending(QueueSubscription spec, QueuePolicy effective,
                                           RStream<String, String> target, String cursor, int count) {
        // XPENDING does not reset idle time. Filter future retries before XCLAIM, never claim-and-skip.
        List<String> result = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY, """
                local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000)
                local rows=redis.call('XPENDING',KEYS[1],ARGV[1],'IDLE',ARGV[2],ARGV[3],'+',128)
                local selected={}; local last='-'
                for _,row in ipairs(rows) do
                  last='('..row[1]
                  local due=redis.call('ZSCORE',KEYS[2],row[1])
                  if not due or tonumber(due)<=now then table.insert(selected,row[1]) end
                  if #selected>=tonumber(ARGV[4]) then break end
                end
                if #rows<128 and #selected<tonumber(ARGV[4]) then last='-' end
                table.insert(selected,1,last); return selected
                """, RScript.ReturnType.LIST, List.of(target.getName(),retries(spec).getName()),
                spec.group(), effective.visibilityTimeout().toMillis(), cursor, count);
        return new ArrayList<>(result);
    }

    RScoredSortedSet<String> retries(QueueSubscription spec) {
        // This retry index shares the Stream hash slot; Pending remains the recovery authority.
        return client.getScoredSortedSet(keys.key("message-retries", RdtSupport.component(spec.destination())
                + ":" + RdtSupport.component(spec.group())), StringCodec.INSTANCE);
    }

    Map<StreamMessageId, Map<String, String>> claimRetries(QueueSubscription spec, QueuePolicy effective,
            RStream<String, String> stream, String consumer, int count) {
        List<String> ids = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                local t = redis.call('TIME')
                local now = t[1] * 1000 + math.floor(t[2] / 1000)
                local due = redis.call('zrangebyscore', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[2])
                local claimed = {}
                for _, id in ipairs(due) do
                  if #redis.call('xpending', KEYS[2], ARGV[1], id, id, 1) == 0 then
                    redis.call('zrem', KEYS[1], id)
                  else
                    redis.call('zadd', KEYS[1], now + tonumber(ARGV[3]), id)
                    table.insert(claimed, id)
                  end
                end
                return claimed
                """, RScript.ReturnType.LIST, List.of(retries(spec).getName(), stream.getName()),
                spec.group(), count, effective.visibilityTimeout().toMillis());
        if (ids.isEmpty()) return Map.of();
        StreamMessageId[] entries = ids.stream().map(id -> {
            String[] parts = id.split("-", 2);
            return new StreamMessageId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }).toArray(StreamMessageId[]::new);
        return stream.claim(spec.group(), consumer, 0, TimeUnit.MILLISECONDS, entries);
    }



}
