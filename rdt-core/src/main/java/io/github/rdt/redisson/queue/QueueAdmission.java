package io.github.rdt.redisson.queue;

import io.github.rdt.exception.RdtQueueException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.support.KeySpace;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Per-destination hard admission, including accepted messages still in the delay set. */
public final class QueueAdmission {
    private final RedissonClient client;
    private final KeySpace keys;
    private final QueuePolicy policy;
    private static final String ACCEPT = """
            local limit = tonumber(ARGV[2])
            local budget = tonumber(ARGV[3])
            local maximum = tonumber(ARGV[5])
            if ARGV[2] == '' then
              limit = tonumber(redis.call('HGET', KEYS[4], 'messages'))
              budget = tonumber(redis.call('HGET', KEYS[4], 'bytes'))
              maximum = tonumber(redis.call('HGET', KEYS[4], 'message'))
              if not limit or not budget or not maximum then return -1 end
            end
            local bytes = string.len(ARGV[1])
            if bytes > maximum then return -2 end
            local streamCount = redis.call('XLEN', KEYS[1])
            local delayCount = redis.call('ZCARD', KEYS[2])
            local expectedStream = tonumber(redis.call('HGET', KEYS[5], 'stream') or '-1')
            local expectedDelay = tonumber(redis.call('HGET', KEYS[5], 'delay') or '-1')
            local used = tonumber(redis.call('GET', KEYS[3]) or '0')
            if expectedStream ~= streamCount or expectedDelay ~= delayCount then
              used = 0
              for _, row in ipairs(redis.call('XRANGE', KEYS[1], '-', '+')) do
                for i = 1, #row[2], 2 do
                  if row[2][i] == 'message' then used = used + string.len(row[2][i+1]) end
                end
              end
              for _, value in ipairs(redis.call('ZRANGE', KEYS[2], 0, -1)) do
                used = used + string.len(value)
              end
              if used == 0 then redis.call('DEL', KEYS[3])
              else redis.call('SET', KEYS[3], used) end
              redis.call('HSET', KEYS[5], 'stream', streamCount, 'delay', delayCount)
            end
            local count = streamCount + delayCount
            if count >= limit then return 0 end
            if used + bytes > budget then return 0 end
            if ARGV[4] == '' then
              redis.call('XADD', KEYS[1], '*', 'message', ARGV[1])
              redis.call('HINCRBY', KEYS[5], 'stream', 1)
            else
              redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
              redis.call('HINCRBY', KEYS[5], 'delay', 1)
            end
            redis.call('INCRBY', KEYS[3], bytes)
            redis.call('HSET', KEYS[4], 'messages', limit, 'bytes', budget, 'message', maximum)
            return 1
            """;

    QueueAdmission(RedissonClient client, KeySpace keys, QueuePolicy policy) {
        this.client = client;
        this.keys = keys;
        this.policy = policy;
    }

    /** Operator replays use the destination's shared producer limits, never an unbounded XADD. */
    public static void replay(RedissonClient client, KeySpace keys, String destination, String encoded) {
        Number accepted = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,
                ACCEPT, RScript.ReturnType.LONG,
                List.of(keys.key("stream", destination), keys.key("message-delay", destination),
                        keys.key("queue-capacity", destination), keys.key("queue-limits", destination),
                        keys.key("queue-capacity-state", destination)),
                encoded, "", "", "", "");
        if (accepted.intValue() != 1)
            throw new RdtQueueException(accepted.intValue() == -2 ? RdtErrorCode.MESSAGE_TOO_LARGE : RdtErrorCode.QUEUE_CAPACITY, "Replay rejected: destination limits missing, message too large or capacity exhausted");
    }

    void validate(String encoded) {
        if (encoded.getBytes(StandardCharsets.UTF_8).length > policy.maxMessageBytes())
            throw new RdtQueueException(RdtErrorCode.MESSAGE_TOO_LARGE, "Message exceeds max-message-bytes");
    }

    void accept(String destination, String encoded, Long due) {
        validate(encoded);
        Number accepted = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,
                ACCEPT, RScript.ReturnType.LONG,
                List.of(keys.key("stream", destination), keys.key("message-delay", destination),
                        keys.key("queue-capacity", destination), keys.key("queue-limits", destination),
                        keys.key("queue-capacity-state", destination)),
                encoded, policy.maxBacklogMessages(), policy.maxBacklogBytes(), due == null ? "" : due.toString(), policy.maxMessageBytes());
        if (accepted.intValue() != 1)
            throw new RdtQueueException(RdtErrorCode.QUEUE_CAPACITY, "Queue capacity exhausted for destination " + destination);
    }
}
