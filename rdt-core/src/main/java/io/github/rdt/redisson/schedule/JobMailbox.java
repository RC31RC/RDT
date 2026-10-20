package io.github.rdt.redisson.schedule;

import io.github.rdt.exception.RdtQueueException;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Atomic enqueue+PEXPIRE. Redis reclaims abandoned runs even with all application nodes stopped. */
final class JobMailbox {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;

    JobMailbox(RedissonClient client, KeySpace keys, JsonCodec json) {
        this.client = client;
        this.keys = keys;
        this.json = json;
    }

    void send(String worker, String run, JobInvocation invocation, Duration ttl, int capacity) {
        Number accepted = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                if redis.call('LLEN', KEYS[1]) >= tonumber(ARGV[3]) then return 0 end
                redis.call('RPUSH', KEYS[1], ARGV[1])
                redis.call('PEXPIRE', KEYS[1], ARGV[2])
                return 1
                """, RScript.ReturnType.LONG, List.of(keys.key("job-worker", worker + ":" + run)),
                json.write(invocation), ttl.toMillis(), capacity);
        if (accepted.intValue() != 1) throw new RdtQueueException("Job mailbox capacity exhausted");
    }

    /** One bounded blocking command per scheduler; timeout permits ownership/draining rechecks. */
    List<String> receive(String worker, String run, int count) throws InterruptedException {
        RdtSupport.require(count > 0, "Mailbox read capacity must be positive");
        var queue = client.<String>getBlockingQueue(keys.key("job-worker", worker + ":" + run), StringCodec.INSTANCE);
        String first = queue.poll(1, TimeUnit.SECONDS);
        if (first == null) return List.of();
        List<String> messages = new ArrayList<>();
        messages.add(first);
        if (count > 1) messages.addAll(queue.poll(count - 1));
        return messages;
    }
}
