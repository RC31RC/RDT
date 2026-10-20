package io.github.rdt.redisson.queue;

import io.github.rdt.exception.RdtQueueException;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.MessageHandler;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryClassifier;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RStream;
import org.redisson.api.RScript;
import org.redisson.client.codec.StringCodec;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamMessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class StreamMessageLifecycle {
    private static final Logger log=LoggerFactory.getLogger(StreamMessageLifecycle.class);
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final DistributedLockTemplate locks;
    private final RuntimeIdentity identity;
    private final RetryClassifier retryClassifier;
    private final StreamRetryIndex retryIndex;
    private final StreamRetention retention;
    private final MessageFailureRecorder failureRecorder;
    private final Map<String,AtomicInteger> listenerActive=new ConcurrentHashMap<>();
    private volatile RdtMetrics metrics=RdtMetrics.NONE;
    void metrics(RdtMetrics value) { metrics=value; }
    int active(String listenerId) { return listenerActive.computeIfAbsent(listenerId,ignored -> new AtomicInteger()).get(); }
    StreamMessageLifecycle(RedissonClient client,KeySpace keys,JsonCodec json,DistributedLockTemplate locks,
                           RuntimeIdentity identity,StreamRetryIndex retryIndex,StreamRetention retention,MessageFailureRecorder failureRecorder, RetryClassifier retryClassifier) {
        this.client=client;this.keys=keys;this.json=json;this.locks=locks;this.identity=identity;
        this.retryIndex=retryIndex;this.retention=retention;this.failureRecorder=failureRecorder;this.retryClassifier=retryClassifier;
    }
    boolean processStream(QueueSubscription spec, QueuePolicy effectivePolicy,
                               RStream<String, String> stream, StreamMessageId id,
                               Map<String, String> data, MessageHandler handler) {
        return processStream(spec, effectivePolicy, stream, id, data, handler, () -> true);
    }

    boolean processStream(QueueSubscription spec, QueuePolicy effectivePolicy,
                               RStream<String, String> stream, StreamMessageId id,
                               Map<String, String> data, MessageHandler handler, BooleanSupplier partitionOwner) {
        String identity = RdtSupport.component(spec.destination()) + ":" + RdtSupport.component(spec.group()) + ":" + id;
        if (!this.identity.accepting()) return false;
        BooleanSupplier runtimeOwner = this.identity.guard();
        BooleanSupplier runtimeAndPartition = () -> runtimeOwner.getAsBoolean() && partitionOwner.getAsBoolean();
        if (!runtimeAndPartition.getAsBoolean()) return false;
        Optional<LockHandle> acquisition = locks.tryLock("message:" + identity,
                new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO));
        if (acquisition.isEmpty()) {
            return false;
        }
        try (LockHandle handle = acquisition.get()) {
            BooleanSupplier owner = () -> runtimeAndPartition.getAsBoolean() && handle.isHeld();
            // Pending, retry timing and failure budget are read in one same-slot operation.
            Number prepared = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY, """
                    if #redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1) == 0 then return -1 end
                    local t = redis.call('TIME'); local now = t[1]*1000+math.floor(t[2]/1000)
                    local due = redis.call('GET', KEYS[2])
                    if due and tonumber(due) > now then return -2 end
                    return tonumber(redis.call('GET', KEYS[3]) or '0')
                    """, RScript.ReturnType.LONG,
                    List.of(stream.getName(), keys.key("message-retry-at", identity),
                            keys.key("message-failures", identity)), spec.group(), id.toString());
            if (prepared.longValue() < 0) return false;
            Message message;
            try {
                message = json.read(data.get("message"), Message.class);
                RdtSupport.require(message != null && message.id() != null, "Invalid message envelope");
            } catch (RuntimeException malformed) {
                Message raw = new Message("malformed:" + id, json.write(data), Instant.now().toString(), 1, null, null, Map.of());
                failureRecorder.saveFailure(failureRecorder.failure(spec, raw, 0, malformed), effectivePolicy);
                if (!owner.getAsBoolean()) return false;
                acknowledge(spec, effectivePolicy, stream, id, identity, raw);
                return true;
            }
            RAtomicLong attempts = client.getAtomicLong(keys.key("message-failures", identity));
            RAtomicLong retryAt = client.getAtomicLong(keys.key("message-retry-at", identity));
            long attempt = prepared.longValue();
            metrics.increment("rdt.queue.delivery", "destination", spec.destination());
            if (attempt >= effectivePolicy.maxAttempts()) {
                failureRecorder.saveFailure(failureRecorder.failure(spec, message, attempt,
                        new RdtQueueException("Failure budget exhausted after recovery")), effectivePolicy);
                if (!owner.getAsBoolean()) return false;
                acknowledge(spec, effectivePolicy, stream, id, identity, message);
                return true;
            }
            try {
                listenerActive.computeIfAbsent(spec.listenerId(), ignored -> new AtomicInteger()).incrementAndGet();
                try { handler.handle(message); }
                finally { listenerActive.get(spec.listenerId()).decrementAndGet(); }
            } catch (Exception e) {
                if (e instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    return false;
                }
                Number failures = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                        local count = redis.call('INCR', KEYS[1])
                        redis.call('PEXPIRE', KEYS[1], ARGV[1]); return count
                        """, RScript.ReturnType.LONG, List.of(attempts.getName()),
                        failureRecorder.attemptsTtl(effectivePolicy).toMillis());
                attempt = failures.longValue();
                failureRecorder.recordFirstFailure(spec, message, effectivePolicy);
                log.warn("Message failed id={} group={} attempt={}", message.id(), spec.group(), attempt, e);
                if (!owner.getAsBoolean()) return false;
                if (!retryClassifier.isRetryable(e) || !effectivePolicy.retry().canRetry((int) attempt)) {
                    failureRecorder.saveFailure(failureRecorder.failure(spec, message, attempt, e), effectivePolicy);
                    if (!owner.getAsBoolean()) return false;
                    acknowledge(spec, effectivePolicy, stream, id, identity, message);
                } else {
                    long due = System.currentTimeMillis() + effectivePolicy.retry().delayAfter((int) attempt).toMillis();
                    retryAt.set(due);
                    retryAt.expire(failureRecorder.attemptsTtl(effectivePolicy));
                    // Normal retry timing is independent of crash visibility. Claim/submit failures keep Pending.
                    retryIndex.retries(spec).add(due, id.toString());
                    retryIndex.retries(spec).expire(failureRecorder.attemptsTtl(effectivePolicy));
                }
                return false;
            }
            if (Thread.currentThread().isInterrupted() || !owner.getAsBoolean()) return false;
            acknowledge(spec, effectivePolicy, stream, id, identity, message);
            return true;
        }
    }

    private void acknowledge(QueueSubscription spec, QueuePolicy effective, RStream<String, String> stream,
                             StreamMessageId id, String deliveryIdentity, Message message) {
        retention.acknowledge(effective, stream, spec.group(), id.toString(),
                List.of(retryIndex.retries(spec).getName(),
                        keys.key("message-failures", deliveryIdentity),
                        keys.key("message-retry-at", deliveryIdentity),
                        keys.key("message-delivery", deliveryIdentity),
                        failureRecorder.firstFailureKey(spec, message)));
    }
}
