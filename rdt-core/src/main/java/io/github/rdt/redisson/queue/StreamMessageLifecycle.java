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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RStream;
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
    void processStream(QueueSubscription spec, QueuePolicy effectivePolicy,
                               RStream<String, String> stream, StreamMessageId id,
                               Map<String, String> data, MessageHandler handler) {
        processStream(spec, effectivePolicy, stream, id, data, handler, () -> true);
    }

    void processStream(QueueSubscription spec, QueuePolicy effectivePolicy,
                               RStream<String, String> stream, StreamMessageId id,
                               Map<String, String> data, MessageHandler handler, BooleanSupplier partitionOwner) {
        String identity = RdtSupport.component(spec.destination()) + ":" + RdtSupport.component(spec.group()) + ":" + id;
        if (!this.identity.accepting()) return;
        BooleanSupplier runtimeOwner = this.identity.guard();
        BooleanSupplier runtimeAndPartition = () -> runtimeOwner.getAsBoolean() && partitionOwner.getAsBoolean();
        if (!runtimeAndPartition.getAsBoolean()) return;
        Optional<LockHandle> acquisition = locks.tryLock("message:" + identity,
                new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO));
        if (acquisition.isEmpty()) {
            return;
        }
        try (LockHandle handle = acquisition.get()) {
            BooleanSupplier owner = () -> runtimeAndPartition.getAsBoolean() && handle.isHeld();
            // 当前工作线程认领消息后、获取锁前，前一个持有者可能已确认消息，因此必须再次检查。
            if (stream.pendingRange(spec.group(), id, id, 1).isEmpty()) {
                return;
            }
            Message message;
            try {
                message = json.read(data.get("message"), Message.class);
                RdtSupport.require(message != null && message.id() != null, "Invalid message envelope");
            } catch (RuntimeException malformed) {
                Message raw = new Message("malformed:" + id, json.write(data), Instant.now().toString(), 1, null, null, Map.of());
                failureRecorder.saveFailure(failureRecorder.failure(spec, raw, 0, malformed), effectivePolicy);
                if (!owner.getAsBoolean()) return;
                stream.ack(spec.group(), id);
                retention.markAcknowledged(effectivePolicy, stream);
                return;
            }
            RAtomicLong attempts = client.getAtomicLong(keys.key("message-failures", identity));
            RAtomicLong retryAt = client.getAtomicLong(keys.key("message-retry-at", identity));
            if (retryAt.get() > System.currentTimeMillis()) {
                metrics.increment("rdt.queue.reclaim", "result", "skipped_not_due");
                return;
            }
            client.getAtomicLong(keys.key("message-delivery", identity)).incrementAndGet();
            client.getAtomicLong(keys.key("message-delivery", identity)).expire(failureRecorder.attemptsTtl(effectivePolicy));
            long attempt = attempts.get();
            // 尝试计数只服务于当前消息的重试预算；消费者崩溃或 Stream 被裁剪后
            // 没有机会执行 delete 时，也必须让这个辅助键自动退出。
            attempts.expire(failureRecorder.attemptsTtl(effectivePolicy));
            if (attempt >= effectivePolicy.maxAttempts()) {
                failureRecorder.saveFailure(failureRecorder.failure(spec, message, attempt,
                        new RdtQueueException("Failure budget exhausted after recovery")), effectivePolicy);
                if (!owner.getAsBoolean()) return;
                stream.ack(spec.group(), id);
                retryIndex.retries(spec).remove(id.toString());
                attempts.delete();
                retention.markAcknowledged(effectivePolicy, stream);
                return;
            }
            try {
                listenerActive.computeIfAbsent(spec.listenerId(), ignored -> new AtomicInteger()).incrementAndGet();
                try { handler.handle(message); }
                finally { listenerActive.get(spec.listenerId()).decrementAndGet(); }
            } catch (Exception e) {
                if (e instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    return;
                }
                attempt = attempts.incrementAndGet();
                attempts.expire(failureRecorder.attemptsTtl(effectivePolicy));
                failureRecorder.recordFirstFailure(spec, message, effectivePolicy);
                log.warn("Message failed id={} group={} attempt={}", message.id(), spec.group(), attempt, e);
                if (!owner.getAsBoolean()) return;
                if (!retryClassifier.isRetryable(e) || !effectivePolicy.retry().canRetry((int) attempt)) {
                    failureRecorder.saveFailure(failureRecorder.failure(spec, message, attempt, e), effectivePolicy);
                    if (!owner.getAsBoolean()) return;
                    stream.ack(spec.group(), id);
                    retryIndex.retries(spec).remove(id.toString());
                    attempts.delete();
                    retryAt.delete();
                    retention.markAcknowledged(effectivePolicy, stream);
                } else {
                    long due = System.currentTimeMillis() + effectivePolicy.retry().delayAfter((int) attempt).toMillis();
                    retryAt.set(due);
                    retryAt.expire(failureRecorder.attemptsTtl(effectivePolicy));
                    // Normal retry timing is independent of crash visibility. Claim/submit failures keep Pending.
                    retryIndex.retries(spec).add(due, id.toString());
                    retryIndex.retries(spec).expire(failureRecorder.attemptsTtl(effectivePolicy));
                }
                return;
            }
            if (Thread.currentThread().isInterrupted() || !owner.getAsBoolean()) return;
            stream.ack(spec.group(), id);
            retryIndex.retries(spec).remove(id.toString());
            attempts.delete();
            retryAt.delete();
            retention.markAcknowledged(effectivePolicy, stream);
        }
    }

}
