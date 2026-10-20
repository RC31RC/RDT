package io.github.rdt.redisson.queue;

import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.FailedMessageStore;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.support.KeySpace;
import java.time.Duration;
import java.time.Instant;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

final class MessageFailureRecorder {
    private final RedissonClient client;
    private final KeySpace keys;
    private final FailedMessageStore failures;
    private final String instanceId;
    private volatile RdtMetrics metrics=RdtMetrics.NONE;
    void metrics(RdtMetrics value) { metrics=value; }
    MessageFailureRecorder(RedissonClient client,KeySpace keys,FailedMessageStore failures,String instanceId) {
        this.client=client;this.keys=keys;this.failures=failures;this.instanceId=instanceId;
    }
    String firstFailureKey(QueueSubscription spec, Message message) {
        return keys.key("message-first-failure",spec.destination()+":"+spec.group()+":"+message.id()
                +(spec.mode()==QueueMode.BROADCAST ? ":"+instanceId : ""));
    }
    void recordFirstFailure(QueueSubscription spec, Message message, QueuePolicy effective) {
        client.<String>getBucket(firstFailureKey(spec,message),StringCodec.INSTANCE)
                .setIfAbsent(Instant.now().toString(),attemptsTtl(effective));
    }
    void saveFailure(FailedMessage failure, QueuePolicy effective) {
        failures.save(failure,effective.deadLetterTtl());
        metrics.increment("rdt.queue.dlq.saved","destination",failure.destination(),"mode",failure.mode().name());
    }

    FailedMessage failure(QueueSubscription spec, Message message, long attempt, Exception error) {
        String detail = error.getClass().getName() + ": " + error.getMessage();
        String at=Instant.now().toString();
        String first=client.<String>getBucket(firstFailureKey(spec,message),StringCodec.INSTANCE).get();
        return new FailedMessage(spec.destination(), spec.group(), message, attempt,
                detail.substring(0, Math.min(detail.length(), 2000)), at, spec.mode(), instanceId,
                message.headers().getOrDefault("originalMessageId",message.id()),first==null ? at : first);
    }

    Duration attemptsTtl(QueuePolicy effectivePolicy) {
        if (!effectivePolicy.streamRetention().isZero()) {
            return effectivePolicy.streamRetention();
        }
        long visibilityMillis = effectivePolicy.visibilityTimeout().toMillis();
        long attemptsMillis = Math.multiplyExact(visibilityMillis, effectivePolicy.maxAttempts() + 1L);
        return Duration.ofMillis(Math.max(attemptsMillis, Duration.ofHours(1).toMillis()));
    }


}
