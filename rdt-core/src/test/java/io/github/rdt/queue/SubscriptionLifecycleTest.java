package io.github.rdt.queue;

import io.github.rdt.support.RetryClassifier;
import io.github.rdt.redisson.queue.RedissonMessageQueueTemplate;

import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.exception.RdtException;

import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.Codec;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SubscriptionLifecycleTest {
    @Test
    void broadcastHandlersShareTheConfiguredGlobalExecutionBudget() throws Exception {
        var redis = mock(RedissonClient.class);
        var topic = mock(RTopic.class);
        var listener = new AtomicReference<MessageListener<String>>();
        when(redis.getTopic(anyString(), any(Codec.class))).thenReturn(topic);
        when(topic.addListener(eq(String.class), any(MessageListener.class))).thenAnswer(invocation -> {
            listener.set(invocation.getArgument(1));
            return 7;
        });
        var policy = new QueuePolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(1), true, Duration.ofDays(1), Duration.ofDays(1), 100, 2, Duration.ofMinutes(1), 1, 4, 8, 32, 262144, 10000, 67108864L, 16777216);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var handled = new AtomicInteger();
        var json = new JsonCodec();
        var queue = new RedissonMessageQueueTemplate(redis, new KeySpace("global-budget-test"), json, new QueuePolicy((policy).maxAttempts(), (policy).retryDelay(), (policy).visibilityTimeout(), (policy).pollTimeout(), (policy).shutdownTimeout(), (policy).autoDeleteAfterAck(), (policy).deadLetterTtl(), (policy).streamRetention(), (policy).streamMaxLength(), (policy).retryMultiplier(), (policy).maxRetryDelay(), 1, (policy).maxBlockingReads(), (policy).readBatchSize(), 32, 262144, 10000, 67108864L, 16777216), mock(DistributedLockTemplate.class), mock(FailedMessageStore.class), activeIdentity("instance-a"), RetryClassifier.infrastructureFailures());
        try {
            queue.subscribe(new QueueSubscription("events", QueueMode.BROADCAST, "group", 4, "", QueueOptions.defaults(), 0, "events" + ":" + "group"), message -> {
                handled.incrementAndGet();
                entered.countDown();
                release.await();
            });
            String encoded = json.write(Message.create("payload", json));
            listener.get().onMessage("topic", encoded);
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            listener.get().onMessage("topic", encoded);
            assertThat(handled.get()).isEqualTo(1);
        } finally {
            release.countDown();
            queue.close();
        }
    }

    @Test
    void unsubscribeFailureRemainsRetryableAndSuccessfulCloseIsIdempotent() {
        var redis = mock(RedissonClient.class);
        var topic = mock(RTopic.class);
        when(redis.getTopic(anyString(), any(Codec.class))).thenReturn(topic);
        when(topic.addListener(eq(String.class), any(MessageListener.class))).thenReturn(7);
        var policy = new QueuePolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(1), true, Duration.ofDays(1), Duration.ofDays(1), 100, 2, Duration.ofMinutes(1), 16, 4, 8, 32, 262144, 10000, 67108864L, 16777216);
        try (var queue = new RedissonMessageQueueTemplate(redis, new KeySpace("close-test"), new JsonCodec(), policy, mock(DistributedLockTemplate.class), mock(FailedMessageStore.class), activeIdentity("instance-b"), RetryClassifier.infrastructureFailures())) {
            var subscription = queue.subscribe(new QueueSubscription("events", QueueMode.BROADCAST, "group", 1, "", QueueOptions.defaults(), 0, "events" + ":" + "group"), message -> {});
            doThrow(new IllegalStateException("unsubscribe failed")).doNothing().when(topic).removeListener(7);
            assertThatThrownBy(subscription::close).hasRootCauseMessage("unsubscribe failed");
            subscription.close();
            subscription.close();
            verify(topic, times(2)).removeListener(7);
        }
        verify(topic, times(2)).removeListener(7);
    }

    @Test
    void reliableBroadcastRejectsGeneratedInstanceIdentityBeforeRedisAccess() {
        var redis = mock(RedissonClient.class);
        var policy = new QueuePolicy(3, Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(1), Duration.ofSeconds(1), true, Duration.ofDays(1), Duration.ofDays(1), 100, 2, Duration.ofMinutes(1), 16, 4, 8, 32, 262144, 10000, 67108864L, 16777216);
        try (var queue = new RedissonMessageQueueTemplate(redis, new KeySpace("identity-test"), new JsonCodec(),
                policy, mock(DistributedLockTemplate.class), mock(FailedMessageStore.class), new RuntimeIdentity(null), RetryClassifier.infrastructureFailures())) {
            assertThatThrownBy(() -> queue.subscribe(QueueSubscription.concurrent(
                    "events", QueueMode.RELIABLE_BROADCAST, "replicas", 1), message -> { }))
                    .isInstanceOf(RdtException.class).hasMessageContaining("stable instance ID");
        }
        verifyNoInteractions(redis);
    }

    private RuntimeIdentity activeIdentity(String id) {
        var identity = new RuntimeIdentity(id);
        identity.acquired("unit-test-token", 1, () -> true);
        return identity;
    }
}
