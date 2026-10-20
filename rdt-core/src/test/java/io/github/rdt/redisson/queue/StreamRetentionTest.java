package io.github.rdt.redisson.queue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.support.KeySpace;

import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;

class StreamRetentionTest {
    @Test
    void acknowledgedMessagesOnlyMarkLocalStateWithoutRedisIo() {
        var client = mock(RedissonClient.class);
        var policy = mock(QueuePolicy.class);
        when(policy.autoDeleteAfterAck()).thenReturn(true);
        @SuppressWarnings("unchecked")
        RStream<String, String> stream = mock(RStream.class);
        when(stream.getName()).thenReturn("events");
        var retention = new StreamRetention(policy, client, new KeySpace("retention-test"));

        for (int i = 0; i < 1000; i++) retention.markAcknowledged(policy, stream);

        verifyNoInteractions(client);
        verify(stream, times(1000)).getName();
        verifyNoMoreInteractions(stream);
    }

    @Test
    void disabledAutomaticDeletionDoesNotScheduleAcknowledgementCleanup() {
        var client = mock(RedissonClient.class);
        var policy = mock(QueuePolicy.class);
        @SuppressWarnings("unchecked")
        RStream<String, String> stream = mock(RStream.class);
        var retention = new StreamRetention(policy, client, new KeySpace("retention-test"));

        retention.markAcknowledged(policy, stream);
        retention.sweepAcknowledged();

        verifyNoInteractions(client, stream);
    }
}
