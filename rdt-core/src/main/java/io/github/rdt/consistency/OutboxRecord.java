package io.github.rdt.consistency;

import io.github.rdt.queue.Message;
import io.github.rdt.queue.QueueMode;

public record OutboxRecord(String id, String namespace, String destination, QueueMode queueMode,
                           Message message, int failureCount, String claimToken) { }
