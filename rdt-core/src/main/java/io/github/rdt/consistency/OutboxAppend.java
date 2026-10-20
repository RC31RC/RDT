package io.github.rdt.consistency;

import io.github.rdt.queue.Message;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.support.RdtSupport;
import java.util.Objects;

/** Full immutable envelope; relay is at-least-once and deliberately does not preserve ordering. */
public record OutboxAppend(String namespace, String destination, QueueMode queueMode, Message message) {
    public OutboxAppend {
        RdtSupport.component(namespace);
        RdtSupport.component(destination);
        Objects.requireNonNull(message, "message");
        RdtSupport.require(queueMode == QueueMode.STREAM || queueMode == QueueMode.RELIABLE_BROADCAST,
                "Outbox requires durable STREAM or RELIABLE_BROADCAST transport");
        RdtSupport.require(message.orderingKey() == null,
                "Outbox does not preserve order; use MessageQueueTemplate.sendOrdered outside Outbox");
    }
}
