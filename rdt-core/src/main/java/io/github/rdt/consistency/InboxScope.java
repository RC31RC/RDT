package io.github.rdt.consistency;

import java.util.Objects;

public record InboxScope(String namespace, String messageId, String consumerGroup) {
    public InboxScope {
        Objects.requireNonNull(namespace); Objects.requireNonNull(messageId); Objects.requireNonNull(consumerGroup);
    }
}
