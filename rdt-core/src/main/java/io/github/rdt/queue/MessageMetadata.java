package io.github.rdt.queue;

import io.github.rdt.support.RdtSupport;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Identity belongs to one publication and remains unchanged during delivery and Outbox relay. */
public record MessageMetadata(String messageId, String createdAt, String key, String orderingKey,
                              Map<String, String> headers) {
    public MessageMetadata {
        RdtSupport.component(messageId);
        RdtSupport.require(createdAt != null && !createdAt.isBlank(), "Message creation time is required");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        RdtSupport.require(headers.size() <= 32, "Message headers must not exceed 32 entries");
    }

    public static MessageMetadata create() {
        return new MessageMetadata(UUID.randomUUID().toString(), Instant.now().toString(), null, null, Map.of());
    }
}
