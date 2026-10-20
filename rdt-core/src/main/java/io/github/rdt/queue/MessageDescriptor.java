package io.github.rdt.queue;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.rdt.support.JsonCodec;
import java.util.Objects;

/**
 * Application-owned typed event: business payload plus publication metadata.
 * Concrete types need a no-argument constructor for trusted startup inspection and JSON decoding.
 * Topic/mode and group policies must be independent of payload and incoming headers.
 */
@JsonAutoDetect(creatorVisibility = JsonAutoDetect.Visibility.NONE)
public abstract class MessageDescriptor<T> {
    @JsonProperty("payload")
    private T payload;
    @JsonIgnore
    private MessageMetadata metadata;

    protected MessageDescriptor() { this(null); }
    protected MessageDescriptor(T payload) { this(payload, MessageMetadata.create()); }
    protected MessageDescriptor(T payload, MessageMetadata metadata) {
        this.payload = payload;
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    public final T payload() { return payload; }
    public final MessageMetadata metadata() { return metadata; }
    public abstract String topic();
    public QueueMode mode() { return QueueMode.STREAM; }
    public ConsumerDescriptor consumer(String group) { return ConsumerDescriptor.defaults(); }

    public final Message envelope(JsonCodec json) {
        return new Message(metadata.messageId(), json.write(this), metadata.createdAt(), 1,
                metadata.key(), metadata.orderingKey(), metadata.headers());
    }

    /** The transport envelope, rather than decoded application data, owns the received identity. */
    public final void restoreMetadata(Message message) {
        metadata = new MessageMetadata(message.id(), message.createdAt(), message.key(), message.orderingKey(), message.headers());
    }
}
