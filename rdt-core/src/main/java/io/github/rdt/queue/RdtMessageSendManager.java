package io.github.rdt.queue;

/** The method selects delivery timing; the business descriptor supplies the event and its destination. */
public interface RdtMessageSendManager {
    PublishReceipt send(MessageDescriptor<?> message);
    PublishReceipt sendAfterCommit(MessageDescriptor<?> message);
    PublishReceipt sendOutbox(MessageDescriptor<?> message);
}
