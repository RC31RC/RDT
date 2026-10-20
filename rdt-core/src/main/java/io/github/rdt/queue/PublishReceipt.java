package io.github.rdt.queue;

/** AFTER_COMMIT is a registration receipt, not proof of Redis delivery. */
public record PublishReceipt(String messageId, PublishMode mode, Status status, String outboxId) {
    public enum Status { SENT, AFTER_COMMIT_REGISTERED, OUTBOX_STORED }
}
