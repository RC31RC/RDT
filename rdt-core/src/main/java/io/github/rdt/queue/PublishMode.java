package io.github.rdt.queue;

/** Explicit producer-side delivery timing; no mode implies a distributed exactly-once guarantee. */
public enum PublishMode {
    /** Immediate transport call; a later business rollback cannot retract delivery. */
    DIRECT,
    /** Best effort after a successful transaction commit; delivery failure is logged only. */
    AFTER_COMMIT,
    /** Persist the full envelope with business writes; relay at least once, without ordering. */
    OUTBOX
}
