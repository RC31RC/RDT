package io.github.rdt.schedule;

/** Handling of a normal trigger later than its misfire threshold. Retries are never skipped. */
public enum ScheduleMisfirePolicy {
    /** Coalesce missed triggers into one invocation, without a catch-up burst. */
    FIRE_ONCE,
    /** Advance to the next regular trigger without invoking the missed one. */
    SKIP
}
