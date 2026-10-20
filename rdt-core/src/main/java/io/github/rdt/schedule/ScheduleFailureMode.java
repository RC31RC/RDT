package io.github.rdt.schedule;

/** Whether a failed invocation ends its trigger or starts a bounded retry chain. */
public enum ScheduleFailureMode {
    /** Record the failure and wait for the next regular cron/rate/delay trigger. */
    CONTINUE,
    /** Retry this trigger first; coalesce regular triggers missed during the retry chain. */
    RETRY
}
