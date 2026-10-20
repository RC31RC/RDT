package io.github.rdt.management;

/** Statistics concern retained dead letters, including those created in the recent window. */
public record DeadLetterStats(long retainedCount, long recentCount, long oldestAtMillis) { }
