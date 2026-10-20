package io.github.rdt.consistency;

public record OutboxStats(long pending, long claimed, long published, long dead) { }
