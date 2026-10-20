package io.github.rdt.schedule;

@FunctionalInterface
public interface CronSchedule { long next(long epochMillis); }
