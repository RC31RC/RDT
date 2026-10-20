package io.github.rdt.schedule;

import java.time.ZoneId;

@FunctionalInterface
public interface CronParser { CronSchedule parse(String expression,ZoneId zone); }
