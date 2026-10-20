package io.github.rdt.spring.schedule;

import io.github.rdt.schedule.CronParser;
import io.github.rdt.schedule.CronSchedule;
import java.time.Instant;
import java.time.ZoneId;
import org.springframework.scheduling.support.CronExpression;

public final class SpringCronParser implements CronParser {
    @Override
    public CronSchedule parse(String expression,ZoneId zone) {
        CronExpression cron=CronExpression.parse(expression);
        return epochMillis -> {
            var next=cron.next(Instant.ofEpochMilli(epochMillis).atZone(zone));
            return next==null ? Long.MAX_VALUE : next.toInstant().toEpochMilli();
        };
    }
}
