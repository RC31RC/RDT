package io.github.rdt.spring.config;

import io.github.rdt.schedule.JobRoute;
import io.github.rdt.schedule.ScheduleFailureMode;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Component overrides take precedence over annotations, which inherit application defaults. */
@ConfigurationProperties("rdt.components")
public record RdtComponentProperties(Map<String, Listener> listeners, Map<String, Job> jobs) {
    public RdtComponentProperties {
        listeners = listeners == null ? Map.of() : Map.copyOf(listeners);
        jobs = jobs == null ? Map.of() : Map.copyOf(jobs);
    }
    public record Listener(Integer concurrency, Integer maxAttempts, Duration retryDelay,
                           Duration visibilityTimeout, Duration pollTimeout, Integer orderedPartitions) { }
    public record Job(Integer maxAttempts, Duration retryDelay, ScheduleFailureMode failureMode,
                      JobRoute route, String targetInstanceId) { }
}
