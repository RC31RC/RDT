package io.github.rdt.spring.config;

import io.github.rdt.exception.RdtConfigurationException;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("rdt.instance")
public record RdtInstanceProperties(
        @DefaultValue("30s")
        Duration lease,
        @DefaultValue("5s")
        Duration renewInterval) {
    public RdtInstanceProperties {
        if (lease == null || renewInterval == null || renewInterval.toMillis() < 1
                || lease.compareTo(renewInterval.multipliedBy(3)) < 0) {
            throw new RdtConfigurationException("Instance lease must cover at least three positive renewal intervals");
        }
    }
}
