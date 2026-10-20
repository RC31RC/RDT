package io.github.rdt.console.alert;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("rdt.console.alerts")
public record RdtAlertProperties(
        @DefaultValue("30s")
        Duration pollInterval,
        @DefaultValue("1m")
        Duration repeatInterval,
        @DefaultValue("5m")
        Duration recentWindow,
        @DefaultValue("1")
        int warningCount,
        @DefaultValue("100")
        int criticalCount,
        @DefaultValue("20")
        int recentCriticalCount,
        @DefaultValue("10m")
        Duration oldestWarningAge) {
    public RdtAlertProperties {
        RdtSupport.positive(pollInterval, "Alert polling interval");
        RdtSupport.positive(repeatInterval, "Alert repeat interval");
        RdtSupport.positive(recentWindow, "Alert recent window");
        RdtSupport.positive(oldestWarningAge, "Alert oldest age");
        RdtSupport.require(warningCount > 0 && criticalCount >= warningCount && recentCriticalCount > 0,
                "Invalid dead letter alert thresholds");
    }
}
