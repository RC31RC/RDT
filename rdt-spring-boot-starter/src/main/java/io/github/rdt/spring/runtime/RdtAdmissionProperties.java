package io.github.rdt.spring.runtime;

import io.github.rdt.runtime.AdaptiveAdmissionController;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("rdt.admission")
public record RdtAdmissionProperties(
        @DefaultValue("true")
        boolean enabled,
        @DefaultValue("2s")
        Duration sampleInterval,
        @DefaultValue("0.85")
        double cpuPause,
        @DefaultValue("0.65")
        double cpuResume,
        @DefaultValue("0.90")
        double databasePause,
        @DefaultValue("0.70")
        double databaseResume,
        @DefaultValue("5s")
        Duration recoveryDelay) {
    public RdtAdmissionProperties {
        RdtSupport.positive(sampleInterval, "Pressure sampling interval");
        new AdaptiveAdmissionController(cpuPause, cpuResume, databasePause, databaseResume, recoveryDelay);
    }
}
