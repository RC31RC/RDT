package io.github.rdt.console.archive;

import io.github.rdt.support.RdtSupport;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.scheduling.support.CronExpression;

@ConfigurationProperties("rdt.console.archive")
public record RdtArchiveProperties(
        @DefaultValue("local")
        String sink,
        Path directory,
        @DefaultValue("0 0 2 * * ?")
        String cron,
        @DefaultValue("Asia/Shanghai")
        ZoneId zone,
        @DefaultValue("10m")
        Duration dispatchedTimeout,
        @DefaultValue("30")
        int retentionDays) {
    public RdtArchiveProperties {
        RdtSupport.require(sink != null && !sink.isBlank(), "Archive sink must be non-blank");
        directory = directory == null ? Path.of(System.getProperty("user.home"), ".rdt-console", "archive") : directory;
        RdtSupport.require(directory.isAbsolute(), "Archive directory must be absolute");
        directory = directory.normalize();
        CronExpression.parse(cron);
        RdtSupport.require(zone != null, "Archive time zone is required");
        RdtSupport.positive(dispatchedTimeout, "Archive dispatched timeout");
        RdtSupport.require(retentionDays > 0, "Archive retention days must be positive");
    }
}
