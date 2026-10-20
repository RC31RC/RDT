package io.github.rdt.console.config;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("rdt.console")
public record RdtConsoleProperties(
        @DefaultValue("/rdt/management")
        String apiPath,
        @DefaultValue("12h")
        Duration tokenTtl) {
    public RdtConsoleProperties {
        RdtSupport.require(apiPath != null && apiPath.startsWith("/") && !apiPath.contains(" ")
                && !apiPath.endsWith("/"), "Console API path must start with '/' and have no trailing slash or spaces");
        RdtSupport.positive(tokenTtl, "Console session TTL");
        RdtSupport.require(tokenTtl.compareTo(Duration.ofDays(7)) <= 0, "Console session TTL must not exceed seven days");
    }
}
