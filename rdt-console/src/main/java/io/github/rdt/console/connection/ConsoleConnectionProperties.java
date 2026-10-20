package io.github.rdt.console.connection;

import io.github.rdt.support.RdtSupport;
import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("rdt.console.connections")
public record ConsoleConnectionProperties(Path directory,
        @DefaultValue("3s")
        Duration connectTimeout,
        @DefaultValue("10s")
        Duration commandTimeout) {
    public ConsoleConnectionProperties {
        directory = directory == null ? Path.of(System.getProperty("user.home"), ".rdt-console") : directory;
        RdtSupport.require(directory.isAbsolute(), "Console connections directory must be absolute");
        RdtSupport.positive(connectTimeout, "Redis connect timeout");
        RdtSupport.positive(commandTimeout, "Redis command timeout");
        RdtSupport.require(connectTimeout.toMillis() <= 30_000 && commandTimeout.toMillis() <= 60_000,
                "Redis timeouts exceed the Console resource budget");
    }
}
