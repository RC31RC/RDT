package io.github.rdt.spring.config;

import io.github.rdt.exception.RdtConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import org.redisson.config.Config;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.core.io.Resource;

/** Native Redisson YAML is kept intact: topology, ACL/TLS, pools, timeouts and advanced options. */
@ConfigurationProperties("rdt.redis")
public record RdtRedisProperties(@DefaultValue Connection runtime, @DefaultValue Connection cache) {
    public record Connection(String config, Resource file) {
        public Connection {
            if (config != null && config.isBlank()) {
                throw new RdtConfigurationException("Explicit rdt.redis config must not be blank; remove it to use defaults");
            }
            if (config != null && !config.isBlank() && file != null) {
                throw new RdtConfigurationException("Choose one of Redisson config or file for each rdt.redis role");
            }
        }
        public boolean configured() { return file != null || config != null && !config.isBlank(); }
        public Config redisson() {
            try {
                if (file != null) {
                    try (InputStream stream = file.getInputStream()) {
                        return Config.fromYAML(stream);
                    }
                }
                if (config != null) return Config.fromYAML(config);
                Config defaults = new Config().setThreads(2).setNettyThreads(2);
                defaults.useSingleServer().setAddress("redis://127.0.0.1:6379")
                        .setConnectionMinimumIdleSize(1).setConnectionPoolSize(12)
                        .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(4);
                return defaults;
            } catch (IOException | IllegalArgumentException failure) {
                // Translate once at the native configuration boundary; do not hide startup errors.
                throw new RdtConfigurationException("Cannot load native rdt.redis config/file; check the resource and Redisson YAML options", failure);
            }
        }
    }
}
