package io.github.rdt.console.config;

import io.github.rdt.console.alert.RdtAlertMonitor;
import io.github.rdt.console.alert.RdtAlertProperties;
import io.github.rdt.console.auth.RdtConsoleAuthenticationFilter;
import io.github.rdt.console.security.ConsoleSecurityService;
import io.github.rdt.console.storage.ConsoleLocalFiles;
import io.github.rdt.console.connection.ConsoleConnectionManager;
import io.github.rdt.console.connection.ConsoleConnectionStore;
import io.github.rdt.console.connection.ConsoleConnectionProperties;
import io.github.rdt.console.connection.ConsoleRedisClientFactory;
import io.github.rdt.exception.RdtConfigurationException;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 只装配控制台所需的 Redis 管理组件，不创建业务侧 RDT 模板。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({RdtConsoleProperties.class, RdtAlertProperties.class, ConsoleConnectionProperties.class})
public class RdtConsoleConfiguration {
    @Bean
    public ApplicationRunner verifyConsoleFrontend() {
        return args -> {
            if (!new ClassPathResource("static/index.html").exists()) {
                throw new RdtConfigurationException("Console frontend is missing; run sh mvnw -pl rdt-console -am package before starting the application");
            }
        };
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public RdtAlertMonitor rdtAlertMonitor(ConsoleConnectionManager management, RdtAlertProperties properties) {
        return new RdtAlertMonitor(management, properties);
    }

    @Bean
    public ConsoleLocalFiles consoleLocalFiles(ConsoleConnectionProperties properties) {
        return new ConsoleLocalFiles(properties.directory());
    }

    @Bean
    public ConsoleSecurityService consoleSecurityService(ConsoleLocalFiles files, RdtConsoleProperties properties) {
        return new ConsoleSecurityService(files, properties.tokenTtl());
    }

    @Bean
    public RdtConsoleAuthenticationFilter rdtConsoleAuthenticationFilter(RdtConsoleProperties properties, ConsoleSecurityService security) {
        return new RdtConsoleAuthenticationFilter(properties, security);
    }

    @Bean
    public ConsoleConnectionStore consoleConnectionStore(ConsoleConnectionProperties properties) {
        return new ConsoleConnectionStore(properties.directory());
    }

    @Bean
    public ConsoleRedisClientFactory consoleRedisClientFactory(ConsoleConnectionProperties properties) {
        return new ConsoleRedisClientFactory(properties);
    }

    @Bean(destroyMethod = "close")
    public ConsoleConnectionManager consoleConnectionManager(ConsoleConnectionStore store, ConsoleRedisClientFactory factory) {
        return new ConsoleConnectionManager(store, factory);
    }
}
