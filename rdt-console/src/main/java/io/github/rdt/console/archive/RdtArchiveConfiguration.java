package io.github.rdt.console.archive;

import io.github.rdt.console.connection.ConsoleConnectionManager;
import io.github.rdt.support.JsonCodec;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "rdt.console.archive", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RdtArchiveProperties.class)
@EnableScheduling
@EnableAsync
public class RdtArchiveConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(RdtArchiveConfiguration.class);

    @Bean
    @ConditionalOnProperty(prefix = "rdt.console.archive", name = "sink", havingValue = "local", matchIfMissing = true)
    public RdtArchiveSinkFactory localFileArchiveSinkFactory(RdtArchiveProperties properties) {
        return date -> {
            var target = properties.directory().resolve("rdt-archive-" + date + ".jsonl.gz");
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) return Optional.empty();
            return Optional.of(new LocalFileArchiveSink(properties.directory(), date, properties.retentionDays()));
        };
    }

    @Bean
    public RdtArchiveJob rdtArchiveJob(ConsoleConnectionManager connections, RdtArchiveSinkFactory sinks,
                                     RdtArchiveProperties properties) {
        var json = new JsonCodec();
        var source = new RdtArchiveJob.Source() {
            @Override
            public boolean available() { return connections.runtimeConfigured(); }

            @Override
            public long read(Instant at, Consumer<List<String>> destination) {
                return connections.withRuntime((client, namespaces) ->
                        new RedisArchiveReader(client, json, namespaces, properties.dispatchedTimeout()).read(at, destination));
            }
        };
        return new RdtArchiveJob(source, sinks, Clock.system(properties.zone()));
    }

    @Bean
    public ThreadPoolTaskExecutor rdtArchiveExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(0);
        // A daily job must not retain an idle worker and its thread stack all day.
        executor.setAllowCoreThreadTimeOut(true);
        executor.setKeepAliveSeconds(30);
        executor.setThreadNamePrefix("rdt-archive-");
        // No backlog: an invocation arriving during a run is skipped immediately.
        executor.setRejectedExecutionHandler((task, pool) -> LOG.info("Skipping archive: executor is occupied or stopping"));
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
    }
}
