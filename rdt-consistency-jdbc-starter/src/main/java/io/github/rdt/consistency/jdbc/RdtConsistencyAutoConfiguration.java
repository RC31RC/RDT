package io.github.rdt.consistency.jdbc;

import io.github.rdt.consistency.ConsumerGuard;
import io.github.rdt.consistency.InboxStore;
import io.github.rdt.consistency.OutboxStore;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.queue.FailedMessageStore;
import io.github.rdt.queue.MessageQueueTemplate;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.spring.config.RdtAutoConfiguration;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;

@AutoConfiguration(before = RdtAutoConfiguration.class, afterName = {
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration"})
@ConditionalOnProperty(prefix = "rdt", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnProperty(prefix = "rdt.consistency", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(RdtConsistencyProperties.class)
public class RdtConsistencyAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public JdbcDialect rdtJdbcDialect(DataSource source) {
        return JdbcDialect.detect(source);
    }

    @Bean
    @ConditionalOnMissingBean(OutboxStore.class)
    public JdbcOutboxStore rdtOutboxStore(DataSource source, PlatformTransactionManager manager,
                                         JdbcDialect dialect, JsonCodec json, RdtConsistencyProperties properties) {
        return new JdbcOutboxStore(source, manager, dialect, json, properties.schema());
    }

    @Bean
    @ConditionalOnMissingBean(InboxStore.class)
    public JdbcInboxStore rdtInboxStore(DataSource source, PlatformTransactionManager manager,
                                       JdbcDialect dialect, RdtConsistencyProperties properties) {
        return new JdbcInboxStore(source, manager, dialect, properties.schema());
    }

    @Bean
    @ConditionalOnMissingBean(ConsumerGuard.class)
    public ConsumerGuard rdtInboxGuard(InboxStore store, PlatformTransactionManager manager,
                                       RdtConsistencyProperties properties) {
        return new JdbcInboxGuard(store, manager, properties.inboxEnabled());
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    public OutboxRelay rdtOutboxRelay(OutboxStore store, MessageQueueTemplate messages, RuntimeIdentity identity,
                                      KeySpace keys, RdtConsistencyProperties properties) {
        return new OutboxRelay(store, messages, identity, keys.namespace(), properties);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnBean({JdbcOutboxStore.class, JdbcInboxStore.class})
    @ConditionalOnMissingBean
    public ConsistencyCleanup rdtConsistencyCleanup(JdbcOutboxStore outbox, JdbcInboxStore inbox,
                                                     DistributedLockTemplate locks, KeySpace keys,
                                                     RdtConsistencyProperties properties) {
        return new ConsistencyCleanup(outbox, inbox, locks, keys.namespace(), properties);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(JdbcOutboxStore.class)
    public JdbcOutboxManagement rdtOutboxManagement(DataSource source, PlatformTransactionManager manager,
                                                    JdbcDialect dialect, KeySpace keys,
                                                    RdtConsistencyProperties properties) {
        return new JdbcOutboxManagement(source, manager, dialect, keys.namespace(), properties.schema());
    }

    @Bean
    @ConditionalOnProperty(prefix = "rdt.consistency", name = "jdbc-dead-letters", havingValue = "true")
    @ConditionalOnMissingBean(FailedMessageStore.class)
    public JdbcFailedMessageStore rdtJdbcDeadLetters(DataSource source, PlatformTransactionManager manager,
                                                     JdbcDialect dialect, JsonCodec json, KeySpace keys,
                                                     RdtConsistencyProperties properties) {
        return new JdbcFailedMessageStore(source, manager, dialect, json, keys.namespace(), properties.schema());
    }
}
