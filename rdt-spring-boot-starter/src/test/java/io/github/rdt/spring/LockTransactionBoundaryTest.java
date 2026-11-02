package io.github.rdt.spring;

import io.github.rdt.lock.DistributedLockManager;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.RdtLock;
import io.github.rdt.lock.DefaultLockManager;
import io.github.rdt.lock.LockKey;
import io.github.rdt.lock.LockOrder;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockScope;
import io.github.rdt.spring.config.RdtAutoConfiguration;
import io.github.rdt.spring.lock.RdtLockAspect;
import io.github.rdt.spring.lock.TransactionalLockManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 验证锁在无事务、当前事务和外层事务中的实际释放边界。
 */
class LockTransactionBoundaryTest {
    @Test
    void methodOwnedTransactionReleasesAfterCommit() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            Fixture bean = context.getBean(Fixture.class);
            bean.invoke();

            assertThat(bean.events()).containsExactly("business", "commit", "Fixture:test-unlock");
        }
    }

    @Test
    void joinedOuterTransactionReleasesAfterOuterCommit() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            Fixture bean = context.getBean(Fixture.class);
            new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).executeWithoutResult(status -> {
                bean.invoke();
                assertThat(bean.events()).containsExactly("business");
            });

            assertThat(bean.events()).containsExactly("business", "commit", "Fixture:test-unlock");
        }
    }

    @Test
    void rollbackReleasesAfterRollbackCompletion() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            Fixture bean = context.getBean(Fixture.class);
            new TransactionTemplate(context.getBean(PlatformTransactionManager.class)).executeWithoutResult(status -> {
                bean.invoke();
                status.setRollbackOnly();
            });

            assertThat(bean.events()).containsExactly("business", "rollback", "Fixture:test-unlock");
        }
    }

    @Test
    void requiresNewKeepsOuterLockUntilOuterTransactionCompletes() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            OuterFixture outer = context.getBean(OuterFixture.class);
            outer.invoke();

            assertThat(outer.events()).containsExactly("outer-business", "inner-business", "inner-commit",
                "InnerFixture:inner-unlock", "outer-commit", "OuterFixture:outer-unlock");
        }
    }

    private static JdbcDataSource dataSource() {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + UUID.randomUUID());
        return source;
    }

    private static final class FirstResource {}

    private static final class SecondResource {}

    @Test
    void noTransactionClosesAtMethodReturnAndAnnotationDoesNotAcquireByItself() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            Fixture bean = context.getBean(Fixture.class);
            bean.noKey();
            assertThat(bean.events()).containsExactly("no-key");
            bean.invokeWithoutTransaction();
            assertThat(bean.events()).containsExactly("no-key", "business", "Fixture:test-unlock");
        }
    }

    @Test
    void firstAcquisitionBeforeTransactionDoesNotAttachToFutureTransaction() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            var manager = context.getBean(DistributedLockManager.class);
            Events events = context.getBean(Events.class);
            try (LockScope scope = manager.beginScope()) {
                manager.lock(LockKey.named(Fixture.class, "before"));
                new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
                    .executeWithoutResult(status -> {
                        manager.lock(LockKey.named(Fixture.class, "during"));
                        assertThat(events.values).isEmpty();
                    });
                assertThat(events.values).isEmpty();
            }
            assertThat(events.values).containsExactly("Fixture:during-unlock", "Fixture:before-unlock");
        }
    }

    @Test
    void selfInvocationDoesNotTriggerLockAspect() {
        try (var context = new AnnotationConfigApplicationContext(Config.class)) {
            Fixture bean = context.getBean(Fixture.class);
            assertThatThrownBy(bean::selfInvokeWithoutProxy).hasMessageContaining("No active transactional lock scope");
        }
    }

    @Test
    void deferredScopeAndRequiresNewCannotBypassOrderOrReleaseOuterLock() {
        DistributedLockTemplate template = mock(DistributedLockTemplate.class);
        when(template.defaultOptions()).thenReturn(LockOptions.defaults());
        List<String> released = new ArrayList<>();
        when(template.tryLock(any(), any())).thenAnswer(call -> {
            String key = call.getArgument(0, String.class);
            LockHandle handle = mock(LockHandle.class);
            doAnswer(ignored -> {
                released.add(key);
                return null;
            }).when(handle).close();
            return Optional.of(handle);
        });
        DistributedLockManager manager = new TransactionalLockManager(
            new DefaultLockManager(template, new LockOrder(List.of(FirstResource.class, SecondResource.class))));
        PlatformTransactionManager transactions = new DataSourceTransactionManager(dataSource());
        LockKey first = LockKey.named(FirstResource.class, "a");
        LockKey second = LockKey.named(SecondResource.class, "b");
        TransactionTemplate outerTransaction = new TransactionTemplate(transactions);
        TransactionTemplate innerTransaction = new TransactionTemplate(transactions);
        innerTransaction.setPropagationBehavior(Propagation.REQUIRES_NEW.value());
        outerTransaction.executeWithoutResult(status -> {
            try (LockScope scope = manager.beginScope()) {
                manager.lock(second);
            }
            assertThat(released).isEmpty();
            assertThatThrownBy(() -> {
                try (LockScope scope = manager.beginScope()) {
                    manager.lock(first);
                }
            }).hasMessageContaining("order inversion");
            innerTransaction.executeWithoutResult(inner -> {
                assertThatThrownBy(() -> {
                    try (LockScope scope = manager.beginScope()) {
                        manager.lock(first);
                    }
                }).hasMessageContaining("order inversion");
            });
            assertThat(released).isEmpty();
        });
        assertThat(released).containsExactly(second.value());
        try (LockScope scope = manager.beginScope()) {
            manager.lock(first);
        }
        assertThat(released).containsExactly(second.value(), first.value());
    }

    @Test
    void openScopeBoundToOuterTransactionCannotAcquireInsideRequiresNew() {
        DistributedLockTemplate template = mock(DistributedLockTemplate.class);
        when(template.defaultOptions()).thenReturn(LockOptions.defaults());
        when(template.tryLock(any(), any())).thenAnswer(call -> Optional.of(mock(LockHandle.class)));
        DistributedLockManager manager = new TransactionalLockManager(new DefaultLockManager(template));
        PlatformTransactionManager transactions = new DataSourceTransactionManager(dataSource());
        TransactionTemplate outer = new TransactionTemplate(transactions);
        TransactionTemplate inner = new TransactionTemplate(transactions);
        inner.setPropagationBehavior(Propagation.REQUIRES_NEW.value());
        outer.executeWithoutResult(status -> {
            try (LockScope scope = manager.beginScope()) {
                manager.lock(LockKey.named(FirstResource.class, "a"));
                inner.executeWithoutResult(
                    innerStatus -> assertThatThrownBy(() -> manager.lock(LockKey.named(FirstResource.class, "b")))
                        .hasMessageContaining("suspended"));
            }
        });
        assertThat(manager.currentScope()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void starterWiresOptionalFixedLockOrderBean() {
        DistributedLockTemplate template = mock(DistributedLockTemplate.class);
        when(template.defaultOptions()).thenReturn(LockOptions.defaults());
        when(template.tryLock(any(), any())).thenAnswer(call -> Optional.of(mock(LockHandle.class)));
        ObjectProvider<LockOrder> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(new LockOrder(List.of(FirstResource.class, SecondResource.class)));
        DistributedLockManager manager = new RdtAutoConfiguration().rdtLockManager(template, provider);
        try (LockScope scope = manager.beginScope()) {
            manager.lock(LockKey.named(SecondResource.class, "b"));
            assertThatThrownBy(() -> manager.lock(LockKey.named(FirstResource.class, "a")))
                .hasMessageContaining("order inversion");
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    @EnableTransactionManagement
    static class Config {
        @Bean
        PlatformTransactionManager transactionManager() {
            JdbcDataSource source = new JdbcDataSource();
            source.setURL("jdbc:h2:mem:" + UUID.randomUUID());
            return new DataSourceTransactionManager(source);
        }

        @Bean
        Events events() {
            return new Events();
        }

        @Bean
        DistributedLockManager lockManager(Events events) {
            DistributedLockTemplate template = mock(DistributedLockTemplate.class);
            when(template.defaultOptions()).thenReturn(LockOptions.defaults());
            when(template.tryLock(any(), any())).thenAnswer(call -> {
                String key = call.getArgument(0, String.class);
                LockHandle handle = mock(LockHandle.class);
                doAnswer(ignored -> {
                    events.values.add(key + "-unlock");
                    return null;
                }).when(handle).close();
                return Optional.of(handle);
            });
            return new TransactionalLockManager(new DefaultLockManager(template));
        }

        @Bean
        RdtLockAspect aspect(DistributedLockManager manager) {
            return new RdtLockAspect(manager);
        }

        @Bean
        Fixture fixture(DistributedLockManager manager, Events events) {
            return new Fixture(manager, events);
        }

        @Bean
        InnerFixture innerFixture(DistributedLockManager manager, Events events) {
            return new InnerFixture(manager, events);
        }

        @Bean
        OuterFixture outerFixture(DistributedLockManager manager, Events events, InnerFixture inner) {
            return new OuterFixture(manager, events, inner);
        }
    }

    static class Events {
        private final List<String> values = new ArrayList<>();
    }

    public static class Fixture {
        private final DistributedLockManager manager;
        private final Events events;

        Fixture(DistributedLockManager manager, Events events) {
            this.manager = manager;
            this.events = events;
        }

        public List<String> events() {
            return List.copyOf(events.values);
        }

        @RdtLock
        public void noKey() {
            events.values.add("no-key");
        }

        @RdtLock
        public void invokeWithoutTransaction() {
            manager.lock(LockKey.named(Fixture.class, "test"));
            events.values.add("business");
        }

        public void selfInvokeWithoutProxy() {
            invokeWithoutTransaction();
        }

        @RdtLock
        @Transactional
        public void invoke() {
            manager.lock(LockKey.named(Fixture.class, "test"));
            events.values.add("business");
            registerCompletionEvents(events, "commit", "rollback");
        }
    }

    public static class OuterFixture {
        private final DistributedLockManager manager;
        private final Events events;
        private final InnerFixture inner;

        OuterFixture(DistributedLockManager manager, Events events, InnerFixture inner) {
            this.manager = manager;
            this.events = events;
            this.inner = inner;
        }

        public List<String> events() {
            return List.copyOf(events.values);
        }

        @RdtLock
        @Transactional
        public void invoke() {
            manager.lock(LockKey.named(OuterFixture.class, "outer"));
            events.values.add("outer-business");
            registerCompletionEvents(events, "outer-commit", "outer-rollback");
            inner.invoke();
        }
    }

    public static class InnerFixture {
        private final DistributedLockManager manager;
        private final Events events;

        InnerFixture(DistributedLockManager manager, Events events) {
            this.manager = manager;
            this.events = events;
        }

        @RdtLock
        @Transactional(propagation = Propagation.REQUIRES_NEW)
        public void invoke() {
            manager.lock(LockKey.named(InnerFixture.class, "inner"));
            events.values.add("inner-business");
            registerCompletionEvents(events, "inner-commit", "inner-rollback");
        }
    }

    private static void registerCompletionEvents(Events events, String commitEvent, String rollbackEvent) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                events.values.add(status == TransactionSynchronization.STATUS_COMMITTED ? commitEvent : rollbackEvent);
            }

            @Override
            public int getOrder() {
                return Integer.MAX_VALUE - 1;
            }
        });
    }
}
