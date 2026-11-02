package io.github.rdt.spring;

import io.github.rdt.cache.CacheEntry;
import io.github.rdt.cache.CacheService;
import io.github.rdt.cache.RdtCacheEvict;
import io.github.rdt.cache.RdtCachePut;
import io.github.rdt.cache.RdtCacheable;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.DistributedLockManager;
import io.github.rdt.lock.DefaultLockManager;
import io.github.rdt.lock.LockKey;
import io.github.rdt.lock.RdtLock;
import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.Message;
import io.github.rdt.spring.cache.RdtCacheAspect;
import io.github.rdt.spring.lock.RdtLockAspect;
import io.github.rdt.spring.support.RdtMethodSupport;
import io.github.rdt.support.JsonCodec;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

/** 代理、事务边界和死信持久化测试。 */
class AspectAndJdbcTest {
    private JdbcDataSource source() {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        return source;
    }

    private Fixture proxy(CacheService cache, DistributedLockTemplate locks) {
        DistributedLockManager lockManager = new DefaultLockManager(locks);
        AspectJProxyFactory factory = new AspectJProxyFactory(new Fixture(lockManager));
        factory.setProxyTargetClass(true);
        factory.addAspect(new RdtCacheAspect(cache, locks, new RdtMethodSupport()));
        factory.addAspect(new RdtLockAspect(lockManager));
        return factory.getProxy();
    }

    @Test
    void lockReleasesAfterBusinessFailureAndPreservesSuppressedCloseFailure() {
        DistributedLockTemplate locks = mock(DistributedLockTemplate.class);
        when(locks.defaultOptions()).thenReturn(io.github.rdt.lock.LockOptions.defaults());
        LockHandle handle = mock(LockHandle.class);
        when(locks.tryLock(eq("Fixture:7"), any())).thenReturn(Optional.of(handle));
        Fixture bean = proxy(mock(CacheService.class), locks);
        assertThatThrownBy(() -> bean.fail(7)).isInstanceOf(IllegalStateException.class).hasMessage("business");
        verify(handle).close();
        doThrow(new IllegalStateException("close")).when(handle).close();
        assertThatThrownBy(() -> bean.fail(7)).hasMessage("business")
            .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
    }

    @Test
    void cacheHitAndSingleFlightMissInvokeCorrectly() {
        CacheService cache = mock(CacheService.class);
        DistributedLockTemplate locks = mock(DistributedLockTemplate.class);
        when(locks.defaultOptions()).thenReturn(io.github.rdt.lock.LockOptions.defaults());
        LockHandle handle = mock(LockHandle.class);
        when(cache.get("item:7", String.class)).thenReturn(new CacheEntry.Hit<>("cached"));
        when(cache.get("item:8", String.class)).thenReturn(new CacheEntry.Miss<>());
        when(cache.getAll(List.of("item:8"), String.class)).thenReturn(Map.of("item:8", new CacheEntry.Miss<>()));
        when(locks.tryLock(eq("cache-load:item:8"), any())).thenReturn(Optional.of(handle));
        Fixture bean = proxy(cache, locks);
        assertThat(bean.read(7)).isEqualTo("cached");
        assertThat(bean.read(8)).isEqualTo("value:8");
        verify(cache).put("item:8", "value:8", Duration.ofSeconds(5));
        verify(handle).close();
    }

    @Test
    void cacheMutationWaitsForCommitAndDoesNotRunOnRollback() {
        CacheService cache = mock(CacheService.class);
        Fixture bean = proxy(cache, mock(DistributedLockTemplate.class));
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(source()));
        tx.executeWithoutResult(status -> {
            bean.put(9);
            verify(cache, never()).put(any(), any(), any());
        });
        verify(cache).put(eq("item:value:9"), eq("value:9"), isNull());
        clearInvocations(cache);
        tx.executeWithoutResult(status -> {
            bean.put(10);
            bean.evict(10);
            status.setRollbackOnly();
        });
        verifyNoInteractions(cache);
        tx.executeWithoutResult(status -> assertThat(bean.read(10)).isEqualTo("value:10"));
        verifyNoInteractions(cache);
    }

    /** 通过真实 Spring 代理调用的非 final 同步服务。 */
    public static class Fixture {
        private final AtomicInteger loads = new AtomicInteger();
        private final DistributedLockManager lockManager;

        public Fixture(DistributedLockManager lockManager) {
            this.lockManager = lockManager;
        }

        /** @param id 业务标识 @return 不会返回的结果 */
        @RdtLock
        public String fail(int id) {
            lockManager.lock(LockKey.forResource(Fixture.class, id));
            throw new IllegalStateException("business");
        }

        /** @param id 业务标识 @return 回源值 */
        @RdtCacheable(key = "'item:' + #id", ttl = "PT5S")
        public String read(int id) {
            loads.incrementAndGet();
            return "value:" + id;
        }

        /** @param id 业务标识 @return 更新后的值 */
        @RdtCachePut(key = "'item:' + #result")
        public String put(int id) {
            return "value:" + id;
        }

        /** @param id 业务标识 */
        @RdtCacheEvict(key = "'item:' + #id")
        public void evict(int id) {}
    }
}
