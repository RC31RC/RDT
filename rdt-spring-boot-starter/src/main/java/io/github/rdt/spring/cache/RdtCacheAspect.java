package io.github.rdt.spring.cache;

import io.github.rdt.exception.RdtLockException;
import io.github.rdt.cache.CacheEntry;
import io.github.rdt.cache.CacheService;
import io.github.rdt.cache.RdtCacheEvict;
import io.github.rdt.cache.RdtCachePut;
import io.github.rdt.cache.RdtCacheable;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.spring.support.RdtMethodSupport;
import java.lang.reflect.Method;
import java.util.List;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** 缓存切面在显式锁切面内层、默认事务切面外层执行。 */
@Aspect
@Order(-100)
public final class RdtCacheAspect {
    private final CacheService cache;
    private final DistributedLockTemplate locks;
    private final RdtMethodSupport methods;

    /**
     * 创建缓存切面。
     * @param cache 功能模板
     * @param locks 缓存回源互斥锁
     * @param methods 表达式解析支持组件
     */
    public RdtCacheAspect(CacheService cache, DistributedLockTemplate locks, RdtMethodSupport methods) {
        this.cache = cache;
        this.locks = locks;
        this.methods = methods;
    }

    /**
     * 在分布式锁内回源；已存在外层事务时绕过缓存，避免缓存未提交数据。
     * @param point 方法调用
     * @param annotation 缓存定义
     * @return 缓存命中值或回源结果
     * @throws Throwable 原始业务异常
     */
    @Around("@annotation(annotation)")
    public Object cacheable(ProceedingJoinPoint point, RdtCacheable annotation) throws Throwable {
        CacheService selected = annotation.region().isBlank() ? cache : cache.region(annotation.region());
        Method method = methods.method(point);
        methods.validate(method);
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            return point.proceed();
        }
        String key = methods.key(annotation.key(), method, point.getTarget(), point.getArgs(), null);
        CacheEntry<Object> entry = selected.get(key, method.getGenericReturnType());
        if (entry instanceof CacheEntry.Hit<Object>(Object value)) {
            return value;
        }
        try (LockHandle ignored = locks.tryLock("cache-load:" + key, LockOptions.defaults()).orElseThrow(
                () -> new RdtLockException(RdtErrorCode.LOCK_TIMEOUT, "Cache loader deadline exceeded"))) {
            // 在选中 region 内二次检查；getAll 与普通读取遵守同一有限陈旧策略。
            CacheEntry<Object> second = selected.<Object>getAll(List.of(key), method.getGenericReturnType()).get(key);
            if (second instanceof CacheEntry.Hit<Object>(Object value)) {
                return value;
            }
            Object result = point.proceed();
            if (result != null || annotation.cacheNull()) {
                afterCommit(() -> selected.put(key, result, methods.duration(annotation.ttl(), null)));
            }
            return result;
        }
    }

    /**
     * 仅在业务成功且事务提交后更新缓存。
     * @param point 方法调用
     * @param annotation 缓存写入定义
     * @return 方法结果
     * @throws Throwable 原始异常
     */
    @Around("@annotation(annotation)")
    public Object put(ProceedingJoinPoint point, RdtCachePut annotation) throws Throwable {
        CacheService selected = annotation.region().isBlank() ? cache : cache.region(annotation.region());
        Method method = methods.method(point);
        methods.validate(method);
        Object result = point.proceed();
        String key = methods.key(annotation.key(), method, point.getTarget(), point.getArgs(), result);
        afterCommit(() -> selected.put(key, result, methods.duration(annotation.ttl(), null)));
        return result;
    }

    /**
     * 在配置指定的执行边界清除缓存。
     * @param point 方法调用
     * @param annotation 缓存清除定义
     * @return 方法结果
     * @throws Throwable 原始异常
     */
    @Around("@annotation(annotation)")
    public Object evict(ProceedingJoinPoint point, RdtCacheEvict annotation) throws Throwable {
        CacheService selected = annotation.region().isBlank() ? cache : cache.region(annotation.region());
        Method method = methods.method(point);
        methods.validate(method);
        String key = methods.key(annotation.key(), method, point.getTarget(), point.getArgs(), null);
        if (annotation.beforeInvocation()) {
            selected.evict(key);
            return point.proceed();
        }
        Object result = point.proceed();
        key = methods.key(annotation.key(), method, point.getTarget(), point.getArgs(), result);
        String evictedKey = key;
        afterCommit(() -> selected.evict(evictedKey));
        return result;
    }

    private void afterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()
                && TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new CacheSynchronization(action));
        } else {
            action.run();
        }
    }

    private record CacheSynchronization(Runnable action) implements TransactionSynchronization {
        @Override
        public void afterCommit() {
            action.run();
        }
    }
}
