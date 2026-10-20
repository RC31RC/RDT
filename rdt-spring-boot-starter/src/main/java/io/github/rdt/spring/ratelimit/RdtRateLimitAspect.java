package io.github.rdt.spring.ratelimit;

import io.github.rdt.exception.RdtRateLimitException;
import io.github.rdt.exception.RdtException;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.ratelimit.RateLimiterTemplate;
import io.github.rdt.ratelimit.RdtRateLimit;
import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.spring.support.RdtMethodSupport;
import java.lang.reflect.Method;
import java.time.Duration;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.annotation.Order;

/** 限流切面先于缓存和锁切面执行，因此缓存命中请求仍会消耗配额。 */
@Aspect
@Order(-300)
public final class RdtRateLimitAspect {
    private final RateLimiterTemplate limiter;
    private final RdtProperties properties;
    private final RdtMethodSupport methods;

    /**
     * 创建限流切面。
     * @param limiter 功能模板
     * @param properties 默认配置
     * @param methods 方法解析支持组件
     */
    public RdtRateLimitAspect(RateLimiterTemplate limiter, RdtProperties properties, RdtMethodSupport methods) {
        this.limiter = limiter;
        this.properties = properties;
        this.methods = methods;
    }

    /**
     * 执行速率限制或带租约的并发限制。
     * @param point 方法调用
     * @param annotation 限流定义
     * @return 业务结果
     * @throws Throwable 原始业务异常
     */
    @Around("@annotation(annotation)")
    public Object around(ProceedingJoinPoint point, RdtRateLimit annotation) throws Throwable {
        Method method = methods.method(point);
        methods.validate(method);
        String key = methods.key(annotation.key(), method, point.getTarget(), point.getArgs(), null);
        Duration wait = methods.duration(annotation.waitTime(), properties.rateLimit().waitTime());
        return switch (annotation.mode()) {
            case QPS -> {
                if (!limiter.tryAcquire(key, annotation.permits(),
                        methods.duration(annotation.interval(), properties.rateLimit().interval()), wait)) {
                    throw rejected();
                }
                yield point.proceed();
            }
            case CONCURRENCY -> {
                try (LockHandle ignored = limiter.tryConcurrency(key, annotation.permits(), wait,
                        methods.duration(annotation.leaseTime(), properties.rateLimit().leaseTime())).orElseThrow(this::rejected)) {
                    yield point.proceed();
                }
            }
        };
    }

    private RdtException rejected() {
        return new RdtRateLimitException("Distributed admission capacity exhausted");
    }
}
