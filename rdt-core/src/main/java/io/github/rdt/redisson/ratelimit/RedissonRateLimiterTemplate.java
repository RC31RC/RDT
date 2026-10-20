package io.github.rdt.redisson.ratelimit;

import io.github.rdt.exception.RdtLockException;
import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.ratelimit.RateLimiterTemplate;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RPermitExpirableSemaphore;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateLimiterConfig;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;

/** 共享限流状态仅初始化一次，普通请求不会重置配额。 */
public final class RedissonRateLimiterTemplate implements RateLimiterTemplate {
    private final RedissonClient client;
    private final KeySpace keys;
    private final Duration idleTtl;

    /**
     * 创建限流器。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param idleTtl QPS 限流器空闲保留时长，必须不小于配置的统计间隔
     */
    public RedissonRateLimiterTemplate(RedissonClient client, KeySpace keys, Duration idleTtl) {
        this.client = client;
        this.keys = keys;
        this.idleTtl = RdtSupport.positive(idleTtl, "limiter idle ttl");
    }

    @Override
    public boolean tryAcquire(String key, long permits, Duration interval, Duration wait) {
        RdtSupport.positive(interval, "rate interval");
        RdtSupport.require(permits > 0 && wait != null && !wait.isNegative() && idleTtl.compareTo(interval) >= 0, "Invalid QPS policy");
        return RedisSupport.redis("acquire rate permit", () -> {
            RRateLimiter limiter = client.getRateLimiter(keys.key("qps", key));
            limiter.trySetRate(RateType.OVERALL, permits, interval, idleTtl);
            RateLimiterConfig config = limiter.getConfig();
            RdtSupport.require(config.getRate() == permits && config.getRateInterval() == interval.toMillis()
                    && config.getRateType() == RateType.OVERALL, "Conflicting QPS configuration for key");
            return limiter.tryAcquire(1, wait);
        });
    }

    @Override
    public Optional<LockHandle> tryConcurrency(String key, int capacity, Duration wait, Duration lease) {
        RdtSupport.positive(lease, "concurrency lease");
        RdtSupport.require(capacity > 0 && wait != null && !wait.isNegative(), "Invalid concurrency policy");
        RPermitExpirableSemaphore semaphore = client.getPermitExpirableSemaphore(keys.key("concurrency", key));
        RedisSupport.redisRun("initialize concurrency limiter", () -> {
            semaphore.trySetPermits(capacity);
            RdtSupport.require(semaphore.getPermits() == capacity, "Conflicting concurrency capacity");
        });
        try {
            String id = semaphore.tryAcquire(wait.toMillis(), lease.toMillis(), TimeUnit.MILLISECONDS);
            return id == null ? Optional.empty() : Optional.of(new Permit(semaphore, id));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RdtLockException(RdtErrorCode.INTERRUPTED, "Concurrency admission interrupted", e);
        } catch (RuntimeException e) {
            if (RedisSupport.isRedisTimeout(e)) {
                throw new RdtRedisException(RdtErrorCode.REDIS_TIMEOUT, "Concurrency admission timed out", e);
            }
            throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "Concurrency admission failed", e);
        }
    }

    private static final class Permit implements LockHandle {
        private final RPermitExpirableSemaphore semaphore;
        private final String id;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Permit(RPermitExpirableSemaphore semaphore, String id) {
            this.semaphore = semaphore;
            this.id = id;
        }

        @Override
        public boolean isHeld() {
            return !closed.get() && RedisSupport.redis("read concurrency lease", () -> semaphore.getLeaseTime(id)) > 0;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)
                    && !RedisSupport.redis("release concurrency lease", () -> semaphore.tryRelease(id))) {
                throw new RdtLockException(RdtErrorCode.LOCK_OWNERSHIP, "Concurrency lease expired; execution exceeded its bound");
            }
        }
    }
}
