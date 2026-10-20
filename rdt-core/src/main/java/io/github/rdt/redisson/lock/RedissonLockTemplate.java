package io.github.rdt.redisson.lock;

import java.util.Objects;
import io.github.rdt.exception.RdtLockException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.api.RLock;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;

/** 基于 Redisson 的锁；固定租约不会被隐式续期。 */
public final class RedissonLockTemplate implements DistributedLockTemplate {
    private final RedissonClient client;
    private final KeySpace keys;
    private LockOptions defaults = LockOptions.defaults();
    public void defaultOptions(LockOptions options) { defaults = Objects.requireNonNull(options); }
    @Override
    public LockOptions defaultOptions() { return defaults; }

    /**
     * 创建锁模板。
     * @param client 共享 Redisson 客户端
     * @param keys 应用命名空间
     */
    public RedissonLockTemplate(RedissonClient client, KeySpace keys) {
        this.client = client;
        this.keys = keys;
    }

    @Override
    public Optional<LockHandle> tryLock(String key, LockOptions options) {
        String name = keys.key(options.type() == LockType.READ || options.type() == LockType.WRITE ? "rwlock" :
                options.type() == LockType.FAIR ? "fairlock" : "lock", key);
        RLock lock = switch (options.type()) {
            case REENTRANT -> client.getLock(name);
            case FAIR -> client.getFairLock(name);
            case READ -> client.getReadWriteLock(name).readLock();
            case WRITE -> client.getReadWriteLock(name).writeLock();
        };
        try {
            boolean acquired = options.leaseTime().isZero()
                    ? lock.tryLock(options.waitTime().toMillis(), TimeUnit.MILLISECONDS)
                    : lock.tryLock(options.waitTime().toMillis(), options.leaseTime().toMillis(), TimeUnit.MILLISECONDS);
            return acquired ? Optional.of(new OwnedLock(lock, Thread.currentThread().threadId())) : Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RdtLockException(RdtErrorCode.INTERRUPTED, "Lock acquisition interrupted", e);
        } catch (RuntimeException e) {
            throw RedisSupport.failure("Lock acquisition", e);
        }
    }

    @Override
    public <T> T execute(String key, LockOptions options, Callable<T> action) throws Exception {
        try (LockHandle ignored = tryLock(key, options).orElseThrow(
                () -> new RdtLockException(RdtErrorCode.LOCK_TIMEOUT, "Lock deadline exceeded"))) {
            return action.call();
        }
    }

    @Override
    public Optional<LockHandle> trySemaphore(String key, int permits, Duration wait) {
        RdtSupport.require(permits > 0 && wait != null && !wait.isNegative(), "Invalid semaphore policy");
        RSemaphore semaphore = client.getSemaphore(keys.key("semaphore", key));
        RedisSupport.redisRun("initialize semaphore", () -> semaphore.trySetPermits(permits));
        try {
            return semaphore.tryAcquire(wait)
                    ? Optional.of(new SemaphoreHandle(semaphore)) : Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RdtLockException(RdtErrorCode.INTERRUPTED, "Semaphore acquisition interrupted", e);
        } catch (RuntimeException e) {
            throw RedisSupport.failure("Semaphore acquisition", e);
        }
    }

    private static final class OwnedLock implements LockHandle {
        private final RLock lock;
        private final long owner;
        private boolean closed;

        private OwnedLock(RLock lock, long owner) {
            this.lock = lock;
            this.owner = owner;
        }

        @Override
        public boolean isHeld() {
            return !closed && Thread.currentThread().threadId() == owner
                    && RedisSupport.redis("check lock ownership", lock::isHeldByCurrentThread);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            if (Thread.currentThread().threadId() != owner) {
                throw new RdtLockException(RdtErrorCode.LOCK_OWNERSHIP, "Lock must be released by its acquiring thread");
            }
            try {
                lock.unlock();
                closed = true;
            } catch (IllegalMonitorStateException e) {
                closed = true;
                throw new RdtLockException(RdtErrorCode.LOCK_OWNERSHIP, "Lock lease expired before release", e);
            } catch (RuntimeException e) {
                throw RedisSupport.failure("Lock release; outcome unknown", e);
            }
        }
    }

    private static final class SemaphoreHandle implements LockHandle {
        private final RSemaphore semaphore;
        private final AtomicBoolean closed = new AtomicBoolean();

        private SemaphoreHandle(RSemaphore semaphore) {
            this.semaphore = semaphore;
        }

        @Override
        public boolean isHeld() {
            return !closed.get();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                RedisSupport.redisRun("release semaphore", semaphore::release);
            }
        }
    }
}
