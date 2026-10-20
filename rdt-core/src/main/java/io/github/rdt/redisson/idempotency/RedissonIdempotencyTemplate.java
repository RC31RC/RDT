package io.github.rdt.redisson.idempotency;

import io.github.rdt.idempotency.IdempotencyHandle;
import io.github.rdt.idempotency.IdempotencyTemplate;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.api.bucket.CompareAndDeleteArgs;
import org.redisson.api.bucket.CompareAndSetArgs;
import org.redisson.client.codec.StringCodec;

/** 使用 RBucket 的原子占位、租约过期和完成状态实现幂等执行。 */
public final class RedissonIdempotencyTemplate implements IdempotencyTemplate {
    private static final String COMPLETED = "completed";
    private final RedissonClient client;
    private final KeySpace keys;

    /**
     * 创建幂等模板。
     *
     * @param client 共享 Redisson 客户端
     * @param keys 命名空间
     */
    public RedissonIdempotencyTemplate(RedissonClient client, KeySpace keys) {
        this.client = client;
        this.keys = keys;
    }

    @Override
    public Optional<IdempotencyHandle> tryStart(String key, Duration leaseTime, Duration completedTtl) {
        RdtSupport.component(key);
        RdtSupport.positive(leaseTime, "idempotency lease");
        RdtSupport.positive(completedTtl, "idempotency completed ttl");
        String token = UUID.randomUUID().toString();
        RBucket<String> bucket = bucket(key);
        boolean acquired = RedisSupport.redis("start idempotent operation",
                () -> bucket.setIfAbsent(token, leaseTime));
        return acquired ? Optional.of(new Handle(bucket, token, completedTtl)) : Optional.empty();
    }

    @Override
    public boolean isCompleted(String key) {
        RdtSupport.component(key);
        return RedisSupport.redis("read idempotency state", () -> COMPLETED.equals(bucket(key).get()));
    }

    private RBucket<String> bucket(String key) {
        return client.getBucket(keys.key("idempotency", key), StringCodec.INSTANCE);
    }

    private static final class Handle implements IdempotencyHandle {
        private final RBucket<String> bucket;
        private final String token;
        private final Duration completedTtl;
        private boolean completed;
        private boolean closed;

        private Handle(RBucket<String> bucket, String token, Duration completedTtl) {
            this.bucket = bucket;
            this.token = token;
            this.completedTtl = completedTtl;
        }

        @Override
        public synchronized boolean complete() {
            if (closed) {
                return completed;
            }
            boolean changed = RedisSupport.redis("complete idempotent operation",
                    () -> bucket.compareAndSet(CompareAndSetArgs.<String>expected(token)
                            .set(COMPLETED).timeToLive(completedTtl)));
            if (changed) {
                completed = true;
                closed = true;
            }
            return changed;
        }

        @Override
        public synchronized boolean abort() {
            if (closed) {
                return false;
            }
            boolean removed = RedisSupport.redis("abort idempotent operation",
                    () -> bucket.compareAndDelete(CompareAndDeleteArgs.expected(token)));
            closed = true;
            return removed;
        }

        @Override
        public synchronized boolean isCompleted() {
            return completed;
        }
    }
}
