package io.github.rdt.redisson.id;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.id.DistributedIdGenerator;
import io.github.rdt.id.IdMode;
import io.github.rdt.id.IdPolicy;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RIdGenerator;
import org.redisson.api.RedissonClient;

/** 复用 RIdGenerator 句柄，在多次调用间保留本地号段。 */
public final class RedissonIdGenerator implements DistributedIdGenerator {
    private final RedissonClient client;
    private final KeySpace keys;
    private final IdPolicy policy;
    private final ConcurrentHashMap<String, RIdGenerator> segments = new ConcurrentHashMap<>();

    /**
     * 创建 ID 分配器。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param policy 序列初始化策略
     */
    public RedissonIdGenerator(RedissonClient client, KeySpace keys, IdPolicy policy) {
        this.client = client;
        this.keys = keys;
        this.policy = policy;
    }

    @Override
    public long next(String sequence) {
        return next(sequence, policy.mode());
    }

    @Override
    public long next(String sequence, IdMode mode) {
        RdtSupport.require(mode != null, "ID mode is required");
        return RedisSupport.redis("allocate ID", () -> switch (mode) {
            case SEGMENT -> segments.computeIfAbsent(sequence, name -> {
                RIdGenerator generator = client.getIdGenerator(keys.key("id-segment", name));
                generator.tryInit(policy.start(), policy.allocationSize());
                return generator;
            }).nextId();
            case ATOMIC -> {
                yield nextAtLeast(sequence, policy.start());
            }
        });
    }

    @Override
    public long nextAtLeast(String sequence, long minimum) {
        RdtSupport.require(minimum > 0, "ID minimum must be positive");
        return RedisSupport.redis("allocate atomic ID", () -> {
            RAtomicLong counter = client.getAtomicLong(keys.key("id-atomic", sequence));
            while (true) {
                long current = counter.get();
                long next = Math.max(Math.addExact(current, 1), Math.max(minimum, policy.start()));
                // Use integer CAS, not Lua arithmetic which loses precision for large Snowflake IDs.
                if (counter.compareAndSet(current, next)) return next;
            }
        });
    }

    @Override
    public String nextFormatted(String sequence, String prefix, String datePattern, int width) {
        RdtSupport.require(prefix != null && datePattern != null && width >= 1 && width <= 64, "Invalid ID formatting");
        String date;
        try {
            date = datePattern.isBlank() ? "" : ZonedDateTime.now(policy.zone())
                    .format(DateTimeFormatter.ofPattern(datePattern, Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new RdtConfigurationException("Invalid date pattern", e);
        }
        return prefix + date + String.format(Locale.ROOT, "%0" + width + "d", next(sequence));
    }
}
