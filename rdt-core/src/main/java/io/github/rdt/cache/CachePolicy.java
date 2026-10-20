package io.github.rdt.cache;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;

/**
 * 缓存保留及有限陈旧时间策略。
 * @param ttl 默认 Redis TTL
 * @param nullTtl 较短的空值缓存 TTL
 * @param jitter 正向 TTL 抖动比例上限
 * @param localEnabled 是否启用 Caffeine
 * @param localTtl 本地缓存保留时长上限，同时受 Redis TTL 约束
 * @param localMaximumSize 本地缓存条目上限
 */
public record CachePolicy(Duration ttl, Duration nullTtl, double jitter,
                          boolean localEnabled, Duration localTtl, long localMaximumSize) {
    /** 校验保留时长和容量限制。 */
    public CachePolicy {
        RdtSupport.positive(ttl, "cache ttl");
        RdtSupport.positive(nullTtl, "null ttl");
        RdtSupport.positive(localTtl, "local ttl");
        RdtSupport.require(jitter >= 0 && jitter <= 1 && localMaximumSize > 0, "Invalid cache policy");
    }
}
