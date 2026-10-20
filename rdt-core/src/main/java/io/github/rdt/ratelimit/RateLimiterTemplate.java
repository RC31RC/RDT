package io.github.rdt.ratelimit;

import io.github.rdt.lock.LockHandle;
import java.time.Duration;
import java.util.Optional;

/** 全局 QPS 限流及显式租约的并发限制。 */
public interface RateLimiterTemplate {
    /**
     * 尝试消耗一个速率许可，不重置已有状态。
     * @param key 逻辑限流键
     * @param permits 速率配额
     * @param interval 速率统计窗口
     * @param wait 最长等待时间
     * @return 获得许可时为 true
     */
    boolean tryAcquire(String key, long permits, Duration interval, Duration wait);

    /**
     * 通过 RPermitExpirableSemaphore 获取并发租约。
     * @param key 逻辑限流键
     * @param capacity 同时有效的租约数量上限
     * @param wait 最长等待时间
     * @param lease 业务执行的硬性时长上限，不会自动续期
     * @return 释放句柄，未获得许可时为空
     */
    Optional<LockHandle> tryConcurrency(String key, int capacity, Duration wait, Duration lease);
}
