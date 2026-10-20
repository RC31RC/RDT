package io.github.rdt.lock;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;

/**
 * 锁获取策略。
 * @param type 锁类型
 * @param waitTime 最长等待时间，零表示立即尝试
 * @param leaseTime 零表示启用看门狗；正值表示固定租约且禁用自动续期
 */
public record LockOptions(LockType type, Duration waitTime, Duration leaseTime) {
    /** 校验锁获取策略。 */
    public LockOptions {
        RdtSupport.require(type != null && waitTime != null && leaseTime != null, "Lock options are required");
        RdtSupport.require(!waitTime.isNegative() && !leaseTime.isNegative(), "Lock durations must not be negative");
        RdtSupport.require(leaseTime.isZero() || leaseTime.toMillis() > 0, "Lease must be zero or at least 1ms");
    }

    /**
     * 返回最多等待三秒、通过看门狗续期的非公平锁配置。
     * @return 最多等待三秒、通过看门狗续期的非公平锁配置
     */
    public static LockOptions defaults() {
        return new LockOptions(LockType.REENTRANT, Duration.ofSeconds(3), Duration.ZERO);
    }
}
