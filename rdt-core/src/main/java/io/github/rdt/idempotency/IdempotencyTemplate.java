package io.github.rdt.idempotency;

import java.time.Duration;
import java.util.Optional;

/** 基于 Redis 短租约和完成状态的幂等执行协调器。 */
public interface IdempotencyTemplate {
    /**
     * 尝试占用一个业务幂等键。
     *
     * <p>占用中的执行在租约到期前不会被重复启动；成功完成后在
     * {@code completedTtl} 内不会再次执行。进程崩溃时不需要人工解锁，
     * 租约到期后可由后续请求接管。</p>
     *
     * @param key 业务幂等键
     * @param leaseTime 执行租约，必须大于零
     * @param completedTtl 成功状态保留时间，必须大于零
     * @return 成功占用时返回句柄，否则返回空值
     */
    Optional<IdempotencyHandle> tryStart(String key, Duration leaseTime, Duration completedTtl);

    /**
     * 判断业务键是否已完成。
     *
     * @param key 业务幂等键
     * @return Redis 中当前状态是否为完成
     */
    boolean isCompleted(String key);
}
