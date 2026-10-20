package io.github.rdt.lock;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Callable;

/** 分布式互斥锁、读写锁及显式信号量操作。 */
public interface DistributedLockTemplate {
    /**
     * 尝试获取锁，保留线程中断状态。
     * @param key 逻辑资源键
     * @param options 锁获取策略
     * @return 已获取的锁句柄，超时返回空值
     */
    Optional<LockHandle> tryLock(String key, LockOptions options);

    default LockOptions defaultOptions() { return LockOptions.defaults(); }

    default Optional<LockHandle> tryLock(String key) { return tryLock(key, defaultOptions()); }

    /**
     * 释放已获取的句柄。
     * @param handle 由当前线程获取的锁句柄
     */
    default void unlock(LockHandle handle) {
        handle.close();
    }

    /**
     * 执行同步回调，并在异常情况下保证释放。
     * @param key 逻辑键
     * @param options 锁获取策略
     * @param action 业务回调，其受检异常保持原样传播
     * @param <T> 结果类型
     * @return 回调结果
     * @throws Exception 回调异常
     */
    <T> T execute(String key, LockOptions options, Callable<T> action) throws Exception;

    /**
     * 获取一个 RSemaphore 许可；该基础信号量不具备崩溃自动回收能力。
     * @param key 逻辑信号量名称
     * @param permits 初始容量，后续调用方必须使用相同容量
     * @param wait 最长等待时间
     * @return 已获取的许可句柄，失败时为空
     */
    Optional<LockHandle> trySemaphore(String key, int permits, Duration wait);
}
