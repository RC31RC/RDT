package io.github.rdt.lock;

/**
 * 管理一次同步业务调用中的多个分布式锁。
 *
 * <p>Manager 可作为共享单例；锁句柄只存在于当前线程的作用域内。
 * 该作用域不能跨线程传播，异步业务必须显式重新建立作用域。</p>
 */
public interface DistributedLockManager {

    /**
     * 开启一个锁作用域。通常由 {@code @RdtLock} 切面负责调用。
     *
     * @return 当前线程的新作用域
     */
    LockScope beginScope();

    /**
     * 在当前作用域追加锁。没有作用域时直接报错，避免锁无法确定释放边界。
     *
     * @param key 逻辑资源键
     * @param options 锁策略
     */
    void lock(String key, LockOptions options);

    /**
     * 使用默认锁策略追加一把资源锁。
     *
     * @param key 逻辑资源键
     */
    default LockOptions defaultOptions() { return LockOptions.defaults(); }

    default void lock(String key) {
        lock(key, defaultOptions());
    }

    /**
     * 返回当前线程的作用域；没有作用域时返回空值。
     *
     * @return 当前作用域
     */
    LockScope currentScope();
}
