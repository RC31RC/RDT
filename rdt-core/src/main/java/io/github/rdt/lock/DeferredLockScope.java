package io.github.rdt.lock;

/**
 * 支持延迟释放的锁作用域。
 *
 * <p>该接口供 Spring 事务协调器使用。业务代码仍然只需要调用
 * {@link DistributedLockManager#lock(String)}，不需要直接处理这个生命周期协议。</p>
 */
public interface DeferredLockScope extends LockScope {

    /**
     * 将作用域标记为在事务完成后释放。
     *
     * <p>调用 {@link #close()} 后，当前方法作用域会立即结束，但已获取的锁会继续保留，
     * 直到协调器调用 {@link #completeClose()}。</p>
     */
    void deferClose();

    /**
     * 在事务提交或回滚完成后真正释放锁。
     */
    void completeClose();
}
