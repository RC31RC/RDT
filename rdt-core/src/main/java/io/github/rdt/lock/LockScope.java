package io.github.rdt.lock;

/** 一次同步业务调用的锁集合及其释放边界。 */
public interface LockScope extends AutoCloseable {

    /**
     * 在当前作用域中追加锁。
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
     * 返回当前作用域是否已经持有指定逻辑锁。
     *
     * @param key 逻辑资源键
     * @return 是否持有
     */
    boolean holds(String key);

    /**
     * 释放当前作用域内的所有锁。物理锁按获取顺序逆序释放。
     */
    @Override
    void close();
}
