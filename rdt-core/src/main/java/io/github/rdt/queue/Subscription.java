package io.github.rdt.queue;

/** 本地持有的监听器注册句柄；关闭不会删除共享 Redis 资源。 */
@FunctionalInterface
public interface Subscription extends AutoCloseable {
    /** 停止当前注册，不删除共享队列或消费组。 */
    @Override
    void close();
}
