package io.github.rdt.queue;

import java.time.Duration;

/** 可扩展的持久化死信存储；save 必须在持久化成功后才能返回。 */
@FunctionalInterface
public interface FailedMessageStore {
    /**
     * 按目的地、消费组和消息 ID 幂等保存。
     * @param failure 已耗尽重试次数的消息投递记录
     */
    void save(FailedMessage failure);

    /** Redis stores honor per-entry TTL; external stores own their retention lifecycle. */
    default void save(FailedMessage failure, Duration ttl) {
        save(failure);
    }
}
