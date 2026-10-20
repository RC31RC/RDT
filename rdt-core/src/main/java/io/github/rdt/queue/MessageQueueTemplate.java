package io.github.rdt.queue;

import java.time.Duration;

/** 显式区分消息传输模式；需要在消费者崩溃后恢复的任务应选择 Stream。 */
public interface MessageQueueTemplate {
    /**
     * 发送持久化 Stream 消息信封。
     * @param destination 逻辑 Stream 名称
     * @param payload 业务数据
     * @return 不可变消息 ID
     */
    default String send(String destination, Object payload) {
        return send(destination, payload, QueueMode.STREAM);
    }

    /**
     * 通过所选传输模式立即发送。
     * @param destination 逻辑资源名
     * @param payload 业务数据
     * @param mode 消息传输模式
     * @return 不可变消息 ID
     */
    String send(String destination, Object payload, QueueMode mode);

    /**
     * 通过持久化到期索引原子转移到 Stream。
     * @param destination 逻辑延迟队列名称
     * @param payload 业务数据
     * @param delay 大于零的延迟时长
     * @return 不可变消息 ID
     */
    String sendDelayed(String destination, Object payload, Duration delay);

    /** Send an explicit JSON envelope, preserving the caller's message ID, business key and headers. */
    default String sendMessage(String destination, Message message) {
        return sendMessage(destination, message, QueueMode.STREAM);
    }

    String sendMessage(String destination, Message message, QueueMode mode);

    /** Stable partition count cannot change while the topic exists. */
    String sendOrdered(String topic, String orderingKey, Object payload, int partitions);

    default String sendOrdered(String topic, String orderingKey, Object payload) {
        return sendOrdered(topic, orderingKey, payload, 16);
    }


    /**
     * 仅向当前在线订阅的实例发布消息。
     * @param destination 逻辑主题名称
     * @param payload 业务数据
     * @return 不可变消息 ID
     */
    default String broadcast(String destination, Object payload) {
        return send(destination, payload, QueueMode.BROADCAST);
    }

    /**
     * 向所有实例可靠广播；每个实例拥有独立 Stream 消费组，支持离线恢复。
     */
    default String reliableBroadcast(String destination, Object payload) {
        return send(destination, payload, QueueMode.RELIABLE_BROADCAST);
    }

    /**
     * 启动带本地并发上限的消费者；处理使用虚拟线程，不共享固定业务线程池。
     * @param subscription 消息传输配置
     * @param handler 同步且幂等的消息处理器
     * @return 本地管理的生命周期句柄
     */
    Subscription subscribe(QueueSubscription subscription, MessageHandler handler);

    /** Register a consumer without manually assembling the subscription record. */
    default Subscription subscribe(String destination, QueueMode mode, String group, int concurrency,
                                   QueueOptions options, MessageHandler handler) {
        return subscribe(new QueueSubscription(destination, mode, group, concurrency, "", options, 0, destination + ":" + group), handler);
    }
}
