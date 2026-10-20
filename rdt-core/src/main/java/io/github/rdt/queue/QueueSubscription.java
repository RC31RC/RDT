package io.github.rdt.queue;

import io.github.rdt.support.RdtSupport;

/**
 * 本地消费者定义；各实例共享消费组，但使用唯一消费者名称。
 * @param destination 逻辑队列、Stream 或主题名称
 * @param mode 消息传输模式
 * @param group Stream 消费组；可靠广播模式作为逻辑广播组名称
 * @param concurrency 本地处理器最大并发数
 * @param displayName 目的地展示名，不参与消息路由或消费组身份
 * @param options 当前订阅的重试、恢复和死信策略覆盖
 */
public record QueueSubscription(String destination, QueueMode mode, String group, int concurrency,
                                String displayName, QueueOptions options, int orderedPartitions, String listenerId) {
    public String effectiveGroup(String instanceId) {
        return mode == QueueMode.RELIABLE_BROADCAST ? group + ":" + instanceId : group;
    }

    public static QueueSubscription concurrent(String topic, QueueMode mode, String group, int concurrency) {
        return new QueueSubscription(topic, mode, group, concurrency, "", QueueOptions.defaults(), 0, topic + ":" + group);
    }

    public static QueueSubscription ordered(String topic, String group, int concurrency, int partitions) {
        return new QueueSubscription(topic, QueueMode.STREAM, group, concurrency, "", QueueOptions.defaults(), partitions, topic + ":" + group);
    }

    /** 校验消费者定义。 */
    public QueueSubscription {
        RdtSupport.require(orderedPartitions >= 0 && orderedPartitions <= 128,
                "Ordered partitions must be between 1 and 128, or 0 for concurrent consumption");
        RdtSupport.require(orderedPartitions == 0 || mode == QueueMode.STREAM,
                "Ordered subscriptions require reliable Stream delivery");
        listenerId = listenerId == null || listenerId.isBlank() ? destination + ":" + group : listenerId.strip();
        RdtSupport.component(listenerId);
        RdtSupport.component(destination);
        RdtSupport.component(group);
        RdtSupport.require(options != null, "Queue options must not be null");
        RdtSupport.require(mode != null && concurrency >= 1 && concurrency <= 256, "Invalid listener concurrency");
        displayName = displayName == null ? "" : displayName.strip();
        RdtSupport.require(displayName.length() <= 200, "Queue display name must not exceed 200 characters");
    }
}
