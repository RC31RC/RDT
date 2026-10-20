package io.github.rdt.management;

/**
 * Stream 消费者只读视图。
 *
 * @param name 消费者名称
 * @param pending 待确认消息数
 * @param idleTimeMillis 空闲时间
 * @param inactiveMillis 最近交互空闲时间
 */
public record StreamConsumerView(String name, long pending, long idleTimeMillis, long inactiveMillis) { }
