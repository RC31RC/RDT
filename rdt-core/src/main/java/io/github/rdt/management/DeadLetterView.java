package io.github.rdt.management;

import io.github.rdt.queue.QueueMode;

/**
 * Redis 死信只读视图。
 *
 * @param destination 原始目的地
 * @param group 消费组
 * @param messageId 消息 ID
 * @param payload JSON 业务载荷
 * @param createdAt 消息创建时间
 * @param attempts 处理尝试次数
 * @param error 异常摘要
 * @param failedAt 失败时间
 * @param displayName 原始目的地的展示名
 */
public record DeadLetterView(String destination, String group, String messageId,
                             String payload, String createdAt, long attempts,
                             String error, String failedAt, String displayName, QueueMode mode,
                             String instanceId, String originalMessageId, String firstFailedAt) {
    /** 未指定展示名时显示目的地。 */
    public DeadLetterView {
        displayName = displayName == null || displayName.isBlank() ? destination : displayName;
    }
}
