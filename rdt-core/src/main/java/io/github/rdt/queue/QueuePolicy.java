package io.github.rdt.queue;

import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryPolicy;
import java.time.Duration;

/**
 * 消费者资源及故障恢复边界。
 * @param maxAttempts 已确认失败预算，不计进程崩溃或未开始的派发
 * @param retryDelay 最小重试退避时长
 * @param visibilityTimeout Stream 待确认消息恢复前所需的空闲时间
 * @param pollTimeout 阻塞读取等待上限
 * @param shutdownTimeout 中断消费者后的停止等待上限
 * @param autoDeleteAfterAck 是否在原子 ACK 路径删除所有消费组已确认的原文
 * @param streamRetention 已完成 Stream 消息的时间裁剪目标；零表示不按时间裁剪，未确认消息不受此目标强制删除
 * @param streamMaxLength 已完成 Stream 消息的数量裁剪目标；零表示不按数量裁剪，未确认消息不受此目标强制删除
 * @param maxMessageBytes maximum UTF-8 encoded envelope size
 * @param maxBacklogMessages hard retained Stream plus delayed count per destination/partition
 * @param maxBacklogBytes hard retained encoded-envelope bytes per destination/partition, excluding Redis overhead
 * @param localBufferBytes conservative encoded-payload budget reserved before reads, not total JVM memory
 * @param maxConcurrentHandlers 当前模板共享的业务处理预算
 */
public record QueuePolicy(int maxAttempts, Duration retryDelay, Duration visibilityTimeout,
                          Duration pollTimeout, Duration shutdownTimeout,
                          boolean autoDeleteAfterAck, Duration deadLetterTtl,
                          Duration streamRetention, int streamMaxLength,
                          double retryMultiplier, Duration maxRetryDelay,
                          int maxConcurrentHandlers, int maxBlockingReads, int readBatchSize, int localBufferCapacity,
                          int maxMessageBytes, int maxBacklogMessages,
                          long maxBacklogBytes, int localBufferBytes) {
    /** 校验消息投递配置。 */
    public QueuePolicy {
        RdtSupport.require(maxMessageBytes > 0 && maxBacklogMessages > 0
                        && maxBacklogBytes >= maxMessageBytes && localBufferBytes >= maxMessageBytes,
                "Queue byte and backlog limits must be positive and fit one message");
        RdtSupport.require(localBufferCapacity >= 1 && localBufferCapacity <= 4096,
                "local buffer capacity must be between 1 and 4096");
        RdtSupport.require(readBatchSize >= 1 && readBatchSize <= 64, "read batch size must be between 1 and 64");
        RdtSupport.require(maxBlockingReads >= 1 && maxBlockingReads <= 64,
                "max blocking reads must be between 1 and 64");
        new RetryPolicy(maxAttempts, retryDelay, retryMultiplier, maxRetryDelay);
        RdtSupport.positive(visibilityTimeout, "visibility timeout");
        RdtSupport.positive(pollTimeout, "poll timeout");
        RdtSupport.positive(shutdownTimeout, "shutdown timeout");
        RdtSupport.require(deadLetterTtl != null && !deadLetterTtl.isNegative(),
                "dead letter ttl must not be negative");
        RdtSupport.require(streamRetention != null && !streamRetention.isNegative(),
                "stream retention must not be negative");
        RdtSupport.require(streamMaxLength >= 0, "stream max length must not be negative");
        RdtSupport.require(maxConcurrentHandlers >= 1 && maxConcurrentHandlers <= 256,
                "max concurrent handlers must be between 1 and 256");
    }

    public RetryPolicy retry() {
        return new RetryPolicy(maxAttempts, retryDelay, retryMultiplier, maxRetryDelay);
    }

    /** Infrastructure lifecycle and producer retention are not consumer-specific. */
    public QueuePolicy resolve(QueueOptions overrides) {
        RdtSupport.require(overrides != null, "Queue options must not be null");
        return new QueuePolicy(
                overrides.maxAttempts() == null ? maxAttempts : overrides.maxAttempts(),
                overrides.retryDelay() == null ? retryDelay : overrides.retryDelay(),
                overrides.visibilityTimeout() == null ? visibilityTimeout : overrides.visibilityTimeout(),
                overrides.pollTimeout() == null ? pollTimeout : overrides.pollTimeout(),
                shutdownTimeout,
                overrides.autoDeleteAfterAck() == null ? autoDeleteAfterAck : overrides.autoDeleteAfterAck(),
                overrides.deadLetterTtl() == null ? deadLetterTtl : overrides.deadLetterTtl(),
                streamRetention, streamMaxLength,
                overrides.retryMultiplier() == null ? retryMultiplier : overrides.retryMultiplier(),
                overrides.maxRetryDelay() == null ? maxRetryDelay : overrides.maxRetryDelay(),
                maxConcurrentHandlers, maxBlockingReads, readBatchSize, localBufferCapacity, maxMessageBytes, maxBacklogMessages, maxBacklogBytes, localBufferBytes);
    }
}
