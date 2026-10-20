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
 * @param autoDeleteAfterAck 是否由周期维护清理所有消费组已确认的消息；ACK 路径仅标记本地状态
 * @param streamRetention Stream 消息最长保留时间；零表示不按时间裁剪
 * @param streamMaxLength Stream 消息数量上限；零表示不按数量裁剪
 * @param maxConcurrentHandlers 当前模板共享的业务处理预算
 */
public record QueuePolicy(int maxAttempts, Duration retryDelay, Duration visibilityTimeout,
                          Duration pollTimeout, Duration shutdownTimeout,
                          boolean autoDeleteAfterAck, Duration deadLetterTtl,
                          Duration streamRetention, int streamMaxLength,
                          double retryMultiplier, Duration maxRetryDelay,
                          int maxConcurrentHandlers, int maxBlockingReads, int readBatchSize, int localBufferCapacity) {
    /** 校验消息投递配置。 */
    public QueuePolicy {
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
                maxConcurrentHandlers, maxBlockingReads, readBatchSize, localBufferCapacity);
    }
}
