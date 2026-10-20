package io.github.rdt.spring.config;

import io.github.rdt.cache.CachePolicy;
import io.github.rdt.id.IdMode;
import io.github.rdt.id.IdPolicy;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.schedule.ScheduleDefinitionChangePolicy;
import io.github.rdt.schedule.ScheduleFailureMode;
import io.github.rdt.schedule.ScheduleMisfirePolicy;
import io.github.rdt.schedule.SchedulePolicy;
import io.github.rdt.schedule.SchedulerSettings;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryPolicy;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * 不可变应用配置；Redis 连接配置由官方 Starter 管理。
 * @param enabled RDT 总开关，不会关闭独立管理的 Redisson Starter
 * @param namespace 应用与环境隔离命名空间
 * @param instanceId 当前进程稳定身份；可靠广播重启恢复需要保持不变
 * @param cache 缓存配置
 * @param lock 锁默认配置
 * @param queue 消费者配置
 * @param schedule 调度器配置
 * @param rateLimit 限流默认配置
 * @param id 序列默认配置
 * @param idempotency 幂等执行配置
 */
@ConfigurationProperties("rdt")
public record RdtProperties(
        @DefaultValue("true")
        boolean enabled,

        @DefaultValue("application:dev")
        String namespace,

        String instanceId,

        @DefaultValue
        Cache cache,

        @DefaultValue
        Lock lock,

        @DefaultValue
        Queue queue,

        @DefaultValue
        Schedule schedule,

        @DefaultValue
        RateLimit rateLimit,
        @DefaultValue
        Id id,
        @DefaultValue
        Idempotency idempotency) {
    /** 绑定配置时校验命名空间。 */
    public RdtProperties {
        RdtSupport.component(namespace);
        instanceId = instanceId == null || instanceId.isBlank() ? null : instanceId.strip();
    }

    /** Obtain the same defaults when constructing infrastructure outside Spring Boot. */
    public static RdtProperties defaults() {
        return new Binder(List.of()).bindOrCreate("rdt", RdtProperties.class);
    }

    /**
     * 缓存配置。
     * @param ttl Redis 缓存存活时间
     * @param nullTtl 空值缓存存活时间
     * @param jitter 正向 TTL 抖动比例
     * @param localEnabled 是否启用允许有限陈旧时间的 Caffeine 缓存
     * @param localTtl 本地缓存最长存活时间
     * @param localMaximumSize 本地缓存条目上限
     */
    public record Cache(
            @DefaultValue("5m")
            Duration ttl,
            @DefaultValue("30s")
            Duration nullTtl,
            @DefaultValue("0.1")
            double jitter,
            @DefaultValue("false")
            boolean localEnabled,
            @DefaultValue("2s")
            Duration localTtl,
            @DefaultValue("10000")
            long localMaximumSize,
            Map<String, Region> regions) {
        public Cache {
            regions=regions==null ? Map.of() : Map.copyOf(regions);
        }
        public Map<String,CachePolicy> regionPolicies() {
            Map<String,CachePolicy> result=new java.util.LinkedHashMap<>();
            regions.forEach((name,value) -> result.put(name,value.resolve(policy())));
            return Map.copyOf(result);
        }

        /**
         * 返回经过校验的核心策略。
         * @return 经过校验的核心策略
         */
        public CachePolicy policy() {
            return new CachePolicy(ttl, nullTtl, jitter, localEnabled, localTtl, localMaximumSize);
        }
    }

    public record Region(Duration ttl,Duration nullTtl,Double jitter,Boolean localEnabled,Duration localTtl,Long localMaximumSize) {
        public CachePolicy resolve(CachePolicy fallback) {
            return new CachePolicy(ttl==null ? fallback.ttl():ttl,nullTtl==null ? fallback.nullTtl():nullTtl,
                    jitter==null ? fallback.jitter():jitter,localEnabled==null ? fallback.localEnabled():localEnabled,
                    localTtl==null ? fallback.localTtl():localTtl,localMaximumSize==null ? fallback.localMaximumSize():localMaximumSize);
        }
    }

    /**
     * 锁默认配置。
     * @param waitTime 获取锁的最长等待时间
     * @param leaseTime 为零时启用看门狗续期
     */
    public record Lock(
            @DefaultValue("3s")
            Duration waitTime,
            @DefaultValue("0s")
            Duration leaseTime) {
        /**
         * 返回经过校验的核心策略。
         * @return 经过校验的核心策略
         */
        public LockOptions policy() {
            return new LockOptions(LockType.REENTRANT, waitTime, leaseTime);
        }
    }

    /**
     * 消息配置。
     * @param enabled 是否启用消息传输和监听器注册
     * @param maxAttempts 处理器最大调用次数
     * @param retryDelay 重试退避时长
     * @param visibilityTimeout 待确认消息恢复所需的空闲时间阈值
     * @param pollTimeout 阻塞读取最长等待时间
     * @param shutdownTimeout 本地工作线程停止等待上限
     * @param autoDeleteAfterAck 所有消费组确认后是否自动删除 Stream 原文
     * @param deadLetterTtl 死信记录最长保留时间；零表示不自动过期
     * @param streamRetention 已完成 Stream 消息的时间裁剪目标；零表示不按时间裁剪，未确认消息不受此目标强制删除
     * @param streamMaxLength 已完成 Stream 消息的数量裁剪目标；零表示不按数量裁剪，未确认消息不受此目标强制删除
     * @param retryMultiplier 指数退避倍率；一表示固定退避
     * @param maxRetryDelay 单次重试退避上限
     * @param concurrency 未在监听器中覆盖时的本地并发默认值
     * @param maxConcurrentHandlers 当前 JVM 所有 RDT 消费订阅共享的处理/阻塞读取预算
     */
    public record Queue(
            @DefaultValue("true")
            boolean enabled,
            @DefaultValue("3")
            int maxAttempts,
            @DefaultValue("1s")
            Duration retryDelay,
            @DefaultValue("30s")
            Duration visibilityTimeout,
            @DefaultValue("2s")
            Duration pollTimeout,
            @DefaultValue("10s")
            Duration shutdownTimeout,
            @DefaultValue("true")
            boolean autoDeleteAfterAck,
            @DefaultValue("30d")
            Duration deadLetterTtl,
            @DefaultValue("7d")
            Duration streamRetention,
            @DefaultValue("10000")
            int streamMaxLength,
            @DefaultValue("1")
            double retryMultiplier,
            @DefaultValue("1h")
            Duration maxRetryDelay,
            @DefaultValue("1")
            int concurrency,
            @DefaultValue("16")
            int maxConcurrentHandlers,
            @DefaultValue("4")
            int maxBlockingReads,
            @DefaultValue("8")
            int readBatchSize,
            @DefaultValue("32")
            int localBufferCapacity,
            @DefaultValue("262144")
            int maxMessageBytes,
            @DefaultValue("10000")
            int maxBacklogMessages,
            @DefaultValue("67108864")
            long maxBacklogBytes,
            @DefaultValue("16777216")
            int localBufferBytes) {
        @ConstructorBinding
        public Queue {
            new QueuePolicy(maxAttempts, retryDelay, visibilityTimeout, pollTimeout, shutdownTimeout,
                    autoDeleteAfterAck, deadLetterTtl, streamRetention, streamMaxLength,
                    retryMultiplier, maxRetryDelay, maxConcurrentHandlers, maxBlockingReads, readBatchSize, localBufferCapacity, maxMessageBytes, maxBacklogMessages, maxBacklogBytes, localBufferBytes);
            RdtSupport.require(concurrency >= 1 && concurrency <= 256, "Invalid listener concurrency");
            RdtSupport.require(maxConcurrentHandlers >= 1 && maxConcurrentHandlers <= 256,
                    "Invalid global message handler capacity");
        }

        /**
         * 返回经过校验的核心策略。
         * @return 经过校验的核心策略
         */
        public QueuePolicy policy() {
            return new QueuePolicy(maxAttempts, retryDelay, visibilityTimeout, pollTimeout, shutdownTimeout,
                    autoDeleteAfterAck, deadLetterTtl, streamRetention, streamMaxLength,
                    retryMultiplier, maxRetryDelay, maxConcurrentHandlers, maxBlockingReads, readBatchSize, localBufferCapacity, maxMessageBytes, maxBacklogMessages, maxBacklogBytes, localBufferBytes);
        }

    }

    /**
     * 调度配置。
     * @param enabled 是否启用集群任务注册
     * @param pollInterval 本地到期检查间隔
     * @param resultTtl 执行记录保留时长
     * @param shutdownTimeout 本地工作线程停止等待上限
     * @param executorName 共享远程执行器名称
     * @param definitionChangePolicy 调度定义变化时的处理策略
     * @param maxConcurrentTasks 单个 JVM 中同时执行的本地任务上限；每次执行仍使用独立虚拟线程
     * @param remoteWorkers 远程执行器在当前 JVM 注册的工作容量
     * @param maxAttempts 单次任务触发包含首次调用在内的最大调用次数
     * @param retryDelay RETRY 模式下失败后的初始退避
     * @param retryMultiplier 指数退避倍率
     * @param maxRetryDelay 单次退避上限
     * @param misfirePolicy 正常触发错过阈值后的处理方式
     * @param misfireThreshold 判定正常触发过期的阈值
     * @param zone 未在任务注解中指定时使用的 cron 时区
     * @param failureMode 默认失败后继续正常周期；只有 RETRY 才使用退避参数
     * @param maxDuePerPoll 单轮最多从 Redis 到期索引读取的任务数
     */
    public record Schedule(
            @DefaultValue("true")
            boolean enabled,
            @DefaultValue("1s")
            Duration pollInterval,
            @DefaultValue("7d")
            Duration resultTtl,
            @DefaultValue("10s")
            Duration shutdownTimeout,
            @DefaultValue("FAIL")
            ScheduleDefinitionChangePolicy definitionChangePolicy,
            @DefaultValue("16")
            int maxConcurrentTasks,
            @DefaultValue("3")
            int maxAttempts,
            @DefaultValue("1s")
            Duration retryDelay,
            @DefaultValue("1")
            double retryMultiplier,
            @DefaultValue("1h")
            Duration maxRetryDelay,
            @DefaultValue("FIRE_ONCE")
            ScheduleMisfirePolicy misfirePolicy,
            @DefaultValue("1m")
            Duration misfireThreshold,
            @DefaultValue("UTC")
            ZoneId zone,
            @DefaultValue("CONTINUE")
            ScheduleFailureMode failureMode,
            @DefaultValue("128")
            int maxDuePerPoll,
            @DefaultValue("30s")
            Duration workerLease,
            @DefaultValue("1000")
            int maxHistoryEntries) {
        /** 校验时长和容量。 */
        @ConstructorBinding
        public Schedule {
            RdtSupport.require(maxHistoryEntries > 0 && maxHistoryEntries <= 1000000, "Invalid history entry limit");
            RdtSupport.positive(workerLease, "schedule worker lease");
            RdtSupport.require(workerLease.compareTo(pollInterval.multipliedBy(3)) >= 0,
                    "schedule worker-lease must cover at least three poll intervals");
            RdtSupport.positive(pollInterval, "schedule poll");
            RdtSupport.positive(resultTtl, "schedule result ttl");
            RdtSupport.positive(shutdownTimeout, "schedule shutdown");
            RdtSupport.require(maxConcurrentTasks > 0 && maxConcurrentTasks <= 256,
                    "Invalid scheduler capacity");
            RdtSupport.require(maxDuePerPoll >= 1 && maxDuePerPoll <= 4096,
                    "Invalid maximum due tasks per poll");
            RdtSupport.require(zone != null, "Schedule zone must not be null");
            new SchedulePolicy(new RetryPolicy(maxAttempts, retryDelay, retryMultiplier, maxRetryDelay),
                    resultTtl, definitionChangePolicy, misfirePolicy, misfireThreshold, failureMode);
        }

        public SchedulerSettings settings() {
            return new SchedulerSettings(pollInterval,resultTtl,shutdownTimeout,maxConcurrentTasks,maxDuePerPoll,workerLease,policy(),maxHistoryEntries);
        }

        public SchedulePolicy policy() {
            return new SchedulePolicy(new RetryPolicy(maxAttempts, retryDelay, retryMultiplier, maxRetryDelay),
                    resultTtl, definitionChangePolicy, misfirePolicy, misfireThreshold, failureMode);
        }
    }

    /**
     * 限流默认配置。
     * @param interval QPS 统计间隔
     * @param waitTime 获取许可的最长等待时间
     * @param leaseTime 并发任务的硬性租约上限
     * @param idleTtl QPS 键空闲保留时长
     */
    public record RateLimit(
            @DefaultValue("1s")
            Duration interval,
            @DefaultValue("0s")
            Duration waitTime,
            @DefaultValue("30s")
            Duration leaseTime,
            @DefaultValue("1h")
            Duration idleTtl) {
        /** 校验限流边界。 */
        public RateLimit {
            RdtSupport.positive(interval, "rate interval");
            RdtSupport.positive(leaseTime, "concurrency lease");
            RdtSupport.positive(idleTtl, "rate idle ttl");
            RdtSupport.require(waitTime != null && !waitTime.isNegative() && idleTtl.compareTo(interval) >= 0,
                    "Invalid rate defaults");
        }
    }

    /**
     * ID 配置。
     * @param mode 默认分配模式
     * @param start 初始序列值
     * @param allocationSize 号段大小
     * @param zone 格式化使用的时区
     */
    public record Id(
            @DefaultValue("SEGMENT")
            IdMode mode,
            @DefaultValue("1")
            long start,
            @DefaultValue("5000")
            long allocationSize,
            @DefaultValue("UTC")
            ZoneId zone) {
        /**
         * 返回经过校验的序列策略。
         * @return 经过校验的序列策略
         */
        public IdPolicy policy() {
            return new IdPolicy(mode, start, allocationSize, zone);
        }
    }

    /**
     * 幂等执行配置。
     *
     * @param enabled 是否创建幂等模板
     */
    public record Idempotency(
            @DefaultValue("true")
            boolean enabled) { }
}
