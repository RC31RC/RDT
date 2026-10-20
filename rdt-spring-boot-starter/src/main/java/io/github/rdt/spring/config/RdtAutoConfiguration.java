package io.github.rdt.spring.config;

import io.github.rdt.support.RetryClassifier;
import io.github.rdt.cache.CacheService;
import io.github.rdt.state.CounterStore;
import io.github.rdt.state.ExpiringMapStore;
import io.github.rdt.state.PersistentMapStore;
import io.github.rdt.state.SharedStateStore;
import io.github.rdt.consistency.OutboxStore;
import io.github.rdt.id.DistributedIdGenerator;
import io.github.rdt.idempotency.EventCoalescer;
import io.github.rdt.idempotency.IdempotencyTemplate;
import io.github.rdt.lock.DefaultLockManager;
import io.github.rdt.lock.DistributedLockManager;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.queue.FailedMessageStore;
import io.github.rdt.queue.RdtMessageSendManager;
import io.github.rdt.queue.MessageQueueTemplate;
import io.github.rdt.ratelimit.RateLimiterTemplate;
import io.github.rdt.redisson.atomic.RedisUtil;
import io.github.rdt.redisson.cache.RedissonCacheService;
import io.github.rdt.redisson.state.RedissonCounterStore;
import io.github.rdt.redisson.state.RedissonExpiringMapStore;
import io.github.rdt.redisson.state.RedissonPersistentMapStore;
import io.github.rdt.redisson.state.RedissonSharedStateStore;
import io.github.rdt.redisson.id.RedissonIdGenerator;
import io.github.rdt.redisson.idempotency.RedisEventCoalescer;
import io.github.rdt.redisson.idempotency.RedissonIdempotencyTemplate;
import io.github.rdt.redisson.lease.RedisOwnerLeaseService;
import io.github.rdt.redisson.lock.RedissonLockTemplate;
import io.github.rdt.redisson.queue.RedisFailedMessageStore;
import io.github.rdt.redisson.queue.RedissonMessageQueueTemplate;
import io.github.rdt.redisson.ratelimit.RedissonRateLimiterTemplate;
import io.github.rdt.redisson.schedule.RedissonScheduledTaskTemplate;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.schedule.ScheduledTaskTemplate;
import io.github.rdt.spring.cache.RdtCacheAspect;
import io.github.rdt.spring.id.RdtIdAspect;
import io.github.rdt.spring.lease.NodeIdentityLease;
import io.github.rdt.spring.lease.NodeIdentityListener;
import io.github.rdt.spring.lease.RdtRuntimeRegistry;
import io.github.rdt.spring.lock.RdtLockAspect;
import io.github.rdt.spring.lock.TransactionalLockManager;
import io.github.rdt.spring.queue.DefaultMessageSendManager;
import io.github.rdt.spring.ratelimit.RdtRateLimitAspect;
import io.github.rdt.spring.runtime.DatabasePressureProbe;
import io.github.rdt.spring.runtime.MicrometerRdtMetrics;
import io.github.rdt.spring.runtime.RdtAdmissionProperties;
import io.github.rdt.spring.runtime.RdtPressureMonitor;
import io.github.rdt.spring.support.RdtAnnotationRegistrar;
import io.github.rdt.spring.support.RdtMethodSupport;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.env.Environment;

/** Uses purpose-specific Redis clients; each component backs off for application-owned beans. */
@AutoConfiguration(after = RdtClientAutoConfiguration.class, afterName = {"org.redisson.spring.starter.RedissonAutoConfigurationV4",
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"})
@ConditionalOnClass(RedissonClient.class)
@ConditionalOnBean(RedissonClient.class)
@ConditionalOnProperty(prefix = "rdt", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties({RdtProperties.class, RdtInstanceProperties.class, RdtComponentProperties.class, RdtAdmissionProperties.class})
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class RdtAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(io.github.rdt.schedule.CronParser.class)
    public io.github.rdt.schedule.CronParser rdtCronParser() { return new io.github.rdt.spring.schedule.SpringCronParser(); }

    @Bean
    public RdtStartupSummary rdtStartupSummary(Map<String, RedissonClient> clients,RdtClientRouter router,RdtProperties properties) {
        return new RdtStartupSummary(clients,router,properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public RdtMetrics rdtMetrics(ObjectProvider<MeterRegistry> registries) {
        MeterRegistry registry = registries.getIfAvailable();
        return registry == null ? RdtMetrics.NONE : new MicrometerRdtMetrics(registry);
    }
    @Bean(initMethod = "start", destroyMethod = "close")
    @ConditionalOnMissingBean
    public RdtPressureMonitor rdtPressureMonitor(RdtAdmissionProperties properties, RuntimeIdentity identity,
                                                 ObjectProvider<DatabasePressureProbe> databases) {
        return new RdtPressureMonitor(properties, identity, databases.orderedStream().toList());
    }

    @Bean
    @ConditionalOnMissingBean
    public RetryClassifier rdtRetryClassifier() {
        return RetryClassifier.infrastructureFailures();
    }

    @Bean
    @ConditionalOnMissingBean
    public RuntimeIdentity rdtRuntimeIdentity(RdtProperties properties) {
        return new RuntimeIdentity(properties.instanceId());
    }

    @Bean
    @ConditionalOnMissingBean
    public RdtClientRouter rdtClientRouter(Map<String, RedissonClient> clients, RdtProperties properties) {
        return new RdtClientRouter(clients);
    }

    @Bean
    @ConditionalOnMissingBean(EventCoalescer.class)
    public EventCoalescer rdtEventCoalescer(RdtClientRouter clients, KeySpace keys) {
        return new RedisEventCoalescer(clients.runtime(), keys);
    }

    @Bean
    @ConditionalOnMissingBean
    public RdtIdAspect rdtIdAspect(DistributedIdGenerator ids, RdtMethodSupport methods) {
        return new RdtIdAspect(ids, methods);
    }

    @Bean
    @ConditionalOnMissingBean(RdtMessageSendManager.class)
    public RdtMessageSendManager rdtMessageSendManager(ObjectProvider<MessageQueueTemplate> transports,
            ObjectProvider<OutboxStore> stores, JsonCodec json, KeySpace keys) {
        return new DefaultMessageSendManager(transports, stores, json, keys);
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisOwnerLeaseService rdtOwnerLeases(
            RdtClientRouter clients, KeySpace keys, DistributedLockTemplate locks) {
        return new RedisOwnerLeaseService(clients.runtime(), keys, locks);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public NodeIdentityLease rdtNodeIdentity(
            RedisOwnerLeaseService leases, RdtProperties properties,
            RdtInstanceProperties instance, ObjectProvider<RdtAnnotationRegistrar> registrar,
            ObjectProvider<NodeIdentityListener> listeners, RuntimeIdentity identity) {
        return new NodeIdentityLease(leases, properties, instance, registrar,
                () -> listeners.orderedStream().toList(), identity);
    }

    @Bean(destroyMethod = "stop")
    @ConditionalOnMissingBean
    public RdtRuntimeRegistry rdtRuntimeRegistry(RdtClientRouter clients, KeySpace keys, JsonCodec json,
                                                 RdtProperties properties, RdtInstanceProperties instance,
                                                 Environment environment, RuntimeIdentity identity, RdtMetrics metrics, RetryClassifier retryClassifier) {
        return new RdtRuntimeRegistry(clients.runtime(), keys, json, properties, instance, environment, identity, metrics);
    }
    /** 创建自动配置实例。 */
    public RdtAutoConfiguration() { }

    /**
     * 返回命名空间。
     * @param properties 应用配置
     * @return 命名空间
     */
    @Bean
    @ConditionalOnMissingBean
    public KeySpace rdtKeySpace(RdtProperties properties) {
        return new KeySpace(properties.namespace());
    }

    /**
     * 返回显式类型的 JSON 载荷编解码器。
     * @return 显式类型的 JSON 载荷编解码器
     */
    @Bean
    @ConditionalOnMissingBean
    public JsonCodec rdtJsonCodec() {
        return new JsonCodec();
    }

    /**
     * 返回共享代理及 SpEL 支持组件。
     * @return 共享代理及 SpEL 支持组件
     */
    @Bean
    @ConditionalOnMissingBean
    public RdtMethodSupport rdtMethodSupport() {
        return new RdtMethodSupport();
    }

    /**
     * 返回分布式锁模板。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param properties 默认配置
     * @return 分布式锁模板
     */
    @Bean
    @ConditionalOnMissingBean(DistributedLockTemplate.class)
    public DistributedLockTemplate rdtLocks(RdtClientRouter clients, KeySpace keys, RdtProperties properties) {
        RedissonLockTemplate locks = new RedissonLockTemplate(clients.runtime(), keys);
        locks.defaultOptions(properties.lock().policy());
        return locks;
    }

    /**
     * 返回同步调用作用域锁管理器。Manager 是无状态单例，锁状态只属于当前调用线程。
     *
     * @param locks 分布式锁模板
     * @return 作用域锁管理器
     */
    @Bean
    @ConditionalOnMissingBean(DistributedLockManager.class)
    public DistributedLockManager rdtLockManager(DistributedLockTemplate locks) {
        return new TransactionalLockManager(new DefaultLockManager(locks));
    }

    /**
     * 返回缓存服务。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param json 编解码器
     * @param properties 配置
     * @param locks 分布式锁模板
     * @return 缓存服务
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(CacheService.class)
    public CacheService rdtCache(RdtClientRouter clients, KeySpace keys, JsonCodec json,
                                         RdtProperties properties) {
        DistributedLockTemplate cacheLocks = new RedissonLockTemplate(clients.cache(), keys);
        var cache = new RedissonCacheService(clients.cache(), keys, json, properties.cache().policy(), cacheLocks);
        properties.cache().regionPolicies().forEach(cache::region);
        return cache;
    }

    @Bean
    @ConditionalOnMissingBean(SharedStateStore.class)
    public SharedStateStore rdtSharedStateStore(RdtClientRouter clients, KeySpace keys, JsonCodec json) {
        return new RedissonSharedStateStore(clients.runtime(), keys, json);
    }

    @Bean
    @ConditionalOnMissingBean(ExpiringMapStore.class)
    public ExpiringMapStore rdtExpiringMapStore(RdtClientRouter clients, KeySpace keys, JsonCodec json) {
        return new RedissonExpiringMapStore(clients.runtime(), keys, json);
    }

    @Bean
    @ConditionalOnMissingBean(PersistentMapStore.class)
    public PersistentMapStore rdtPersistentMapStore(RdtClientRouter clients, KeySpace keys, JsonCodec json) {
        return new RedissonPersistentMapStore(clients.runtime(), keys, json);
    }

    @Bean
    @ConditionalOnMissingBean(CounterStore.class)
    public CounterStore rdtCounterStore(RdtClientRouter clients, KeySpace keys) {
        return new RedissonCounterStore(clients.runtime(), keys);
    }

    /**
     * 返回ID 分配器。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param properties 配置
     * @return ID 分配器
     */
    @Bean
    @ConditionalOnMissingBean(DistributedIdGenerator.class)
    public DistributedIdGenerator rdtIds(RdtClientRouter clients, KeySpace keys, RdtProperties properties) {
        return new RedissonIdGenerator(clients.runtime(), keys, properties.id().policy());
    }

    /**
     * 返回幂等执行模板。
     *
     * @param client 共享客户端
     * @param keys 命名空间
     * @return 幂等执行模板
     */
    @Bean
    @ConditionalOnMissingBean(IdempotencyTemplate.class)
    @ConditionalOnProperty(prefix = "rdt.idempotency", name = "enabled", havingValue = "true", matchIfMissing = true)
    public IdempotencyTemplate rdtIdempotency(RdtClientRouter clients, KeySpace keys) {
        return new RedissonIdempotencyTemplate(clients.runtime(), keys);
    }

    /**
     * 返回限流模板。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param properties 配置
     * @return 限流模板
     */
    @Bean
    @ConditionalOnMissingBean(RateLimiterTemplate.class)
    public RateLimiterTemplate rdtRateLimiter(RdtClientRouter clients, KeySpace keys, RdtProperties properties) {
        return new RedissonRateLimiterTemplate(clients.runtime(), keys, properties.rateLimit().idleTtl());
    }

    /**
     * 返回原子操作工具。
     * @param client 共享客户端
     * @param keys 命名空间
     * @return 原子操作工具
     */
    @Bean
    @ConditionalOnMissingBean
    public RedisUtil rdtRedisUtil(RdtClientRouter clients, KeySpace keys) {
        return new RedisUtil(clients.runtime(), keys);
    }

    /**
     * 返回持久化死信存储。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param json 编解码器
     * @return 持久化死信存储
     */
    @Bean
    @ConditionalOnMissingBean(FailedMessageStore.class)
    public RedisFailedMessageStore rdtFailedMessageStore(
            RdtClientRouter clients,
            KeySpace keys,
            JsonCodec json,
            RdtProperties properties) {
        return new RedisFailedMessageStore(clients.runtime(), keys, json, properties.queue().deadLetterTtl());
    }

    /**
     * 返回消息传输模式。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param json 编解码器
     * @param properties 配置
     * @param locks 分布式锁模板
     * @param failures 持久化存储
     * @return 消息传输模式
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(MessageQueueTemplate.class)
    @ConditionalOnProperty(prefix = "rdt.queue", name = "enabled", havingValue = "true", matchIfMissing = true)
    public MessageQueueTemplate rdtMessages(RdtClientRouter clients, KeySpace keys, JsonCodec json,
                                                    RdtProperties properties, DistributedLockTemplate locks, FailedMessageStore failures, RuntimeIdentity identity, RdtMetrics metrics, RetryClassifier retryClassifier) {
        var queue = new RedissonMessageQueueTemplate(clients.runtime(), keys, json, properties.queue().policy(), locks, failures, identity, retryClassifier);
        queue.metrics(metrics);
        return queue;
    }

    /**
     * 返回调度模板。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param json 编解码器
     * @param locks 分布式锁模板
     * @param properties 配置
     * @return 调度模板
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(ScheduledTaskTemplate.class)
    @ConditionalOnProperty(prefix = "rdt.schedule", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ScheduledTaskTemplate rdtScheduler(RdtClientRouter clients, KeySpace keys, JsonCodec json,
                                                     DistributedLockTemplate locks, RdtProperties properties, RuntimeIdentity identity, RdtMetrics metrics, io.github.rdt.schedule.CronParser crons, RetryClassifier retryClassifier) {
        var scheduler = new RedissonScheduledTaskTemplate(clients.runtime(), keys, json, locks, properties.schedule().settings(), crons, identity, retryClassifier);
        scheduler.metrics(metrics);
        return scheduler;
    }

    /**
     * 返回锁切面。
     * @param properties 默认配置
     * @return 锁切面
     */
    @Bean
    @ConditionalOnMissingBean
    public RdtLockAspect rdtLockAspect(DistributedLockManager lockManager) {
        return new RdtLockAspect(lockManager);
    }

    /**
     * 返回限流切面。
     * @param limiter 功能模板
     * @param properties 默认配置
     * @param methods 公共支持组件
     * @return 限流切面
     */
    @Bean
    @ConditionalOnMissingBean
    public RdtRateLimitAspect rdtRateLimitAspect(RateLimiterTemplate limiter, RdtProperties properties, RdtMethodSupport methods) {
        return new RdtRateLimitAspect(limiter, properties, methods);
    }

    /**
     * 返回缓存切面。
     * @param cache 功能模板
     * @param locks 分布式锁模板
     * @param methods 公共支持组件
     * @return 缓存切面
     */
    @Bean
    @ConditionalOnMissingBean
    public RdtCacheAspect rdtCacheAspect(CacheService cache, DistributedLockTemplate locks, RdtMethodSupport methods) {
        return new RdtCacheAspect(cache, locks, methods);
    }

    /**
     * 返回生命周期注册器。
     * @param beans Bean 工厂
     * @param queues 可选消息传输模板
     * @param scheduler 可选调度模板
     * @param methods 公共支持组件
     * @param json 编解码器
     * @return 生命周期注册器
     */
    @Bean
    @ConditionalOnMissingBean
    public RdtAnnotationRegistrar rdtAnnotationRegistrar(ConfigurableListableBeanFactory beans,
            ObjectProvider<MessageQueueTemplate> queues, ObjectProvider<ScheduledTaskTemplate> scheduler,
            RdtMethodSupport methods, JsonCodec json, RdtProperties properties, RdtComponentProperties components) {
        return new RdtAnnotationRegistrar(beans, queues, scheduler, methods, json, properties, components);
    }

}
