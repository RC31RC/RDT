package io.github.rdt.spring.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Owns purpose-specific clients, without replacing or rewriting native Redisson configuration. */
@AutoConfiguration(beforeName = {"org.redisson.spring.starter.RedissonAutoConfigurationV4",
        "org.redisson.spring.starter.RedissonAutoConfigurationV2"})
@ConditionalOnProperty(prefix = "rdt", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RdtRedisProperties.class)
public class RdtClientAutoConfiguration {
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "rdtRuntimeRedis")
    public RedissonClient rdtRuntimeRedis(RdtRedisProperties properties) {
        return Redisson.create(properties.runtime().redisson());
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "rdtCacheRedis")
    @Conditional(CacheConfigured.class)
    public RedissonClient rdtCacheRedis(RdtRedisProperties properties) {
        return Redisson.create(properties.cache().redisson());
    }
    static final class CacheConfigured implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return context.getEnvironment().containsProperty("rdt.redis.cache.config")
                    || context.getEnvironment().containsProperty("rdt.redis.cache.file");
        }
    }
}
