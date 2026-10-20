package io.github.rdt.spring.config;

import io.github.rdt.exception.RdtConfigurationException;
import java.util.Map;
import java.util.Objects;
import org.redisson.api.RedissonClient;

/** Selects application-owned Redis clients without creating or closing any client. */
public final class RdtClientRouter {
    private final RedissonClient runtime;
    private final RedissonClient cache;
    private final String runtimeBeanName;
    private final String cacheBeanName;

    public RdtClientRouter(Map<String, RedissonClient> clients) {
        Objects.requireNonNull(clients, "clients");
        runtimeBeanName = clients.containsKey("rdtRuntimeRedis") ? "rdtRuntimeRedis" : singleClient(clients);
        runtime = clients.get(runtimeBeanName);
        cacheBeanName = clients.containsKey("rdtCacheRedis") ? "rdtCacheRedis" : runtimeBeanName;
        cache = clients.get(cacheBeanName);
    }

    public RedissonClient runtime() {
        return runtime;
    }

    public RedissonClient cache() {
        return cache;
    }

    public String runtimeBeanName() {
        return runtimeBeanName;
    }

    public String cacheBeanName() {
        return cacheBeanName;
    }

    private String singleClient(Map<String, RedissonClient> clients) {
        if (clients.size() != 1) {
            throw new RdtConfigurationException("Provide the runtime Redis role as rdtRuntimeRedis; configure rdt.redis.runtime.config/file or declare that bean");
        }
        return clients.keySet().iterator().next();
    }
}
