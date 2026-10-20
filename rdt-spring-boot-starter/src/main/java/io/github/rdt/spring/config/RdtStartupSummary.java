package io.github.rdt.spring.config;

import io.github.rdt.exception.RdtConfigurationException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;

/** Deliberately never serializes Redisson Config: it contains credentials. */
public final class RdtStartupSummary implements SmartInitializingSingleton {
    private static final Logger log=LoggerFactory.getLogger(RdtStartupSummary.class);
    private final Map<String,RedissonClient> clients;
    private final RdtClientRouter router;
    private final RdtProperties properties;
    public RdtStartupSummary(Map<String,RedissonClient> clients,RdtClientRouter router,RdtProperties properties) {
        this.clients=clients;this.router=router;this.properties=properties;
    }
    @Override
    public void afterSingletonsInstantiated() {
        String settings="namespace="+properties.namespace()+",runtime="+router.runtimeBeanName()+",cache="+router.cacheBeanName()
                +",lock="+properties.lock().policy()+",queue="+properties.queue().policy()+",schedule="+properties.schedule();
        log.info("RDT effective configuration fingerprint={} namespace={} runtimeBean={} cacheBean={} lockWait={} lockLease={} queueConcurrency={} blockingReads={} scheduleConcurrency={} dueBatch={}",fingerprint(settings),properties.namespace(),router.runtimeBeanName(),router.cacheBeanName(),properties.lock().waitTime(),properties.lock().leaseTime(),properties.queue().maxConcurrentHandlers(),properties.queue().maxBlockingReads(),properties.schedule().maxConcurrentTasks(),properties.schedule().maxDuePerPoll());
        if (!properties.lock().leaseTime().isZero()) log.warn("RDT default lock lease={} is FIXED; watchdog renewal is DISABLED. Long callbacks may lose mutual exclusion",properties.lock().leaseTime());
        clients.forEach((name,client) -> {
            var config=client.getConfig();
            if (config==null) return;
            if (config.isSingleConfig()) {
                var endpoint=config.useSingleServer();
                log.info("RDT Redis bean={} endpoint={} db={} pool={} subscriptions={} nettyThreads={}",name,
                        endpoint(endpoint.getAddress()),endpoint.getDatabase(),endpoint.getConnectionPoolSize(),endpoint.getSubscriptionConnectionPoolSize(),config.getNettyThreads());
            } else if (config.isClusterConfig()) {
                log.info("RDT Redis bean={} cluster={} db=0",name,config.useClusterServers().getNodeAddresses().stream().map(this::endpoint).toList());
            } else if (config.isSentinelConfig()) {
                log.info("RDT Redis bean={} sentinels={} db={}",name,config.useSentinelServers().getSentinelAddresses().stream().map(this::endpoint).toList(),config.useSentinelServers().getDatabase());
            } else log.info("RDT Redis bean={} topology=externally-configured",name);
        });
        if (router.runtime()==router.cache()) log.warn("RDT runtime/cache share a client. Logical prefixes do NOT isolate eviction or memory budgets");
    }
    private String endpoint(String address) { URI uri=URI.create(address);return uri.getHost()+":"+uri.getPort(); }
    private String fingerprint(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))).substring(0,16); }
        catch (NoSuchAlgorithmException impossible) { throw new RdtConfigurationException("SHA-256 unavailable", impossible); }
    }
}
