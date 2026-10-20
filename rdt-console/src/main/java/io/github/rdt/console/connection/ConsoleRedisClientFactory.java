package io.github.rdt.console.connection;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtRedisException;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.BaseMasterSlaveServersConfig;
import org.redisson.config.Config;

/** Each configured role has its own small client; no Spring Data or business worker infrastructure. */
public class ConsoleRedisClientFactory {
    private final ConsoleConnectionProperties properties;

    public ConsoleRedisClientFactory(ConsoleConnectionProperties properties) { this.properties = properties; }

    Config configuration(RedisConnectionProfile profile) {
        Config config = new Config().setThreads(2).setNettyThreads(2);
        config.setUsername(emptyToNull(profile.username())).setPassword(emptyToNull(profile.password()));
        switch (profile.topology()) {
            case SINGLE -> config.useSingleServer().setAddress(profile.addresses().getFirst())
                    .setDatabase(profile.database()).setConnectionMinimumIdleSize(1).setConnectionPoolSize(4)
                    .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(1)
                    .setConnectTimeout((int) properties.connectTimeout().toMillis())
                    .setTimeout((int) properties.commandTimeout().toMillis()).setRetryAttempts(0);
            case CLUSTER -> pools(config.useClusterServers().addNodeAddress(profile.addresses().toArray(String[]::new)));
            case SENTINEL -> {
                var server = config.useSentinelServers().addSentinelAddress(profile.addresses().toArray(String[]::new))
                        .setMasterName(profile.masterName()).setDatabase(profile.database())
                        .setSentinelUsername(emptyToNull(profile.sentinelUsername()))
                        .setSentinelPassword(emptyToNull(profile.sentinelPassword()));
                pools(server);
            }
        }
        return config;
    }

    public RedissonClient create(RedisConnectionProfile profile) {
        try {
            return Redisson.create(configuration(profile));
        } catch (RuntimeException failure) {
            throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION,
                    "Redis connection failed; check node addresses, database, ACL credentials and TLS in connection settings", failure);
        }
    }

    private void pools(BaseMasterSlaveServersConfig<?> server) {
        server.setMasterConnectionMinimumIdleSize(1).setMasterConnectionPoolSize(4)
                .setSlaveConnectionMinimumIdleSize(1).setSlaveConnectionPoolSize(4)
                .setSubscriptionConnectionMinimumIdleSize(1).setSubscriptionConnectionPoolSize(1)
                .setConnectTimeout((int) properties.connectTimeout().toMillis())
                .setTimeout((int) properties.commandTimeout().toMillis()).setRetryAttempts(0);
    }

    private static String emptyToNull(String value) { return value == null || value.isEmpty() ? null : value; }
}
