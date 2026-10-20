package io.github.rdt.console.connection;

import io.github.rdt.support.RdtSupport;
import java.net.URI;
import java.util.List;

/** Credentials are never returned by the settings API. Null update passwords mean keep the saved value. */
public record RedisConnectionProfile(Topology topology, List<String> addresses, int database,
                                     String masterName, String username, String password,
                                     String sentinelUsername, String sentinelPassword) {
    public enum Topology { SINGLE, CLUSTER, SENTINEL }

    public RedisConnectionProfile {
        RdtSupport.require(topology != null, "Redis topology is required");
        RdtSupport.require(addresses != null && !addresses.isEmpty() && addresses.size() <= 32,
                "Provide between 1 and 32 Redis node addresses");
        addresses = List.copyOf(addresses);
        for (String address : addresses) {
            URI uri = URI.create(address);
            RdtSupport.require(("redis".equals(uri.getScheme()) || "rediss".equals(uri.getScheme()))
                            && uri.getHost() != null && uri.getPort() > 0 && uri.getPort() <= 65535
                            && uri.getUserInfo() == null && uri.getQuery() == null && uri.getFragment() == null
                            && (uri.getPath() == null || uri.getPath().isEmpty()),
                    "Use redis://host:port or rediss://host:port without credentials or paths");
        }
        RdtSupport.require(addresses.stream().map(value -> URI.create(value).getScheme()).distinct().count() == 1,
                "Redis nodes must use the same TLS scheme");
        RdtSupport.require(database >= 0 && (topology != Topology.CLUSTER || database == 0),
                "Redis database must be non-negative; Cluster only supports database 0");
        RdtSupport.require(topology != Topology.SINGLE || addresses.size() == 1,
                "Single-server Redis requires exactly one address");
        RdtSupport.require(topology != Topology.SENTINEL || masterName != null && !masterName.isBlank(),
                "Sentinel requires a master name");
    }

    public RedisConnectionProfile withSavedPasswords(RedisConnectionProfile saved) {
        return new RedisConnectionProfile(topology, addresses, database, masterName, username,
                password != null ? password : saved == null ? null : saved.password,
                sentinelUsername, sentinelPassword != null ? sentinelPassword : saved == null ? null : saved.sentinelPassword);
    }
}
