package io.github.rdt.console.connection;

import io.github.rdt.support.RdtSupport;
import java.util.List;

public record ConsoleConnections(RedisConnectionProfile runtime, RedisConnectionProfile cache,
                                 List<String> managedNamespaces) {
    public ConsoleConnections {
        managedNamespaces = managedNamespaces == null ? List.of() : List.copyOf(managedNamespaces);
        RdtSupport.require(managedNamespaces.size() <= 200, "At most 200 managed namespaces are allowed");
        managedNamespaces.forEach(RdtSupport::component);
    }

    public static ConsoleConnections empty() { return new ConsoleConnections(null, null, List.of()); }

    public ConsoleConnections withSavedPasswords(ConsoleConnections saved) {
        return new ConsoleConnections(runtime == null ? null : runtime.withSavedPasswords(saved.runtime),
                cache == null ? null : cache.withSavedPasswords(saved.cache), managedNamespaces);
    }
}
