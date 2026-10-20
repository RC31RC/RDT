package io.github.rdt.console.auth;

import io.github.rdt.console.security.ConsoleRole;
import java.util.Set;

public record ManagementPrincipal(String username, String displayName, ConsoleRole role, Set<String> permissions) {
    public ManagementPrincipal { permissions = Set.copyOf(permissions); }
    public boolean permits(String permission) { return permissions.contains(permission); }
}
