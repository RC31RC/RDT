package io.github.rdt.console.security;

import java.util.Set;

/** Backend permissions are authoritative; menus only reflect these capabilities. */
public enum ConsoleRole {
    ADMIN(Set.of("runtime:read", "runtime:operate", "runtime:delete", "connections:manage", "users:manage", "cache:clear")),
    OPERATOR(Set.of("runtime:read", "runtime:operate")),
    VIEWER(Set.of("runtime:read"));

    private final Set<String> permissions;
    ConsoleRole(Set<String> permissions) { this.permissions = permissions; }
    public Set<String> permissions() { return permissions; }
}
