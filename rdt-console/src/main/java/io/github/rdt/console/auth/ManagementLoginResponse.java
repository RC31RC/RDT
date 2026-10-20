package io.github.rdt.console.auth;

public record ManagementLoginResponse(String token, long expiresAtMillis, ManagementPrincipal principal) { }
