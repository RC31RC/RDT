package io.github.rdt.console.auth;

/** @param username 管理账号 @param password 管理密码 */
public record ManagementLoginRequest(String username, String password) { }
