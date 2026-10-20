package io.github.rdt.console.web;

import io.github.rdt.console.auth.ManagementLoginRequest;
import io.github.rdt.console.auth.ManagementLoginResponse;
import io.github.rdt.console.auth.ManagementPrincipal;
import io.github.rdt.console.auth.RdtConsoleAuthenticationFilter;
import io.github.rdt.console.security.ConsoleSecurityService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Protocol conversion only. Local accounts and sessions are independent of managed Redis. */
@RestController
@RequestMapping("${rdt.console.api-path:/rdt/management}/auth")
public final class RdtConsoleAuthController {
    private final ConsoleSecurityService security;
    public RdtConsoleAuthController(ConsoleSecurityService security) { this.security = security; }

    @PostMapping("/login")
    public ManagementLoginResponse login(
            @RequestBody
            ManagementLoginRequest request, HttpServletRequest servlet) {
        var login = security.login(request.username(), request.password(), servlet.getRemoteAddr(), System.currentTimeMillis());
        return new ManagementLoginResponse(login.token(), login.expiresAtMillis(), login.principal());
    }

    @GetMapping("/me")
    public ManagementPrincipal me(
            @RequestAttribute(RdtConsoleAuthenticationFilter.PRINCIPAL_ATTRIBUTE)
            ManagementPrincipal principal) { return principal; }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletRequest request) {
        security.logout(RdtConsoleAuthenticationFilter.token(request));
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/password")
    public ResponseEntity<Void> password(
            @RequestAttribute(RdtConsoleAuthenticationFilter.PRINCIPAL_ATTRIBUTE)
            ManagementPrincipal principal,
            @RequestBody
            ConsoleSecurityService.PasswordChange command) {
        security.changePassword(principal.username(), command);
        return ResponseEntity.noContent().build();
    }
}
