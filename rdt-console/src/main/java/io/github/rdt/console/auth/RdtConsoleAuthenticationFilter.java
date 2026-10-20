package io.github.rdt.console.auth;

import io.github.rdt.console.config.RdtConsoleProperties;
import io.github.rdt.console.security.ConsoleSecurityService;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/** Deny-by-default HTTP boundary. A hidden menu never replaces these backend checks. */
public final class RdtConsoleAuthenticationFilter extends OncePerRequestFilter {
    public static final String PRINCIPAL_ATTRIBUTE = "io.github.rdt.console.auth.principal";
    private static final Logger AUDIT = LoggerFactory.getLogger("rdt.console.audit");
    private static final Set<String> READ_ROOTS = Set.of("status", "namespaces", "overview", "streams", "tasks", "instances", "dead-letters", "alerts");
    private final RdtConsoleProperties properties;
    private final ConsoleSecurityService security;

    public RdtConsoleAuthenticationFilter(RdtConsoleProperties properties, ConsoleSecurityService security) {
        this.properties = properties;
        this.security = security;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = path(request);
        if (!path.startsWith(properties.apiPath() + "/")) return true;
        String relative = path.substring(properties.apiPath().length());
        return request.getMethod().equals("POST") && relative.equals("/auth/login");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var authenticated = security.authenticate(token(request), System.currentTimeMillis());
        if (authenticated.isEmpty()) { reject(response, 401, "Authentication required"); return; }
        ManagementPrincipal principal = authenticated.get();
        String permission = permission(path(request).substring(properties.apiPath().length()), request.getMethod());
        if (!permission.isEmpty() && !principal.permits(permission)) {
            reject(response, 403, "Permission denied: " + permission);
            return;
        }
        request.setAttribute(PRINCIPAL_ATTRIBUTE, principal);
        try { chain.doFilter(request, response); }
        finally {
            if (!Set.of("GET", "HEAD").contains(request.getMethod())) {
                AUDIT.info("actor={} method={} path={} namespace={} status={}", principal.username(), request.getMethod(),
                        path(request), request.getParameter("namespace"), response.getStatus());
            }
        }
    }

    static String permission(String path, String method) {
        if (path.equals("/auth/me") && method.equals("GET")
                || Set.of("/auth/logout", "/auth/password").contains(path) && method.equals("POST")) return "";
        String root = path.split("/", 3).length > 1 ? path.split("/", 3)[1] : "";
        if (path.equals("/cache/clear") && method.equals("POST")) return "cache:clear";
        if (root.equals("connections")) return "connections:manage";
        if (root.equals("users")) return "users:manage";
        if (method.equals("GET") && READ_ROOTS.contains(root)) return "runtime:read";
        if (method.equals("DELETE") || root.equals("dead-letters") && method.equals("POST")) return "runtime:delete";
        if (method.equals("POST") && Set.of("tasks", "instances").contains(root)) return "runtime:operate";
        return "unsupported:deny";
    }

    public static String token(HttpServletRequest request) {
        String header = request.getHeader("Authorization");
        return header != null && header.startsWith("Bearer ") ? header.substring(7).trim() : null;
    }

    private static String path(HttpServletRequest request) { return request.getRequestURI().substring(request.getContextPath().length()); }
    private static void reject(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write("{\"code\":\"AUTHORIZATION\",\"message\":\"" + message + "\"}");
    }
}
