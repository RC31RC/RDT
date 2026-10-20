package io.github.rdt.console.web;

import io.github.rdt.console.connection.ConsoleConnectionManager;
import io.github.rdt.console.connection.ConsoleConnectionManager.SettingsView;
import io.github.rdt.console.connection.ConsoleConnectionManager.Update;
import io.github.rdt.console.connection.RedisConnectionProfile;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Protected by the same local Console login even before Redis is configured. */
@RestController
@RequestMapping("${rdt.console.api-path:/rdt/management}/connections")
public class RdtConsoleConnectionsController {
    private final ConsoleConnectionManager connections;
    public RdtConsoleConnectionsController(ConsoleConnectionManager connections) { this.connections = connections; }

    @GetMapping
    public SettingsView settings() { return connections.view(); }

    @PutMapping
    public SettingsView save(
            @RequestBody
            Update update) { return connections.save(update); }

    @PostMapping("/test/{role}")
    public Map<String, Boolean> test(
            @PathVariable
            ConsoleConnectionManager.Role role,
            @RequestBody
            RedisConnectionProfile profile) {
        connections.test(role, profile);
        return Map.of("reachable", true);
    }
}
