package io.github.rdt.console.web;

import io.github.rdt.console.security.ConsoleSecurityService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("${rdt.console.api-path:/rdt/management}/users")
public final class RdtConsoleUsersController {
    private final ConsoleSecurityService security;
    public RdtConsoleUsersController(ConsoleSecurityService security) { this.security = security; }

    @GetMapping
    public List<ConsoleSecurityService.UserView> users() { return security.users(); }

    @PostMapping
    public ConsoleSecurityService.UserView create(
            @RequestBody
            ConsoleSecurityService.Create command) { return security.create(command); }

    @PutMapping("/{username}")
    public ConsoleSecurityService.UserView update(
            @PathVariable
            String username,
            @RequestBody
            ConsoleSecurityService.Update command) { return security.update(username, command); }

    @DeleteMapping("/{username}")
    public ResponseEntity<Void> delete(
            @PathVariable
            String username) { security.delete(username); return ResponseEntity.noContent().build(); }
}
