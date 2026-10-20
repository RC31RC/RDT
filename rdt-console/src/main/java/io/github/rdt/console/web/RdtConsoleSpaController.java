package io.github.rdt.console.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/** Vue history routes and management APIs are served by the same executable JAR. */
@Controller
public class RdtConsoleSpaController {
    @GetMapping({"/", "/login", "/overview", "/streams", "/tasks", "/dead-letters", "/instances", "/settings", "/users", "/profile"})
    public String index() {
        return "forward:/index.html";
    }
}
