package io.github.rdt.console.security;

import io.github.rdt.console.auth.ManagementPrincipal;
import io.github.rdt.console.storage.ConsoleLocalFiles;
import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.RdtSupport;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Small standalone Console identity store; it never uses the managed Redis or an enterprise database. */
public final class ConsoleSecurityService {
    private final ConsoleLocalFiles files;
    private final ConsolePasswords passwords = new ConsolePasswords();
    private final JsonCodec json = new JsonCodec();
    private final SecureRandom random = new SecureRandom();
    private final Duration tokenTtl;
    private final Semaphore loginSlots = new Semaphore(2);
    private final Map<String, Attempt> attempts = new LinkedHashMap<>();
    private final String dummyHash = passwords.hash("Unmatchable-" + token());
    private State state;

    public ConsoleSecurityService(ConsoleLocalFiles files, Duration tokenTtl) {
        this.files = files;
        this.tokenTtl = tokenTtl;
        byte[] saved = files.read("security.json");
        if (saved == null) {
            // Bootstrap once; subsequent starts must preserve changed credentials and roles.
            User administrator = new User("rdt", "RDT Administrator", ConsoleRole.ADMIN, true,
                    passwords.hash("rdt"), 1);
            persist(new State(List.of(administrator), List.of()));
        } else {
            state = json.read(new String(saved, StandardCharsets.UTF_8), State.class);
            validateState(state);
        }
    }

    public Login login(String username, String password, String source, long now) {
        RdtSupport.require(username != null && username.length() <= 40 && password != null && password.length() <= 128,
                "Invalid login credentials");
        String attemptKey = source + ":" + username;
        synchronized (this) {
            Attempt attempt = attempts.get(attemptKey);
            if (attempt != null && attempt.count() >= 5 && now - attempt.startedAt() < 60_000) {
                throw status(HttpStatus.TOO_MANY_REQUESTS, "Too many login attempts; retry in one minute");
            }
        }
        if (!loginSlots.tryAcquire()) throw status(HttpStatus.TOO_MANY_REQUESTS, "Login is busy; retry shortly");
        try {
            User candidate;
            synchronized (this) { candidate = find(username).orElse(null); }
            boolean matches = passwords.matches(password, candidate == null ? dummyHash : candidate.passwordHash());
            synchronized (this) {
                User current = find(username).orElse(null);
                if (!matches || current == null || !current.enabled() || candidate.version() != current.version()) {
                    recordFailure(attemptKey, now);
                    throw status(HttpStatus.UNAUTHORIZED, "Invalid account or password");
                }
                attempts.remove(attemptKey);
                String raw = token();
                long expiry = now + tokenTtl.toMillis();
                List<Session> sessions = new ArrayList<>(state.sessions().stream().filter(value -> value.expiresAt() > now).toList());
                if (sessions.size() >= 128) sessions.removeFirst();
                sessions.add(new Session(digest(raw), current.username(), current.version(), expiry));
                persist(new State(state.users(), sessions));
                return new Login(raw, expiry, principal(current));
            }
        } finally { loginSlots.release(); }
    }

    public synchronized Optional<ManagementPrincipal> authenticate(String token, long now) {
        if (token == null || token.length() != 43) return Optional.empty();
        String hashed = digest(token);
        return state.sessions().stream().filter(session -> session.expiresAt() > now && session.digest().equals(hashed))
                .findFirst().flatMap(session -> find(session.username())
                        .filter(user -> user.enabled() && user.version() == session.version()).map(this::principal));
    }

    public synchronized void logout(String token) {
        if (token == null) return;
        String hashed = digest(token);
        List<Session> kept = state.sessions().stream().filter(value -> !value.digest().equals(hashed)).toList();
        if (kept.size() != state.sessions().size()) persist(new State(state.users(), kept));
    }

    public synchronized List<UserView> users() { return state.users().stream().map(ConsoleSecurityService::view).toList(); }

    public synchronized UserView create(Create command) {
        validateUser(command.username(), command.displayName());
        validatePassword(command.password());
        RdtSupport.require(command.role() != null, "Choose a user role");
        if (find(command.username()).isPresent()) throw status(HttpStatus.CONFLICT, "Username already exists");
        RdtSupport.require(state.users().size() < 100, "Console supports at most 100 local accounts");
        User user = new User(command.username(), command.displayName(), command.role(), true, passwords.hash(command.password()), 1);
        List<User> next = new ArrayList<>(state.users());
        next.add(user);
        persist(new State(next, state.sessions()));
        return view(user);
    }

    public synchronized UserView update(String username, Update command) {
        User previous = requireUser(username);
        validateUser(username, command.displayName());
        RdtSupport.require(command.role() != null, "Choose a user role");
        String hash = previous.passwordHash();
        if (command.password() != null && !command.password().isEmpty()) {
            validatePassword(command.password());
            hash = passwords.hash(command.password());
        }
        User updated = new User(username, command.displayName(), command.role(), command.enabled(), hash, previous.version() + 1);
        List<User> users = state.users().stream().map(user -> user.username().equals(username) ? updated : user).toList();
        ensureAdministrator(users);
        persist(new State(users, sessionsWithout(username)));
        return view(updated);
    }

    public synchronized void delete(String username) {
        requireUser(username);
        List<User> users = state.users().stream().filter(user -> !user.username().equals(username)).toList();
        ensureAdministrator(users);
        persist(new State(users, sessionsWithout(username)));
    }

    public synchronized void changePassword(String username, PasswordChange command) {
        User user = requireUser(username);
        if (!passwords.matches(command.currentPassword(), user.passwordHash())) throw status(HttpStatus.FORBIDDEN, "Current password is incorrect");
        validatePassword(command.newPassword());
        update(username, new Update(user.displayName(), user.role(), user.enabled(), command.newPassword()));
    }

    private void persist(State next) {
        files.write("security.json", json.write(next).getBytes(StandardCharsets.UTF_8));
        state = next;
    }

    private void recordFailure(String key, long now) {
        Attempt previous = attempts.get(key);
        if (attempts.size() >= 256 && !attempts.containsKey(key)) attempts.remove(attempts.keySet().iterator().next());
        attempts.put(key, previous == null || now - previous.startedAt() >= 60_000
                ? new Attempt(now, 1) : new Attempt(previous.startedAt(), previous.count() + 1));
    }

    private Optional<User> find(String username) { return state.users().stream().filter(user -> user.username().equals(username)).findFirst(); }
    private User requireUser(String username) { return find(username).orElseThrow(() -> status(HttpStatus.NOT_FOUND, "User does not exist")); }
    private List<Session> sessionsWithout(String username) { return state.sessions().stream().filter(session -> !session.username().equals(username)).toList(); }
    private ManagementPrincipal principal(User user) { return new ManagementPrincipal(user.username(), user.displayName(), user.role(), user.role().permissions()); }
    private static UserView view(User user) { return new UserView(user.username(), user.displayName(), user.role(), user.enabled()); }
    private static void ensureAdministrator(List<User> users) {
        if (users.stream().noneMatch(user -> user.enabled() && user.role() == ConsoleRole.ADMIN)) {
            throw status(HttpStatus.CONFLICT, "At least one enabled administrator must remain");
        }
    }
    private static void validateUser(String username, String displayName) {
        RdtSupport.require(username != null && username.matches("[A-Za-z0-9_-]{3,40}"), "Username must be 3-40 letters, digits, '_' or '-'");
        RdtSupport.require(displayName != null && !displayName.isBlank() && displayName.length() <= 60, "Display name must contain 1-60 characters");
    }
    private static void validatePassword(String password) {
        RdtSupport.require(password != null && password.length() >= 12 && password.length() <= 128, "Password must contain 12-128 characters");
    }
    private static void validateState(State state) {
        if (state.users() == null || state.sessions() == null || state.users().size() > 100 || state.sessions().size() > 128) {
            throw new RdtConfigurationException("Invalid local Console identity store");
        }
        ensureAdministrator(state.users());
    }
    private String token() { byte[] bytes = new byte[32]; random.nextBytes(bytes); return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes); }
    private static String digest(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.US_ASCII))); }
        catch (NoSuchAlgorithmException failure) { throw new RdtConfigurationException("SHA-256 is unavailable", failure); }
    }
    private static ResponseStatusException status(HttpStatus status, String message) { return new ResponseStatusException(status, message); }

    public record State(List<User> users, List<Session> sessions) {
        public State { users = List.copyOf(users); sessions = List.copyOf(sessions); }
    }
    public record User(String username, String displayName, ConsoleRole role, boolean enabled, String passwordHash, long version) { }
    public record Session(String digest, String username, long version, long expiresAt) { }
    private record Attempt(long startedAt, int count) { }
    public record UserView(String username, String displayName, ConsoleRole role, boolean enabled) { }
    public record Create(String username, String displayName, ConsoleRole role, String password) { }
    public record Update(String displayName, ConsoleRole role, boolean enabled, String password) { }
    public record PasswordChange(String currentPassword, String newPassword) { }
    public record Login(String token, long expiresAtMillis, ManagementPrincipal principal) { }
}
