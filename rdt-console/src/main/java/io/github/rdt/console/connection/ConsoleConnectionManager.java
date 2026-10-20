package io.github.rdt.console.connection;

import io.github.rdt.console.management.RdtNamespaceManagementService;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtRedisException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.redisson.api.RedissonClient;
import org.redisson.api.redisnode.RedisNodes;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** A read lease covers the entire query/command/archive, not just obtaining its client. */
public class ConsoleConnectionManager implements AutoCloseable {
    public enum Role { RUNTIME, CACHE }
    private final ConsoleConnectionStore store;
    private final ConsoleRedisClientFactory factory;
    private final Semaphore probes = new Semaphore(1);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private ConsoleConnections settings;
    private Session runtime;
    private Session cache;
    private volatile long revision = ThreadLocalRandom.current().nextLong(1, 1L << 52);

    public ConsoleConnectionManager(ConsoleConnectionStore store, ConsoleRedisClientFactory factory) {
        this.store = store;
        this.factory = factory;
        settings = store.load();
        runtime = session(settings.runtime());
        cache = session(settings.cache());
    }

    public long revision() { return revision; }

    public boolean runtimeConfigured() {
        lock.readLock().lock();
        try { return runtime != null; } finally { lock.readLock().unlock(); }
    }

    public <T> T withManagement(Function<RdtNamespaceManagementService, T> action) {
        return withRuntime((client, namespaces) -> action.apply(new RdtNamespaceManagementService(client, namespaces)));
    }

    public <T> T withCommandManagement(long expectedRevision, Function<RdtNamespaceManagementService, T> action) {
        return useRuntime(expectedRevision, (client, namespaces) ->
                action.apply(new RdtNamespaceManagementService(client, namespaces)));
    }

    public <T> T withCacheCommandManagement(long expectedRevision, Role role,
                                                  Function<RdtNamespaceManagementService, T> action) {
        Session selected;
        List<String> namespaces;
        lock.readLock().lock();
        try {
            if (expectedRevision != revision) throw new ResponseStatusException(HttpStatus.CONFLICT, "Redis target changed; reload settings");
            selected = role == Role.CACHE ? cache : runtime;
            if (selected == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Selected Redis role is not configured");
            selected.retain();
            namespaces = settings.managedNamespaces();
        } finally { lock.readLock().unlock(); }
        try { return action.apply(new RdtNamespaceManagementService(selected.client(), namespaces)); }
        finally { selected.release(); }
    }

    public <T> T withRuntime(BiFunction<RedissonClient, List<String>, T> action) {
        return useRuntime(null, action);
    }

    private <T> T useRuntime(Long expectedRevision, BiFunction<RedissonClient, List<String>, T> action) {
        Session selected;
        List<String> namespaces;
        lock.readLock().lock();
        try {
            if (expectedRevision != null && expectedRevision != revision) {
                throw new ResponseStatusException(HttpStatus.CONFLICT, "Redis target changed; reload connection settings before issuing commands");
            }
            if (runtime == null) throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION,
                    "Runtime Redis is not configured; open Console connection settings after login");
            selected = runtime;
            selected.retain();
            namespaces = settings.managedNamespaces();
        } finally { lock.readLock().unlock(); }
        try { return action.apply(selected.client(), namespaces); }
        finally { selected.release(); }
    }

    public SettingsView view() {
        lock.readLock().lock();
        try { return new SettingsView(revision, view(runtime), view(cache), settings.managedNamespaces()); }
        finally { lock.readLock().unlock(); }
    }

    public SettingsView save(Update update) {
        Session oldRuntime;
        Session oldCache;
        lock.writeLock().lock();
        try {
            if (update.revision() != revision) throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Connection settings changed; reload before saving");
            ConsoleConnections next = new ConsoleConnections(update.runtime(), update.cache(), update.managedNamespaces())
                    .withSavedPasswords(settings);
            // Persist first. A failed disk write must leave the active settings/clients untouched.
            store.save(next);
            oldRuntime = runtime;
            oldCache = cache;
            settings = next;
            runtime = session(next.runtime());
            cache = session(next.cache());
            revision++;
        } finally { lock.writeLock().unlock(); }
        retire(oldRuntime);
        retire(oldCache);
        return view();
    }

    public void test(Role role, RedisConnectionProfile submitted) {
        RedisConnectionProfile resolved;
        lock.readLock().lock();
        try {
            var saved = role == Role.RUNTIME ? settings.runtime() : settings.cache();
            if (submitted == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Provide a Redis connection");
            resolved = submitted.withSavedPasswords(saved);
        } finally { lock.readLock().unlock(); }
        if (!probes.tryAcquire()) throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS,
                "Another Redis connection test is already running");
        RedissonClient probe = null;
        try {
            probe = factory.create(resolved);
            boolean reachable = switch (resolved.topology()) {
                case SINGLE -> probe.getRedisNodes(RedisNodes.SINGLE).pingAll();
                case CLUSTER -> probe.getRedisNodes(RedisNodes.CLUSTER).pingAll();
                case SENTINEL -> probe.getRedisNodes(RedisNodes.SENTINEL_MASTER_SLAVE).pingAll();
            };
            if (!reachable) throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION,
                    "Not all configured Redis nodes responded to PING");
        } finally {
            try { if (probe != null) probe.shutdown(0, 2, TimeUnit.SECONDS); }
            finally { probes.release(); }
        }
    }

    @Override
    public void close() {
        Session oldRuntime;
        Session oldCache;
        lock.writeLock().lock();
        try { oldRuntime = runtime; oldCache = cache; runtime = null; cache = null; }
        finally { lock.writeLock().unlock(); }
        retire(oldRuntime);
        retire(oldCache);
    }

    private Session session(RedisConnectionProfile profile) { return profile == null ? null : new Session(profile); }
    private static void retire(Session session) { if (session != null) session.retire(); }

    private static ProfileView view(Session session) {
        if (session == null) return null;
        RedisConnectionProfile p = session.profile;
        return new ProfileView(p.topology(), p.addresses(), p.database(), p.masterName(), p.username(),
                p.password() != null && !p.password().isEmpty(), p.sentinelUsername(),
                p.sentinelPassword() != null && !p.sentinelPassword().isEmpty(), session.client != null);
    }

    private final class Session {
        private final RedisConnectionProfile profile;
        private volatile RedissonClient client;
        private int leases;
        private boolean retired;
        private Session(RedisConnectionProfile profile) { this.profile = profile; }
        private synchronized void retain() { leases++; }
        private synchronized RedissonClient client() {
            if (client == null) client = factory.create(profile);
            return client;
        }
        private void release() {
            synchronized (this) { leases--; }
            closeIfUnused();
        }
        private void retire() {
            synchronized (this) { retired = true; }
            closeIfUnused();
        }
        private void closeIfUnused() {
            RedissonClient unused;
            synchronized (this) {
                if (!retired || leases != 0 || client == null) return;
                unused = client;
                client = null;
            }
            unused.shutdown(0, 2, TimeUnit.SECONDS);
        }
    }

    public record Update(long revision, RedisConnectionProfile runtime, RedisConnectionProfile cache,
                         List<String> managedNamespaces) { }
    public record SettingsView(long revision, ProfileView runtime, ProfileView cache, List<String> managedNamespaces) { }
    public record ProfileView(RedisConnectionProfile.Topology topology, List<String> addresses, int database,
                              String masterName, String username, boolean passwordSet, String sentinelUsername,
                              boolean sentinelPasswordSet, boolean clientActive) { }
}
