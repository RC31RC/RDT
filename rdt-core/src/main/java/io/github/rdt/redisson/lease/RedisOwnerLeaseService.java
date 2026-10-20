package io.github.rdt.redisson.lease;

import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Thread-independent leases for sockets. Every renewal/release compares the acquisition token. */
public class RedisOwnerLeaseService {
    private final RedissonClient redis;
    private final KeySpace keys;
    private final DistributedLockTemplate locks;

    public RedisOwnerLeaseService(RedissonClient redis, KeySpace keys, DistributedLockTemplate locks) {
        this.redis = Objects.requireNonNull(redis);
        this.keys = Objects.requireNonNull(keys);
        this.locks = Objects.requireNonNull(locks);
    }

    public Optional<Lease> acquire(String name, String owner, Duration ttl) {
        RdtSupport.require(name != null && !name.isBlank() && owner != null && !owner.isBlank(),
                "Lease name and owner are required");
        validateTtl(ttl);
        var lock = locks.tryLock("owner-acquire:" + name,
                new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO));
        if (lock.isEmpty()) return Optional.empty();
        try (var ignored = lock.get()) {
            String key = keys.key("owner", name);
            if (redis.getBucket(key, StringCodec.INSTANCE).isExists()) return Optional.empty();
            long fence = redis.getAtomicLong(keys.key("owner-fence", name)).incrementAndGet();
            String token = owner + ":" + fence + ":" + UUID.randomUUID();
            return redis.getBucket(key, StringCodec.INSTANCE).setIfAbsent(token, ttl)
                    ? Optional.of(new Lease(name, token, fence)) : Optional.empty();
        }
    }

    public Optional<Lease> find(String name) {
        String token = redis.<String>getBucket(keys.key("owner", name), StringCodec.INSTANCE).get();
        if (token == null) return Optional.empty();
        return Optional.of(decode(name, token));
    }

    /** Batch lookup is a read view, not permission to skip the final acquisition-token check. */
    public Map<String, Lease> findAll(Collection<String> names) {
        if (names.isEmpty()) return Map.of();
        Map<String, String> values = redis.getBuckets(StringCodec.INSTANCE)
                .get(names.stream().map(name -> keys.key("owner", name)).toArray(String[]::new));
        Map<String, Lease> result = new LinkedHashMap<>();
        names.forEach(name -> {
            String token = values.get(keys.key("owner", name));
            if (token != null) result.put(name, decode(name, token));
        });
        return Map.copyOf(result);
    }

    private Lease decode(String name, String token) {
        int last = token.lastIndexOf(':');
        int fence = token.lastIndexOf(':', last - 1);
        return new Lease(name, token, Long.parseLong(token.substring(fence + 1, last)));
    }

    public boolean renew(Lease lease, Duration ttl) {
        validateTtl(ttl);
        return redis.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,
                "if redis.call('get', KEYS[1]) == ARGV[1] then "
                        + "return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
                RScript.ReturnType.LONG, List.of(keys.key("owner", lease.name())),
                lease.token(), ttl.toMillis()).equals(1L);
    }

    public boolean isHeld(Lease lease) {
        return lease != null && lease.token().equals(
                redis.getBucket(keys.key("owner", lease.name()), StringCodec.INSTANCE).get());
    }

    public void release(Lease lease) {
        redis.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,
                "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
                RScript.ReturnType.LONG, List.of(keys.key("owner", lease.name())), lease.token());
    }

    public record Lease(String name, String token, long fencingVersion) {
        public String owner() {
            return token.substring(0, token.lastIndexOf(':', token.lastIndexOf(':') - 1));
        }
    }

    private static void validateTtl(Duration ttl) {
        RdtSupport.require(ttl != null && !ttl.isNegative() && ttl.toMillis() > 0,
                "Lease TTL must be at least one millisecond");
    }
}
