package io.github.rdt.redisson.management;

import io.github.rdt.support.KeySpace;
import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import org.redisson.api.RScript;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** Shared indexes keep interactive management requests independent of Redis keyspace size. */
public final class ManagementIndex {
    private final RedissonClient client;
    private final KeySpace keys;

    public ManagementIndex(RedissonClient client, KeySpace keys) {
        this.client = client;
        this.keys = keys;
    }

    public RScoredSortedSet<String> resources(String kind) {
        return client.getScoredSortedSet(keys.key("management-index", kind), StringCodec.INSTANCE);
    }

    public RScoredSortedSet<String> expiries(String kind) {
        return client.getScoredSortedSet(keys.key("management-expiry",kind),StringCodec.INSTANCE);
    }

    public void register(String kind, String name) { resources(kind).add(0, name); }

    public void record(String kind, String id, long time, Duration ttl) {
        client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                local t = redis.call('TIME'); local now = t[1]*1000+math.floor(t[2]/1000)
                redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
                local expires = tonumber(ARGV[3]) == 0 and 9007199254740991 or now+tonumber(ARGV[3])
                redis.call('ZADD', KEYS[2], expires, ARGV[1])
                local old = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 256)
                for _, id in ipairs(old) do
                  redis.call('ZREM', KEYS[1], id); redis.call('ZREM', KEYS[2], id)
                end
                local latest = redis.call('ZREVRANGE', KEYS[2], 0, 0, 'WITHSCORES')
                if #latest > 0 then
                  if tonumber(latest[2]) == 9007199254740991 then
                    redis.call('PERSIST', KEYS[1]); redis.call('PERSIST', KEYS[2])
                  else
                    redis.call('PEXPIREAT', KEYS[1], math.floor(tonumber(latest[2])))
                    redis.call('PEXPIREAT', KEYS[2], math.floor(tonumber(latest[2])))
                  end
                end
                return 1
                """, RScript.ReturnType.LONG, List.of(resources(kind).getName(), expiries(kind).getName()),
                id, time, ttl.toMillis());
    }

    public void remove(String kind, String id) {
        resources(kind).remove(id);
        expiries(kind).remove(id);
    }

    public void prune(String kind) {
        List<Object> scriptKeys = new ArrayList<>();
        scriptKeys.add(resources(kind).getName());
        scriptKeys.add(expiries(kind).getName());
        if (kind.startsWith("attempts:")) {
            String taskId = kind.substring("attempts:".length());
            scriptKeys.add(keys.key("schedule-results", taskId));
            scriptKeys.add(keys.key("management-index", "failures:" + taskId));
            scriptKeys.add(keys.key("management-expiry", "failures:" + taskId));
        }
        client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                local t = redis.call('TIME'); local now = t[1]*1000+math.floor(t[2]/1000)
                local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 4096)
                for _, id in ipairs(ids) do
                  redis.call('ZREM', KEYS[1], id); redis.call('ZREM', KEYS[2], id)
                  if #KEYS > 2 then
                    redis.call('HDEL', KEYS[3], id)
                    redis.call('ZREM', KEYS[4], id); redis.call('ZREM', KEYS[5], id)
                  end
                end
                return #ids
                """, RScript.ReturnType.LONG, scriptKeys);
    }
}
