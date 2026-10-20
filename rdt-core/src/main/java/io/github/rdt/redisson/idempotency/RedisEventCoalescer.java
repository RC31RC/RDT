package io.github.rdt.redisson.idempotency;

import io.github.rdt.idempotency.EventCoalescer;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** A single Redis key and server clock keep matching atomic across nodes and cluster slots. */
public final class RedisEventCoalescer implements EventCoalescer {
    private static final String OBSERVE = """
            local time = redis.call('TIME')
            local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
            local window = tonumber(ARGV[3])
            local rounds = cjson.decode(redis.call('GET', KEYS[1]) or '[]')
            local live = {}
            for _, round in ipairs(rounds) do
                if now - round.time <= window then table.insert(live, round) end
            end
            for _, round in ipairs(live) do
                if round.events[ARGV[1]] == ARGV[2] then return round.id end
            end
            local selected = nil
            for _, round in ipairs(live) do
                if not round.events[ARGV[1]] and round.count < tonumber(ARGV[5]) then
                    selected = round
                    break
                end
            end
            if not selected then
                if #live >= tonumber(ARGV[4]) then
                    return redis.error_reply('RDT correlation capacity exceeded')
                end
                selected = {id=ARGV[6], time=now, events={}, count=0}
                table.insert(live, selected)
            end
            selected.events[ARGV[1]] = ARGV[2]
            selected.count = selected.count + 1
            redis.call('SET', KEYS[1], cjson.encode(live), 'PX', window * 2)
            return selected.id
            """;
    private final RedissonClient redis;
    private final KeySpace keys;

    public RedisEventCoalescer(RedissonClient redis, KeySpace keys) {
        this.redis = Objects.requireNonNull(redis);
        this.keys = Objects.requireNonNull(keys);
    }

    @Override
    public String observe(String fingerprint, String source, String eventId, Policy policy) {
        RdtSupport.component(fingerprint);
        RdtSupport.require(source != null && !source.isBlank() && source.length() <= 512
                        && eventId != null && !eventId.isBlank() && eventId.length() <= 512,
                "Bounded source and event identity are required");
        Objects.requireNonNull(policy, "policy");
        return RedisSupport.redis("correlate event observations", () ->
                redis.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, OBSERVE,
                        RScript.ReturnType.VALUE, List.of(keys.key("event-correlation", fingerprint)),
                        source, eventId, policy.window().toMillis(), policy.maxRounds(),
                        policy.maxSources(), UUID.randomUUID().toString()));
    }
}
