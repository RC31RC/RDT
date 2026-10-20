package io.github.rdt.redisson.queue;

import io.github.rdt.queue.Message;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueuePolicy;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;

import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

final class QueueProducer {
    private static final Logger log = LoggerFactory.getLogger(QueueProducer.class);
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final RuntimeIdentity identity;
    private final StreamRetention retention;
    private final QueueStreams streams;
    private final AtomicBoolean closed;
    private final QueueAdmission admission;
    private final DelayTransferCoordinator delayLeader;
    private final AtomicInteger delayCursor = new AtomicInteger();

    QueueProducer(
            RedissonClient client,
            KeySpace keys,
            JsonCodec json,
            RuntimeIdentity identity,
            StreamRetention retention,
            QueueStreams streams,
            AtomicBoolean closed, QueuePolicy policy) {
        admission = new QueueAdmission(client, keys, policy);
        delayLeader = new DelayTransferCoordinator(client, keys, identity);
        this.client = client;
        this.keys = keys;
        this.json = json;
        this.identity = identity;
        this.retention = retention;
        this.streams = streams;
        this.closed = closed;
    }

    public String send(String destination, Object payload, QueueMode mode) {
        Message message = Message.create(payload, json);
        RdtSupport.require(mode != null && !closed.get(), "Transport is closed or mode is missing");
        RedisSupport.redisRun(
                "send message",
                () -> {
                    String encoded = json.write(message);
                    admission.validate(encoded);
                    switch (mode) {
                        case STREAM, RELIABLE_BROADCAST -> {
                            RStream<String, String> target = streams.stream(destination);
                            admission.accept(destination, encoded, null);
                            retention.trimStream(target, destination);
                        }
                        case BROADCAST ->
                                client.getTopic(
                                                keys.key("topic", destination),
                                                StringCodec.INSTANCE)
                                        .publish(encoded);
                    }
                });
        return message.id();
    }

    public String sendMessage(String destination, Message message, QueueMode mode) {
        RdtSupport.require(
                !closed.get() && message != null && mode != null,
                "Transport closed or envelope invalid");
        RdtSupport.require(message.orderingKey() == null, "Use sendOrdered for ordered envelopes");
        RedisSupport.redisRun(
                "send JSON envelope",
                () -> {
                    String encoded = json.write(message);
                    admission.validate(encoded);
                    if (mode == QueueMode.BROADCAST) {
                        client.getTopic(keys.key("topic", destination), StringCodec.INSTANCE)
                                .publish(encoded);
                    } else {
                        RStream<String, String> target = streams.stream(destination);
                        admission.accept(destination, encoded, null);
                        retention.trimStream(target, destination);
                    }
                });
        return message.id();
    }

    public String sendDelayed(String destination, Object payload, Duration delay) {
        RdtSupport.require(!closed.get(), "Transport is closed");
        RdtSupport.positive(delay, "message delay");
        Message message = Message.create(payload, json);
        // Register first. A crashed producer can leave an empty topic, never an undiscoverable
        // accepted message.
        client.<String>getScoredSortedSet(keys.key("delay-topics", "topics"), StringCodec.INSTANCE)
                .add(0, destination);
        admission.accept(destination, json.write(message), System.currentTimeMillis() + delay.toMillis());
        return message.id();
    }

    public String sendOrdered(String topic, String orderingKey, Object payload, int partitions) {
        RdtSupport.require(
                !closed.get() && orderingKey != null && !orderingKey.isBlank(),
                "Ordered send requires an open transport and ordering key");
        streams.registerPartitions(topic, partitions);
        CRC32 hash = new CRC32();
        hash.update(orderingKey.getBytes(StandardCharsets.UTF_8));
        int partition = (int) (hash.getValue() % partitions);
        Message base = Message.create(payload, json);
        Message message =
                new Message(
                        base.id(),
                        base.payload(),
                        base.createdAt(),
                        1,
                        orderingKey,
                        orderingKey,
                        Map.of());
        String destination = streams.partitionTopic(topic, partition);
        admission.accept(destination, json.write(message), null);
        retention.trimStream(streams.stream(destination), destination);
        return message.id();
    }

    private String delayKey(String topic) {
        // Namespace tags keep the delay index and destination Stream in the same slot.
        return keys.key("message-delay", topic);
    }

    void transferDelayed() {
        if (closed.get() || !identity.accepting() || !delayLeader.isLeader()) return;
        long deadline = System.nanoTime() + 50_000_000L;
        do {
            int offset = delayCursor.get();
            var topics = client.<String>getScoredSortedSet(
                    keys.key("delay-topics", "topics"), StringCodec.INSTANCE).valueRange(offset, offset + 63);
            delayCursor.set(topics.size() < 64 ? 0 : offset + 64);
            for (String topic : topics) {
                try {
                    Number moved;
                    do {
                        moved = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                                local t = redis.call('TIME')
                                local now = t[1] * 1000 + math.floor(t[2] / 1000)
                                local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 256)
                                for _, value in ipairs(due) do
                                  redis.call('XADD', KEYS[2], '*', 'message', value)
                                  redis.call('ZREM', KEYS[1], value)
                                end
                                if #due > 0 then
                                  redis.call('HINCRBY', KEYS[4], 'delay', -#due)
                                  redis.call('HINCRBY', KEYS[4], 'stream', #due)
                                end
                                return #due
                                """, RScript.ReturnType.LONG,
                                List.of(delayKey(topic), streams.stream(topic).getName(),
                                        keys.key("queue-capacity", topic),
                                        keys.key("queue-capacity-state", topic)));
                    } while (moved.intValue() == 256 && System.nanoTime() < deadline && !closed.get());
                    retention.trimStream(streams.stream(topic), topic);
                } catch (RuntimeException error) {
                    log.warn("Reliable delay transfer failed topic={}", topic, error);
                }
            }
            if (topics.size() < 64) break;
        } while (System.nanoTime() < deadline && !closed.get());
    }

    void close() {
        try {
            delayLeader.close();
        } catch (RuntimeException error) {
            log.debug("Delayed transfer lease release failed during shutdown", error);
        }
    }
}
