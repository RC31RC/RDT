package io.github.rdt.redisson.queue;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;

import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class QueueStreams {
    private final RedissonClient client;
    private final KeySpace keys;
    private final Set<String> indexedStreams = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> orderedTopics = new ConcurrentHashMap<>();

    QueueStreams(RedissonClient client, KeySpace keys) {
        this.client = client;
        this.keys = keys;
    }

    String partitionTopic(String topic, int partition) {
        return topic + "~ordered~" + partition;
    }

    void registerPartitions(String topic, int partitions) {
        RdtSupport.require(
                partitions >= 1 && partitions <= 128,
                "Ordered partition count must be between 1 and 128");
        Integer known = orderedTopics.get(topic);
        if (known != null) {
            RdtSupport.require(
                    known == partitions, "Conflicting ordered partition count for " + topic);
            return;
        }
        var metadata =
                client.<String, String>getMap(
                        keys.key("queue-metadata", topic), StringCodec.INSTANCE);
        metadata.fastPutIfAbsent("orderedPartitions", Integer.toString(partitions));
        RdtSupport.require(
                Integer.toString(partitions).equals(metadata.get("orderedPartitions")),
                "Conflicting ordered partition count for " + topic);
        orderedTopics.put(topic, partitions);
    }

    void createGroup(QueueSubscription spec) {
        try {
            stream(spec.destination())
                    .createGroup(
                            StreamCreateGroupArgs.name(spec.group())
                                    .id(StreamMessageId.ALL)
                                    .makeStream());
        } catch (RedisException e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                throw new RdtRedisException(
                        RdtErrorCode.REDIS_OPERATION, "Cannot create consumer group", e);
            }
        }
    }

    RStream<String, String> stream(String destination) {
        if (indexedStreams.add(destination)) {
            try {
                new ManagementIndex(client, keys).register("streams", destination);
            } catch (RuntimeException failure) {
                indexedStreams.remove(destination);
                throw failure;
            }
        }
        return client.getStream(keys.key("stream", destination), StringCodec.INSTANCE);
    }
}
