package io.github.rdt.redisson.cache;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.KeySpace;

import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.util.function.Consumer;

/** Each region has its own channel; state stores never use this bus. */
final class CacheInvalidationBus {
    private final RTopic topic;
    private final RTopic clearing;
    private int clearingListener = -1;

    CacheInvalidationBus(RedissonClient client, KeySpace keys, KeySpace rootKeys) {
        topic = client.getTopic(keys.key("cache-events", "invalidate"), StringCodec.INSTANCE);
        clearing = client.getTopic(rootKeys.key("cache-events", "clear-all"), StringCodec.INSTANCE);
    }

    int subscribe(Consumer<String> invalidator) {
        return RedisSupport.redis(
                "subscribe cache invalidation",
                () -> {
                    int id =
                            topic.addListener(
                                    String.class, (channel, key) -> invalidator.accept(key));
                    try {
                        clearingListener =
                                clearing.addListener(
                                        String.class, (channel, key) -> invalidator.accept("*"));
                        return id;
                    } catch (RuntimeException failure) {
                        topic.removeListener(id);
                        throw failure;
                    }
                });
    }

    void publish(String key, boolean localSubscriber) {
        RedisSupport.redisRun(
                "invalidate local cache copies",
                () -> {
                    // A writer without L1 can still have readers with L1 on other nodes.
                    // Do not cache a zero subscriber count: a new reader must see every later
                    // write.
                    if (localSubscriber || topic.countSubscribers() > 0) {
                        topic.publish(key);
                    }
                });
    }

    void unsubscribe(int listenerId) {
        RedisSupport.redisRun(
                "unsubscribe cache invalidation",
                () -> {
                    topic.removeListener(listenerId);
                    if (clearingListener >= 0) clearing.removeListener(clearingListener);
                });
    }
}
