package io.github.rdt.redisson.queue;

import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.FailedMessageStore;
import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** 持久化 Redis 死信存储；通过 MapCache 为每条死信设置独立 TTL。 */
public final class RedisFailedMessageStore implements FailedMessageStore {
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final Duration ttl;

    /**
     * 创建 Redis 失败记录存储。
     * @param client 共享客户端
     * @param keys 命名空间
     * @param json 编解码器
     */
    public RedisFailedMessageStore(RedissonClient client, KeySpace keys, JsonCodec json) {
        this(client, keys, json, Duration.ZERO);
    }

    /**
     * 创建带生命周期边界的死信存储。
     *
     * @param client 共享 Redisson 客户端
     * @param keys 应用命名空间
     * @param json JSON 编解码器
     * @param ttl 单条死信保留时间；零表示永久保留
     */
    public RedisFailedMessageStore(RedissonClient client, KeySpace keys, JsonCodec json, Duration ttl) {
        this.client = client;
        this.keys = keys;
        this.json = json;
        this.ttl = ttl == null ? Duration.ZERO : ttl;
    }

    @Override
    public void save(FailedMessage failure) {
        save(failure, ttl);
    }

    @Override
    public void save(FailedMessage failure, Duration retention) {
        RdtSupport.require(retention != null && !retention.isNegative(), "dead letter ttl must not be negative");
        String name = keys.key("dead-letters", failure.destination());
        String field = failure.field();
        RedisSupport.redisRun("persist dead letter", () -> {
            if (retention.isZero()) {
                client.<String, String>getMapCache(name, StringCodec.INSTANCE).put(field, json.write(failure));
            } else {
                client.<String, String>getMapCache(name, StringCodec.INSTANCE)
                        .fastPut(field, json.write(failure), retention.toMillis(), TimeUnit.MILLISECONDS);
            }
            new ManagementIndex(client, keys).record("dead-letters",
                    json.write(new String[]{failure.destination(), field}), System.currentTimeMillis(), retention);
        });
    }
}
