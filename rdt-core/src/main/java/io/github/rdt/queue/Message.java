package io.github.rdt.queue;

import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.RdtSupport;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * 稳定的消息信封；业务载荷使用显式 JSON，不传输任意 Java 对象。
 * @param id 重试期间保持不变的消息 ID
 * @param payload JSON 业务载荷
 * @param createdAt ISO-8601 格式的创建时间
 */
public record Message(String id, String payload, String createdAt, int schemaVersion,
                      String key, String orderingKey, Map<String, String> headers) {
    public Message {
        RdtSupport.component(id);
        RdtSupport.require(payload != null && createdAt != null && !createdAt.isBlank(), "Message payload and timestamp are required");
        RdtSupport.require(schemaVersion == 1, "Unsupported message schema version: " + schemaVersion);
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        RdtSupport.require(headers.size() <= 32, "Message headers must not exceed 32 entries");
    }

    /**
     * 创建新事件。
     * @param payload 业务数据
     * @param codec 序列化器
     * @return 新消息信封
     */
    public static Message create(Object payload, JsonCodec codec) {
        return new Message(UUID.randomUUID().toString(), codec.write(payload), Instant.now().toString(), 1, null, null, Map.of());
    }

    /**
     * 使用受信任的目标类型读取业务数据。
     * @param codec 序列化器
     * @param type 目标类型
     * @param <T> 目标类型
     * @return 反序列化后的数据
     */
    public <T> T body(JsonCodec codec, Type type) {
        return codec.read(payload, type);
    }
}
