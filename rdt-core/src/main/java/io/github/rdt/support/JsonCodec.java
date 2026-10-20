package io.github.rdt.support;

import io.github.rdt.exception.RdtSerializationException;
import java.lang.reflect.Type;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

/** 使用显式类型的 JSON，不启用默认多态类型或 Java 原生反序列化。 */
public final class JsonCodec {
    private final JsonMapper mapper = JsonMapper.builder().findAndAddModules()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    /** 创建独立 Jackson 3 编解码器，不修改 Spring Boot 的映射器。 */
    public JsonCodec() { }

    /**
     * 序列化数据，支持 null。
     * @param value 数据值
     * @return JSON 文档
     */
    public String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new RdtSerializationException("Cannot encode payload", e);
        }
    }

    /**
     * 使用应用提供的受信任类型反序列化。
     * @param json JSON 文档
     * @param type 类或包含泛型信息的返回类型
     * @param <T> 结果类型
     * @return 反序列化后的值
     */
    public <T> T read(String json, Type type) {
        try {
            return mapper.readValue(json, mapper.constructType(type));
        } catch (Exception e) {
            throw new RdtSerializationException("Cannot decode payload", e);
        }
    }
}
