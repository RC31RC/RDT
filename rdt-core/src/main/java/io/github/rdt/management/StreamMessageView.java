package io.github.rdt.management;

import java.util.Map;

/** Redis Stream 中一条消息的管理视图。 */
public record StreamMessageView(String id, Map<String, String> fields) {
    /** 防止调用方修改消息字段。 */
    public StreamMessageView {
        fields = Map.copyOf(fields);
    }
}
