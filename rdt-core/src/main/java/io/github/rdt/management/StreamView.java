package io.github.rdt.management;

import java.util.List;

/**
 * Redis Stream 只读视图。
 *
 * @param destination 逻辑目的地
 * @param redisKey 实际 Redis 键
 * @param length Stream 长度
 * @param groups 消费组列表
 * @param lastGeneratedId 最近生成的消息 ID
 * @param displayName 目的地展示名，未注册时回退到目的地
 */
public record StreamView(String destination, String redisKey, long length,
                         List<StreamGroupView> groups, String lastGeneratedId, String displayName) {
    /** 防止调用方修改查询结果。 */
    public StreamView {
        groups = List.copyOf(groups);
        displayName = displayName == null || displayName.isBlank() ? destination : displayName;
    }
}
