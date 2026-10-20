package io.github.rdt.management;

import java.util.List;

/**
 * Stream 消费组只读视图。
 *
 * @param name 消费组名称
 * @param consumers 消费者数量
 * @param pending 待确认消息数
 * @param lag 尚未投递到消费组的消息数
 * @param lastDeliveredId 最后投递消息 ID
 * @param pendingTotal PEL 总数
 * @param pendingLowestId PEL 最小消息 ID
 * @param pendingHighestId PEL 最大消息 ID
 * @param consumerViews 消费者列表
 */
public record StreamGroupView(String name, int consumers, long pending, long lag,
                              String lastDeliveredId, long pendingTotal,
                              String pendingLowestId, String pendingHighestId,
                              List<StreamConsumerView> consumerViews) {
    /** 防止调用方修改查询结果。 */
    public StreamGroupView {
        consumerViews = List.copyOf(consumerViews);
    }
}
