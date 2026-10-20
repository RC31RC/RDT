package io.github.rdt.management;

/**
 * 当前命名空间的只读运行概览。
 *
 * @param namespace 命名空间
 * @param streamCount Stream 数量
 * @param streamLength Stream 条目总数
 * @param groupCount 消费组数量
 * @param pendingCount 待确认消息总数
 * @param taskCount 调度任务数量
 * @param taskFailureCount 任务失败记录数量
 * @param deadLetterCount 死信数量
 */
public record ManagementOverview(String namespace, long streamCount, long streamLength,
                                  long groupCount, long pendingCount, long taskCount,
                                  long taskFailureCount, long deadLetterCount, boolean partial) {
}
