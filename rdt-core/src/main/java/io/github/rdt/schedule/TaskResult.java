package io.github.rdt.schedule;

/**
 * 一条保留的集群任务执行记录。
 * @param taskId 逻辑任务 ID
 * @param scheduledAt 计划执行时间
 * @param startedAt 实际开始时间
 * @param completedAt 完成时间
 * @param instanceId 进程唯一标识
 * @param successful 回调是否正常完成
 * @param detail 长度受限的 JSON 结果或异常诊断信息
 */
public record TaskResult(String taskId, String scheduledAt, String startedAt, String completedAt,
                         String instanceId, boolean successful, String detail,
                         String runId, String executionId, int attempt, String status) {
}
