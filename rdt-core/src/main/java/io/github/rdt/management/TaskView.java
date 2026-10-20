package io.github.rdt.management;


/**
 * 持久化调度任务只读视图。
 *
 * @param taskId 逻辑任务 ID
 * @param redisKey 状态 Redis 键
 * @param definition 注册时的调度定义
 * @param nextAtMillis 下次计划时间，Long.MAX_VALUE 表示不再触发
 * @param displayName 可读名称，未注册名称时回退到任务 ID
 */
public record TaskView(String taskId, String redisKey, String definition,
                       long nextAtMillis, String displayName,
                       boolean paused, String executionStatus, String executionId, String route) {
    /** 统一任务标识与展示名。 */
    public TaskView {
        displayName = displayName == null || displayName.isBlank() ? taskId : displayName;
    }
}
