package io.github.rdt.schedule;

/**
 * 任务 ID 已存在时，调度定义发生变化的处理策略。
 */
public enum ScheduleDefinitionChangePolicy {
    /**
     * 拒绝启动，避免相同任务 ID 被不同版本悄悄改变语义。
     */
    FAIL,
    /**
     * 保留任务 ID，使用新定义重置下一次触发时间。
     */
    UPDATE
}
