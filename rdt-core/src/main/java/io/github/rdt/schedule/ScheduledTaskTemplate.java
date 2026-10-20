package io.github.rdt.schedule;

import io.github.rdt.queue.Subscription;
import java.util.List;
import java.util.concurrent.Callable;

/** 支持集群协调的 Bean 调度，通过有界且自动过期的 JSON 信箱进行跨节点派发。 */
public interface ScheduledTaskTemplate {
    /**
     * 注册参与共享调度的本地可调用任务。
     * @param taskId 集群内稳定的任务 ID
     * @param schedule 各实例完全一致的调度定义
     * @param task 同步业务任务
     * @return 本地取消句柄，关闭后保留共享调度状态
     */
    default Subscription register(String taskId, ScheduleSpec schedule, Callable<?> task) {
        return register(taskId, "", schedule, ScheduleOptions.defaults(), task);
    }

    /**
     * 注册带展示名的任务；名称变化不应创建新任务或重置调度进度。
     * @param taskId 集群稳定 ID
     * @param displayName 管理端展示名，空值不覆盖已有名称
     * @param schedule 调度定义
     * @param task 同步业务任务
     * @return 本地取消句柄
     */
    default Subscription register(String taskId, String displayName, ScheduleSpec schedule, Callable<?> task) {
        return register(taskId, displayName, schedule, ScheduleOptions.defaults(), task);
    }

    /**
     * 注册带独立策略的集群任务。未设置的字段继承 RDT 公共默认值。
     * 失败模式为 CONTINUE 时，退避参数不改变正常触发时间线。
     *
     * @param taskId 集群稳定 ID
     * @param displayName 管理端展示名
     * @param schedule 正常调度定义
     * @param options 当前任务的策略覆盖
     * @param task 同步业务任务
     * @return 本地取消句柄
     */
    default Subscription register(String taskId, String displayName, ScheduleSpec schedule,
                          ScheduleOptions options, Callable<?> task) {
        return registerContextual(taskId,displayName,schedule,options,context -> task.call());
    }

    Subscription registerContextual(String taskId,String displayName,ScheduleSpec schedule,
                                   ScheduleOptions options,ContextualTask task);

    default Subscription register(String taskId, ScheduleSpec schedule, ScheduleOptions options, Callable<?> task) {
        return register(taskId, "", schedule, options, task);
    }

    /**
     * 读取注解任务、编程式调度保留的执行记录。
     * @param taskId 任务 ID
     * @return 执行标识到执行结果的映射
     */
    List<TaskResult> results(String taskId, int offset, int limit);
}
