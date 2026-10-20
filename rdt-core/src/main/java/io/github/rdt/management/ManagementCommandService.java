package io.github.rdt.management;

import io.github.rdt.queue.FailedMessage;

/**
 * 对单个 RDT 命名空间执行受限管理操作。
 *
 * <p>命令只接受逻辑资源名，调用方不得传入 Redis 实际键。Stream 删除不能撤销，且不能
 * 中断已经开始的消费者业务回调；HTTP 边界应增加认证、权限、审计和二次确认。</p>
 */
public interface ManagementCommandService {
    /**
     * 请求已注册的本地调度任务尽快被业务实例执行。
     *
     * <p>创建持久化 execution，由具备该 job 能力的业务实例执行。</p>
     *
     * @param taskId 逻辑任务 ID
     * @return 已创建执行时为 {@code true}；任务不存在或存在活跃执行时为 {@code false}
     */
    boolean triggerTask(String taskId);

    boolean pauseTask(String taskId, boolean paused);
    boolean drainInstance(String instanceId, boolean draining);
    boolean retryExecution(String taskId, String executionId);

    /** 删除一个任务已经保存的全部执行记录，不删除任务定义。 */
    boolean deleteTaskResults(String taskId);

    /**
     * 确认所有消费组中的待确认记录并删除一个 Stream 条目。
     *
     * @param destination 逻辑目的地
     * @param messageId Redis Stream 消息 ID
     * @return 删除了条目时为 {@code true}
     */
    boolean deleteStreamMessage(String destination, String messageId);

    /**
     * 删除一条保存在 Redis 中的死信记录。
     *
     * @param destination 原始逻辑目的地
     * @param group 消费组
     * @param messageId Redis Stream 消息 ID
     * @return 删除了记录时为 {@code true}
     */
    boolean deleteDeadLetter(String destination, String group, String messageId, String broadcastInstance);

    /** 将死信重新投递到原始 Stream，并删除已成功重投的死信记录。 */
    boolean retryDeadLetter(String destination, String group, String messageId, String broadcastInstance);

    /** Clear cached bucket entries after an explicit operator confirmation. */
    int clearCache(boolean confirmed);
}
