package io.github.rdt.schedule;

/**
 * 声明式集群定时任务的统一执行契约。
 *
 * <p>Spring 项目使用 starter 中的 {@code RdtScheduled} 声明实现类，RDT
 * 启动时会注册该 Bean；到达共享触发时间后通过 Spring 代理调用 {@link #execute()}。</p>
 */
public interface RdtScheduledTask {
    /** The business task supplies its cadence, retention and retry settings. */
    TaskDescriptor descriptor();

    /**
     * 执行一次同步任务。
     *
     * <p>方法正常返回表示本次执行成功；抛出异常表示失败。只有启用 RETRY、通过
     * 应用配置的 RetryClassifier 且仍有预算时，才重试本次触发。</p>
     */
    void execute() throws Exception;
}
