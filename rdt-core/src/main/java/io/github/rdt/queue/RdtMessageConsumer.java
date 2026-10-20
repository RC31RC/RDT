package io.github.rdt.queue;

/**
 * 声明式消息消费者的统一处理契约。
 *
 * @param <T> 应用定义的消息描述符，包含类型化载荷和传输元数据
 */
@FunctionalInterface
public interface RdtMessageConsumer<T extends MessageDescriptor<?>> {
    /**
     * 同步消费一条消息。
     *
     * <p>方法正常返回后 RDT 才会确认可靠消息；异常只有通过应用配置的 RetryClassifier 且仍有预算时才重试，
     * 其余失败进入死信。业务副作用仍需使用消息 ID 或业务唯一键保证幂等。</p>
     *
     * @param payload 反序列化并恢复传输元数据后的业务消息描述符
     */
    void consume(T payload) throws Exception;
}
