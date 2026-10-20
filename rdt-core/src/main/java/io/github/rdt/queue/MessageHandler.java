package io.github.rdt.queue;

/** 同步处理器契约；应使用消费组和消息 ID 实现副作用幂等。 */
@FunctionalInterface
public interface MessageHandler {
    /**
     * 处理一条消息信封。
     * @param message 待处理事件
     * @throws Exception 可参与重试的业务异常
     */
    void handle(Message message) throws Exception;
}
