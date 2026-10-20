package io.github.rdt.exception;

/** 稳定、可供程序识别的错误分类。 */
public enum RdtErrorCode {
    /** 配置或参数无效。 */
    CONFIGURATION,
    /** Database consistency storage or transaction failed. */
    CONSISTENCY,
    /** Local diagnostic archive failed. */
    ARCHIVE,
    /** Redis 命令失败，实际执行结果可能未知。 */
    REDIS_OPERATION,
    /** Redis 在客户端等待窗口内没有返回结果。 */
    REDIS_TIMEOUT,
    /** 未在等待期限内获取锁。 */
    LOCK_TIMEOUT,
    /** 所有权已过期，或尝试在非持有线程释放锁。 */
    LOCK_OWNERSHIP,
    /** 当前线程没有可用于追加锁的作用域。 */
    LOCK_SCOPE,
    /** 调用线程被中断，中断标记已恢复。 */
    INTERRUPTED,
    /** 并发容量或速率配额耗尽。 */
    RATE_LIMITED,
    /** Reliable queue rejected admission without accepting the message. */
    QUEUE_CAPACITY,
    /** Encoded message exceeds the configured byte limit. */
    MESSAGE_TOO_LARGE,
    /** 序列化或反序列化失败。 */
    SERIALIZATION,
    /** 监听器或调度任务执行失败。 */
    EXECUTION,
    /** 表达式求值失败。 */
    EXPRESSION
}
