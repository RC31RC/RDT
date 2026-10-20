package io.github.rdt.ratelimit;

/** 相互独立的全局限流策略。 */
public enum RateMode {
    /** 限制每个时间间隔内的许可数，在所有客户端间共享。 */
    QPS,
    /** 限制同时执行的同步调用数量。 */
    CONCURRENCY
}
