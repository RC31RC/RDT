package io.github.rdt.id;

/** 序列分配模式。 */
public enum IdMode {
    /** 客户端号段分配，保证唯一，但不保证全局有序或无空洞。 */
    SEGMENT,
    /** 每次分配执行一次 Redis 递增，不代表业务事务编号无空洞。 */
    ATOMIC
}
