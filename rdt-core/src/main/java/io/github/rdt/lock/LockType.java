package io.github.rdt.lock;

/** 所有锁变体均可重入，且由线程持有。 */
public enum LockType {
    /** 非公平互斥锁。 */
    REENTRANT,
    /** 按先到先得顺序排队的公平互斥锁。 */
    FAIR,
    /** 共享读锁，与 WRITE 使用同一个实际键。 */
    READ,
    /** 独占写锁，与 READ 使用同一个实际键。 */
    WRITE
}
