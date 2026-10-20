package io.github.rdt.lock;

/** 表示一次加锁层级；必须在获取线程关闭，推荐使用 try-with-resources。 */
public interface LockHandle extends AutoCloseable {
    /**
     * 返回该句柄是否仍持有有效锁。
     * @return 该句柄是否仍持有有效锁
     */
    boolean isHeld();

    /** 仅释放本次获取的锁层级，重复关闭不会产生额外副作用。 */
    @Override
    void close();
}
