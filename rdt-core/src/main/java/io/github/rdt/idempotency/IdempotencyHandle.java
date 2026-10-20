package io.github.rdt.idempotency;

/**
 * 一次幂等执行的租约句柄。
 *
 * <p>业务成功后必须调用 {@link #complete()}；未完成的句柄关闭时会释放占位，
 * 允许后续请求在租约到期前重试。</p>
 */
public interface IdempotencyHandle extends AutoCloseable {
    /**
     * 将本次执行标记为已完成。
     *
     * @return 当前句柄仍是所有者且状态修改成功
     */
    boolean complete();

    /**
     * 主动释放未完成的占位。
     *
     * @return 当前句柄仍是所有者且删除成功
     */
    boolean abort();

    /**
     * 返回是否已经完成。
     *
     * @return 已完成状态
     */
    boolean isCompleted();

    /** 未完成的句柄关闭时释放占位。 */
    @Override
    default void close() {
        if (!isCompleted()) {
            abort();
        }
    }
}
