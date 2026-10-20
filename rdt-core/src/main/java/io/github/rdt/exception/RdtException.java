package io.github.rdt.exception;

/** RDT 自身异常的统一基类；业务异常保持原样传播。 */
public abstract class RdtException extends RuntimeException {
    /** 可稳定序列化的错误分类。 */
    private final RdtErrorCode code;

    /**
     * 创建 RDT 异常。
     * @param code 稳定的错误分类
     * @param message 不含敏感信息的诊断消息
     */
    protected RdtException(RdtErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * 创建保留原始原因的 RDT 异常。
     * @param code 稳定的错误分类
     * @param message 诊断消息
     * @param cause 原始异常
     */
    protected RdtException(RdtErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    /**
     * 返回稳定的错误分类。
     * @return 稳定的错误分类
     */
    public RdtErrorCode code() {
        return code;
    }
}
