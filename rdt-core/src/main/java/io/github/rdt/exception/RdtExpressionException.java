package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtExpressionException extends RdtException {
    public RdtExpressionException(String message) {
        super(RdtErrorCode.EXPRESSION, message);
    }

    public RdtExpressionException(String message, Throwable cause) {
        super(RdtErrorCode.EXPRESSION, message, cause);
    }
}
