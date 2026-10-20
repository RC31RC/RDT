package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtQueueException extends RdtException {
    public RdtQueueException(String message) {
        super(RdtErrorCode.EXECUTION, message);
    }

    public RdtQueueException(String message, Throwable cause) {
        super(RdtErrorCode.EXECUTION, message, cause);
    }
}
