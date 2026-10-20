package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtConsistencyException extends RdtException {
    public RdtConsistencyException(String message) {
        super(RdtErrorCode.CONSISTENCY, message);
    }

    public RdtConsistencyException(String message, Throwable cause) {
        super(RdtErrorCode.CONSISTENCY, message, cause);
    }
}
