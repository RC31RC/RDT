package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtLockException extends RdtException {
    public RdtLockException(RdtErrorCode code, String message) {
        super(code, message);
    }

    public RdtLockException(RdtErrorCode code, String message, Throwable cause) {
        super(code, message, cause);
    }
}
