package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtRateLimitException extends RdtException {
    public RdtRateLimitException(String message) {
        super(RdtErrorCode.RATE_LIMITED, message);
    }

    public RdtRateLimitException(String message, Throwable cause) {
        super(RdtErrorCode.RATE_LIMITED, message, cause);
    }
}
