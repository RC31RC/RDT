package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtScheduleException extends RdtException {
    public RdtScheduleException(RdtErrorCode code, String message) {
        super(code, message);
    }

    public RdtScheduleException(RdtErrorCode code, String message, Throwable cause) {
        super(code, message, cause);
    }
}
