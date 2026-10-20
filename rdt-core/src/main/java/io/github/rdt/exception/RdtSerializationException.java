package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtSerializationException extends RdtException {
    public RdtSerializationException(String message) {
        super(RdtErrorCode.SERIALIZATION, message);
    }

    public RdtSerializationException(String message, Throwable cause) {
        super(RdtErrorCode.SERIALIZATION, message, cause);
    }
}
