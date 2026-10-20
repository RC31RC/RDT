package io.github.rdt.exception;

/** Component-specific RDT failure; business exceptions are not translated into this type. */
public final class RdtConfigurationException extends RdtException {
    public RdtConfigurationException(String message) {
        super(RdtErrorCode.CONFIGURATION, message);
    }

    public RdtConfigurationException(String message, Throwable cause) {
        super(RdtErrorCode.CONFIGURATION, message, cause);
    }
}
