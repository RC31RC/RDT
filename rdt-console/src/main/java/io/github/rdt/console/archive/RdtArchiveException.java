package io.github.rdt.console.archive;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtException;

/** Local archive failure; staging cleanup never hides the original cause. */
public final class RdtArchiveException extends RdtException {
    public RdtArchiveException(String message) {
        super(RdtErrorCode.ARCHIVE, message);
    }

    public RdtArchiveException(String message, Throwable cause) {
        super(RdtErrorCode.ARCHIVE, message, cause);
    }
}
