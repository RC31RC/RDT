package io.github.rdt.support;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtException;

/** Classifies synchronous business callback failures, independently of retry timing and budget. */
@FunctionalInterface
public interface RetryClassifier {
    boolean isRetryable(Exception failure);

    /** Unknown, configuration and permanent failures are not retried automatically. */
    static RetryClassifier infrastructureFailures() {
        return failure -> failure instanceof RdtException rdt
                && (rdt.code() == RdtErrorCode.REDIS_OPERATION
                    || rdt.code() == RdtErrorCode.REDIS_TIMEOUT
                    || rdt.code() == RdtErrorCode.RATE_LIMITED);
    }
}
