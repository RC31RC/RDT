package io.github.rdt.schedule;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtScheduleException;
import java.util.function.BooleanSupplier;

/** Explicit synchronous checkpoint, never ThreadLocal and never a substitute for downstream fencing. */
public record TaskExecutionContext(String jobId,String executionId,String dispatchToken,BooleanSupplier lease) {
    public void assertLease() {
        if (Thread.currentThread().isInterrupted() || !lease.getAsBoolean()) throw new RdtScheduleException(RdtErrorCode.EXECUTION, "RDT task lease lost: " + jobId);
    }
}
