package io.github.rdt.redisson.schedule;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtScheduleException;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.function.Consumer;

/** The ONLY executable type in the normal worker protocol. Business arguments stay JSON. */
public final class JobInvocation implements Runnable, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final int schemaVersion = 1;
    private final String jobId;
    private final String executionId;
    private final String dispatchToken;
    private transient Consumer<JobInvocation> handler;

    public JobInvocation(String jobId, String executionId, String dispatchToken) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.dispatchToken = dispatchToken;
        validate();
    }
    private void validate() {
        if (schemaVersion != 1 || !bounded(jobId) || !bounded(executionId) || !bounded(dispatchToken)) {
            throw new RdtScheduleException(RdtErrorCode.EXECUTION, "Invalid RDT invocation envelope");
        }
    }

    private boolean bounded(String value) {
        return value != null && !value.isBlank() && value.length() <= 512;
    }

    @Serial
    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        try { validate(); }
        catch (IllegalArgumentException failure) { throw new InvalidObjectException("Invalid RDT invocation envelope"); }
    }

    public String jobId() { return jobId; }
    public String executionId() { return executionId; }
    public String dispatchToken() { return dispatchToken; }
    public void handler(Consumer<JobInvocation> value) { handler = value; }
    @Override
    public void run() {
        if (schemaVersion != 1 || handler == null) throw new RdtScheduleException(RdtErrorCode.EXECUTION, "Unsupported/unregistered RDT invocation");
        handler.accept(this);
    }
}
