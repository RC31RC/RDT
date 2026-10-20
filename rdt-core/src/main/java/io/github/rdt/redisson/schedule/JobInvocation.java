package io.github.rdt.redisson.schedule;

import io.github.rdt.support.RdtSupport;

/** Data-only worker protocol; executable business code is never deserialized from Redis. */
public record JobInvocation(String jobId, String executionId, String dispatchToken) {
    public JobInvocation {
        for (String value : new String[]{jobId, executionId, dispatchToken})
            RdtSupport.require(value != null && !value.isBlank() && value.length() <= 512,
                    "Invalid RDT invocation envelope");
    }
}
