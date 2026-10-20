package io.github.rdt.schedule;

/** Persistent, recoverable logical trigger. Each dispatch carries a fresh attempt token. */
public record JobExecution(String executionId, String jobId, long scheduledAt, int attempt,
                           String status, String instanceId, String runId, String dispatchToken,
                           long updatedAt, long retryAt, String detail, int dispatchRejections) {
    public JobExecution transition(String nextStatus, String worker, String run, String token,
                                   long now, long retry, int nextAttempt, String reason) {
        return new JobExecution(executionId, jobId, scheduledAt, nextAttempt, nextStatus,
                worker, run, token, now, retry, reason, "RUNNING".equals(nextStatus) ? 0 : dispatchRejections);
    }
    public JobExecution rejected(String nextStatus,long now,long retry,String reason) {
        return new JobExecution(executionId,jobId,scheduledAt,attempt,nextStatus,"","","",now,retry,reason,dispatchRejections+1);
    }
    public boolean terminal() {
        return "SUCCEEDED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status);
    }
}
