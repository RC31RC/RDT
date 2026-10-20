package io.github.rdt.schedule;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;

/** Framework-neutral runtime settings. Spring configuration is converted at assembly time. */
public record SchedulerSettings(Duration pollInterval,Duration resultTtl,Duration shutdownTimeout,
                                int maxConcurrentTasks,int maxDuePerPoll,Duration workerLease,SchedulePolicy policy) {
    public SchedulerSettings {
        RdtSupport.positive(pollInterval,"schedule poll");RdtSupport.positive(resultTtl,"result ttl");
        RdtSupport.positive(shutdownTimeout,"shutdown timeout");RdtSupport.positive(workerLease,"worker lease");
        RdtSupport.require(maxConcurrentTasks>0 && maxConcurrentTasks<=256 && maxDuePerPoll>0 && maxDuePerPoll<=4096,"Invalid scheduler budget");
        RdtSupport.require(workerLease.compareTo(pollInterval.multipliedBy(3))>=0,"worker lease must cover three polls");
    }
}
