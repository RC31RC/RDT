package io.github.rdt.management;

import io.github.rdt.runtime.RuntimeInstance;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.schedule.TaskResult;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Bounded snapshots of current shared runtime state. */
public interface ManagementQueryService {
    ManagementOverview overview();

    DeadLetterStats deadLetterStats(Duration recentWindow);
    ManagementPage<StreamView> streams(int offset, int limit);
    Optional<StreamView> stream(String destination);
    List<StreamMessageView> streamMessages(String destination, int limit);
    ManagementPage<TaskView> tasks(int offset, int limit);
    Optional<TaskView> task(String taskId);
    List<TaskResult> taskResults(String taskId, int offset, int limit);
    List<JobExecution> executions(String taskId, int offset, int limit);
    ManagementPage<DeadLetterView> deadLetters(int offset, int limit);
    List<RuntimeInstance> instances();
}
