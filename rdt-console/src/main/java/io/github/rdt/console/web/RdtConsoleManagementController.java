package io.github.rdt.console.web;

import io.github.rdt.console.alert.DeadLetterAlert;
import io.github.rdt.console.alert.RdtAlertMonitor;
import io.github.rdt.console.connection.ConsoleConnectionManager;
import io.github.rdt.management.DeadLetterView;
import io.github.rdt.management.ManagementOverview;
import io.github.rdt.management.ManagementPage;
import io.github.rdt.management.StreamGroupView;
import io.github.rdt.management.StreamMessageView;
import io.github.rdt.management.StreamView;
import io.github.rdt.management.TaskView;
import io.github.rdt.runtime.RuntimeInstance;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.schedule.TaskResult;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/** 多 namespace 的 Redis 共享状态查询和受限管理操作。 */
@RestController
@RequestMapping("${rdt.console.api-path:/rdt/management}")
public final class RdtConsoleManagementController {
    private final ConsoleConnectionManager management;
    private final RdtAlertMonitor alerts;

    /** @param management namespace 服务 */
    public RdtConsoleManagementController(ConsoleConnectionManager management, RdtAlertMonitor alerts) {
        this.management = management;
        this.alerts = alerts;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of("revision", management.revision(), "runtimeConfigured", management.runtimeConfigured());
    }

    @GetMapping("/alerts/dead-letters")
    public DeadLetterAlert deadLetterAlert(
            @RequestParam
            String namespace) {
        return alerts.snapshot(namespace);
    }

    /** @return 当前控制台允许访问的 namespace */
    @GetMapping("/namespaces")
    public List<String> namespaces() {
        return management.withManagement(service -> service.namespaces());
    }

    /** @param namespace namespace @return 概览 */
    @GetMapping("/overview")
    public ManagementOverview overview(
            @RequestParam
            String namespace) {
        return management.withManagement(service -> service.queries(namespace).overview());
    }

    /** @param namespace namespace @return 当前租约有效的业务进程 */
    @GetMapping("/instances")
    public List<RuntimeInstance> instances(
            @RequestParam
            String namespace) {
        return management.withManagement(service -> service.queries(namespace).instances());
    }

    @GetMapping("/streams")
    public ManagementPage<StreamView> streamPage(
            @RequestParam
            String namespace,
            @RequestParam(defaultValue = "0")
            int offset,
            @RequestParam(defaultValue = "50")
            int limit) {
        return management.withManagement(service -> service.queries(namespace).streams(offset, limit));
    }

    @GetMapping("/tasks")
    public ManagementPage<TaskView> taskPage(
            @RequestParam
            String namespace,
            @RequestParam(defaultValue = "0")
            int offset,
            @RequestParam(defaultValue = "50")
            int limit) {
        return management.withManagement(service -> service.queries(namespace).tasks(offset, limit));
    }

    @GetMapping("/dead-letters")
    public ManagementPage<DeadLetterView> deadLetterPage(
            @RequestParam
            String namespace,
            @RequestParam(defaultValue = "0")
            int offset,
            @RequestParam(defaultValue = "50")
            int limit) {
        return management.withManagement(service -> service.queries(namespace).deadLetters(offset, limit));
    }

    @GetMapping("/tasks/{taskId}/attempts")
    public List<TaskResult> attempts(
            @RequestParam
            String namespace,
            @PathVariable
            String taskId,
            @RequestParam(defaultValue = "0")
            int offset,
            @RequestParam(defaultValue = "50")
            int limit) {
        return management.withManagement(service -> service.queries(namespace).taskResults(taskId, offset, limit));
    }

    /** @param namespace namespace @param destination Stream 逻辑目的地 @return Stream 详情 */
    @GetMapping("/streams/{destination}")
    public ResponseEntity<StreamView> stream(
            @RequestParam
            String namespace,
            @PathVariable
            String destination) {
        return management.withManagement(service -> ResponseEntity.of(service.queries(namespace).stream(destination)));
    }

    /** @param namespace namespace @param destination Stream 逻辑目的地 @return 消费组 */
    @GetMapping("/streams/{destination}/groups")
    public ResponseEntity<List<StreamGroupView>> streamGroups(
            @RequestParam
            String namespace,
            @PathVariable
            String destination) {
        return management.withManagement(service -> ResponseEntity.of(service.queries(namespace).stream(destination).map(StreamView::groups)));
    }

    /** @param namespace namespace @param destination Stream 逻辑目的地 @param limit 最多返回条数 @return 最近消息 */
    @GetMapping("/streams/{destination}/messages")
    public List<StreamMessageView> streamMessages(
            @RequestParam
            String namespace,
            @PathVariable
            String destination,
            @RequestParam(defaultValue = "50")
            int limit) {
        return management.withManagement(service -> service.queries(namespace).streamMessages(destination, limit));
    }

    /** @param namespace namespace @param taskId 任务 ID @return 任务详情 */
    @GetMapping("/tasks/{taskId}")
    public ResponseEntity<TaskView> task(
            @RequestParam
            String namespace,
            @PathVariable
            String taskId) {
        return management.withManagement(service -> ResponseEntity.of(service.queries(namespace).task(taskId)));
    }

    /** @param namespace namespace @param taskId 任务 ID @return 是否推进了调度水位 */
    @PostMapping("/tasks/{taskId}/trigger")
    public ManagementCommandResponse triggerTask(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String taskId) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(service.commands(namespace).triggerTask(taskId)));
    }

    @GetMapping("/tasks/{taskId}/executions")
    public List<JobExecution> executions(
            @RequestParam
            String namespace,
            @PathVariable
            String taskId,
            @RequestParam(defaultValue = "0")
            int offset,
            @RequestParam(defaultValue = "50")
            int limit) {
        return management.withManagement(service -> service.queries(namespace).executions(taskId, offset, limit));
    }

    @PostMapping("/tasks/{taskId}/retire")
    public ManagementCommandResponse retire(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String taskId,
            @RequestParam
            boolean confirmed) {
        if (!confirmed) return new ManagementCommandResponse(false);
        return management.withCommandManagement(connectionRevision,
                service -> new ManagementCommandResponse(service.commands(namespace).retireTask(taskId)));
    }

    @PostMapping("/tasks/{taskId}/paused")
    public ManagementCommandResponse paused(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String taskId,
            @RequestParam
            boolean paused) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(service.commands(namespace).pauseTask(taskId, paused)));
    }

    @PostMapping("/tasks/{taskId}/executions/{executionId}/retry")
    public ManagementCommandResponse retryExecution(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String taskId,
            @PathVariable
            String executionId) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(service.commands(namespace).retryExecution(taskId, executionId)));
    }

    @PostMapping("/instances/{instanceId}/draining")
    public ManagementCommandResponse draining(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String instanceId,
            @RequestParam
            boolean draining) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(service.commands(namespace).drainInstance(instanceId, draining)));
    }

    @PostMapping("/cache/clear")
    public CacheClearResponse clearCache(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @RequestParam
            ConsoleConnectionManager.Role role,
            @RequestParam
            boolean confirmed) {
        return management.withCacheCommandManagement(connectionRevision, role,
                service -> new CacheClearResponse(service.commands(namespace).clearCache(confirmed)));
    }

    public record CacheClearResponse(int deleted) { }

    /** 删除指定任务的执行记录。 */
    @DeleteMapping("/tasks/{taskId}/results")
    public ManagementCommandResponse deleteTaskResults(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String taskId) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(service.commands(namespace).deleteTaskResults(taskId)));
    }

    /** @param namespace namespace @param destination Stream 目的地 @param messageId 消息 ID @return 删除结果 */
    @DeleteMapping("/streams/{destination}/messages/{messageId}")
    public ManagementCommandResponse deleteStreamMessage(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String destination,
            @PathVariable
            String messageId) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(
                service.commands(namespace).deleteStreamMessage(destination, messageId)));
    }

    /** @param namespace namespace @param destination 目的地 @param group 消费组 @param messageId 消息 ID @return 删除结果 */
    @DeleteMapping("/dead-letters/{destination}/groups/{group}/messages/{messageId}")
    public ManagementCommandResponse deleteDeadLetter(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String destination,
            @PathVariable
            String group,
            @PathVariable
            String messageId,
            @RequestParam(defaultValue = "")
            String broadcastInstance) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(
                service.commands(namespace).deleteDeadLetter(destination, group, messageId, broadcastInstance)));
    }

    /** 重新投递一条死信。 */
    @PostMapping("/dead-letters/{destination}/groups/{group}/messages/{messageId}/retry")
    public ManagementCommandResponse retryDeadLetter(
            @RequestHeader("X-Rdt-Connection-Revision")
            long connectionRevision,
            @RequestParam
            String namespace,
            @PathVariable
            String destination,
            @PathVariable
            String group,
            @PathVariable
            String messageId,
            @RequestParam(defaultValue = "")
            String broadcastInstance) {
        return management.withCommandManagement(connectionRevision, service -> new ManagementCommandResponse(
                service.commands(namespace).retryDeadLetter(destination, group, messageId, broadcastInstance)));
    }

    /** @param changed 是否改变了 Redis 状态 */
    public record ManagementCommandResponse(boolean changed) { }
}
