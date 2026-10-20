package io.github.rdt.console.alert;

import io.github.rdt.console.connection.ConsoleConnectionManager;
import io.github.rdt.console.management.RdtNamespaceManagementService;
import io.github.rdt.management.DeadLetterStats;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Server-side monitoring continues even when no browser is open. Delivery is the console and rate-limited logs. */
public final class RdtAlertMonitor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RdtAlertMonitor.class);
    private final ConsoleConnectionManager connections;
    private final RdtAlertProperties properties;
    private final Map<String, DeadLetterAlert> snapshots = new ConcurrentHashMap<>();
    private final Map<String, Long> loggedAt = new ConcurrentHashMap<>();
    private ScheduledExecutorService timer;
    private long sampledRevision = -1;

    public RdtAlertMonitor(ConsoleConnectionManager connections, RdtAlertProperties properties) {
        this.connections = connections;
        this.properties = properties;
    }

    public void start() {
        timer = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("rdt-console-alerts").factory());
        timer.scheduleWithFixedDelay(this::sampleAll, 0, properties.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void sampleAll() {
        if (!connections.runtimeConfigured()) return;
        try {
            var namespaces = connections.withManagement(RdtNamespaceManagementService::namespaces);
            snapshots.keySet().retainAll(namespaces);
            loggedAt.keySet().retainAll(namespaces);
            for (String namespace : namespaces) sample(namespace);
        } catch (RuntimeException failure) {
            log.warn("RDT alert namespace discovery failed", failure);
        }
    }

    public synchronized DeadLetterAlert sample(String namespace) {
        long revision = connections.revision();
        DeadLetterAlert result = connections.withManagement(management -> {
            resetIfChanged();
            return sample(management, namespace);
        });
        if (revision != connections.revision()) resetIfChanged();
        return result;
    }

    private DeadLetterAlert sample(RdtNamespaceManagementService management, String namespace) {
        long now = System.currentTimeMillis();
        DeadLetterAlert alert;
        // Validate namespace before caching anything, including failure snapshots.
        var queries = management.queries(namespace);
        try {
            DeadLetterStats stats = queries.deadLetterStats(properties.recentWindow());
            boolean critical = stats.retainedCount() >= properties.criticalCount()
                    || stats.recentCount() >= properties.recentCriticalCount();
            boolean warning = stats.retainedCount() >= properties.warningCount()
                    || stats.oldestAtMillis() > 0 && now - stats.oldestAtMillis() >= properties.oldestWarningAge().toMillis();
            String level = critical ? "CRITICAL" : warning ? "WARNING" : "OK";
            alert = new DeadLetterAlert(namespace, level, stats.retainedCount(), stats.recentCount(), stats.oldestAtMillis(),
                    now, critical || warning ? "死信需要人工处理，请检查下游服务和消费异常后重投。" : "未达到死信告警阈值。");
        } catch (RuntimeException failure) {
            alert = new DeadLetterAlert(namespace, "UNKNOWN", -1, -1, 0, now, "死信监控不可用，请检查 Redis 连接。");
        }
        DeadLetterAlert previous = snapshots.put(namespace, alert);
        boolean changed = previous == null || !previous.level().equals(alert.level());
        if (changed || !alert.level().equals("OK") && now - loggedAt.getOrDefault(namespace, 0L) >= properties.repeatInterval().toMillis()) {
            loggedAt.put(namespace, now);
            if (alert.level().equals("OK")) log.info("RDT dead letter alert cleared namespace={}", namespace);
            else log.warn("RDT dead letter alert namespace={} level={} retained={} recent={} oldestAt={}",
                    namespace, alert.level(), alert.retainedCount(), alert.recentCount(), alert.oldestAtMillis());
        }
        return alert;
    }

    public synchronized DeadLetterAlert snapshot(String namespace) {
        long revision = connections.revision();
        DeadLetterAlert result = connections.withManagement(management -> {
            resetIfChanged();
            management.queries(namespace);
            DeadLetterAlert value = snapshots.get(namespace);
            return value == null || System.currentTimeMillis() - value.checkedAtMillis() > properties.pollInterval().toMillis() * 2
                    ? sample(management, namespace) : value;
        });
        if (revision != connections.revision()) resetIfChanged();
        return result;
    }

    private void resetIfChanged() {
        if (sampledRevision == connections.revision()) return;
        snapshots.clear();
        loggedAt.clear();
        sampledRevision = connections.revision();
    }

    @Override
    public void close() {
        if (timer != null) timer.shutdownNow();
    }
}
