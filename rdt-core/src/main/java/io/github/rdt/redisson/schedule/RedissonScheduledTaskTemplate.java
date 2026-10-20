package io.github.rdt.redisson.schedule;

import java.util.Objects;
import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtException;
import io.github.rdt.lock.DistributedLockTemplate;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockType;
import io.github.rdt.queue.Subscription;
import io.github.rdt.redisson.management.ManagementIndex;
import io.github.rdt.redisson.runtime.RedisRuntimeDirectoryProvider;
import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.runtime.RdtMetrics;
import io.github.rdt.runtime.RuntimeComponent;
import io.github.rdt.runtime.RuntimeDirectoryProvider;
import io.github.rdt.runtime.RuntimeIdentity;
import io.github.rdt.runtime.RuntimeInstance;
import io.github.rdt.schedule.ContextualTask;
import io.github.rdt.schedule.CronParser;
import io.github.rdt.schedule.CronSchedule;
import io.github.rdt.schedule.JobExecution;
import io.github.rdt.schedule.JobRoute;
import io.github.rdt.schedule.ScheduleDefinitionChangePolicy;
import io.github.rdt.schedule.ScheduleMisfirePolicy;
import io.github.rdt.schedule.ScheduleOptions;
import io.github.rdt.schedule.SchedulePolicy;
import io.github.rdt.schedule.ScheduleSpec;
import io.github.rdt.schedule.ScheduledTaskTemplate;
import io.github.rdt.schedule.SchedulerSettings;
import io.github.rdt.schedule.TaskExecutionContext;
import io.github.rdt.schedule.TaskResult;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import io.github.rdt.support.RetryClassifier;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.redisson.api.RExecutorService;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.WorkerOptions;
import org.redisson.api.options.ExecutorOptions;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 持久化调度进度并使用看门狗锁保护，以有界并发的虚拟线程执行任务。 */
public final class RedissonScheduledTaskTemplate implements ScheduledTaskTemplate, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RedissonScheduledTaskTemplate.class);
    private static final String REDIS_TIME = """
            local t = redis.call('TIME')
            return t[1] * 1000 + math.floor(t[2] / 1000)
            """;
    private final RedissonClient client;
    private final KeySpace keys;
    private final JsonCodec json;
    private final JobExecutionRepository executions;
    private final ScheduleDueIndex due;
    private final DistributedLockTemplate locks;
    private final SchedulerSettings options;
    private final SchedulePolicy defaultPolicy;
    private final CronParser cronParser;
    private final String instanceId;
    private final RuntimeIdentity identity;
    private final RetryClassifier retryClassifier;
    private final RExecutorService jobWorkers;
    private final Map<String, ActiveAttempt> activeAttempts = new ConcurrentHashMap<>();
    private final AtomicBoolean nativeWorkersStarted = new AtomicBoolean();
    private volatile long directoryRefresh;
    private volatile List<RuntimeInstance> directory = List.of();
    private final long workLeaseMillis;
    private final Map<String, Registration> registrations = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon(true).name("rdt-schedule-timer").factory());
    private final ScheduledExecutorService leaseTimer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon(true).name("rdt-schedule-renewal").factory());
    // Each submitted invocation gets its own virtual thread; the semaphore below only applies backpressure.
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore capacity;
    private volatile RdtMetrics metrics = RdtMetrics.NONE;
    private final RuntimeDirectoryProvider routing;
    private final Map<String, Semaphore> jobSlots = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> jobActive = new ConcurrentHashMap<>();
    public void metrics(RdtMetrics value) {
        metrics = value;
        value.gauge("rdt.schedule.reservation", this, scheduler -> scheduler.reservations(), "node", instanceId);
    }
    private double reservations() {
        Long count = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_ONLY,
                "local n=0; local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000); for _,v in ipairs(redis.call('hvals',KEYS[1])) do if tonumber(v)>now then n=n+1 end end; return n",
                RScript.ReturnType.LONG, List.of(reservationKey(instanceId, identity.runId())));
        return count == null ? 0 : count;
    }
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong lastRedisFailureLogNanos = new AtomicLong();
    private final AtomicInteger dueCursor = new AtomicInteger();
    private static final long REDIS_FAILURE_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    public RedissonScheduledTaskTemplate(RedissonClient client, KeySpace keys, JsonCodec json,
                                        DistributedLockTemplate locks, SchedulerSettings options, CronParser cronParser,
                                        RuntimeIdentity identity, RetryClassifier retryClassifier) {
        this.identity = identity;
        this.retryClassifier = Objects.requireNonNull(retryClassifier, "retryClassifier");
        this.cronParser = cronParser;
        this.client = client;
        this.keys = keys;
        this.json = json;
        this.locks = locks;
        this.options = options;
        executions=new JobExecutionRepository(client,keys,json,options.resultTtl());
        due=new ScheduleDueIndex(client,keys,json);
        routing = new RedisRuntimeDirectoryProvider(client, keys, json);
        workLeaseMillis = options.workerLease().toMillis();
        this.instanceId = identity.instanceId();
        jobWorkers = nativeExecutor(identity.instanceId(), identity.runId());
        defaultPolicy = options.policy();
        capacity = new Semaphore(options.maxConcurrentTasks());
        long renewalInterval = Math.max(1, Math.min(workLeaseMillis / 3, options.pollInterval().toMillis()));
        leaseTimer.scheduleWithFixedDelay(this::renewAttempts, renewalInterval, renewalInterval, TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(this::tick, options.pollInterval().toMillis(),
                options.pollInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Subscription registerContextual(String taskId, String displayName, ScheduleSpec schedule,
                                 ScheduleOptions taskOptions, ContextualTask task) {
        RdtSupport.component(taskId);
        String name = displayName == null ? "" : displayName.strip();
        RdtSupport.require(name.length() <= 200, "Task display name must not exceed 200 characters");
        RdtSupport.require(!closed.get() && task != null && schedule != null && taskOptions != null,
                "Scheduler closed or registration invalid");
        CronSchedule cronExpression = parseCron(schedule);
        SchedulePolicy policy = defaultPolicy.resolve(taskOptions);
        Registration registration = new Registration(taskId, name, schedule, cronExpression, policy, task);
        registration.route = taskOptions.route() == null ? JobRoute.HEALTH_FIRST : taskOptions.route();
        registration.targetInstanceId = taskOptions.targetInstanceId() == null ? "" : taskOptions.targetInstanceId();
        RdtSupport.require(registration.route != JobRoute.SPECIFIED || !registration.targetInstanceId.isBlank(),
                "SPECIFIED route requires targetInstanceId for job " + taskId);
        RdtSupport.require(registrations.putIfAbsent(taskId, registration) == null, "Duplicate local task ID: " + taskId);
        try {
            locks.execute("schedule:" + taskId, LockOptions.defaults(), () -> {
                RMap<String, String> state = state(taskId);
                ensureScheduleState(registration, state, true);
                state.fastPut("route", registration.route.name());
                state.fastPut("targetInstanceId", registration.targetInstanceId);
                due.refreshDueIndex(taskId, state);
                // 展示名独立于调度指纹，未指定名称的注册不能覆盖已有名称。
                if (!name.isBlank()) {
                    RedisSupport.redisRun("save task display name", () -> state.fastPut("displayName", name));
                }
                return null;
            });
        } catch (Exception e) {
            registrations.remove(taskId, registration);
            if (e instanceof RdtException failure) {
                throw failure;
            }
            throw new RdtRedisException(RdtErrorCode.REDIS_OPERATION, "Schedule registration failed", e);
        }

        new ManagementIndex(client, keys).register("tasks", taskId);
        identity.register("job:" + taskId, () -> new RuntimeComponent("job", taskId, "", "", fingerprint(registration), 1, jobActive.computeIfAbsent(taskId, ignored -> new AtomicInteger()).get(), 0, 0));
        return () -> {
            registration.active = false;
            registrations.remove(taskId, registration);
            identity.unregister("job:" + taskId);
            Thread runner = registration.runner;
            if (runner != null) runner.interrupt();
        };
    }

    private void tick() {
        if (closed.get()) {
            return;
        }
        try {
            dispatchDue();
        } catch (RuntimeException e) {
            logDispatchFailure("scheduler", "scheduler", e);
        }
    }

    private void dispatchDue() {
        if (!identity.acceptingNewWork()) return;
        if (!registrations.isEmpty() && nativeWorkersStarted.compareAndSet(false, true)) {
            try {
                jobWorkers.registerWorkers(WorkerOptions.defaults().workers(options.maxConcurrentTasks())
                        .executorService(workers).tasksInjector(task -> {
                            if (!(task instanceof JobInvocation invocation)) {
                                throw new RdtConfigurationException("Untrusted RDT worker task type");
                            }
                            invocation.handler(this::executeInvocation);
                        }));
            } catch (RuntimeException failure) {
                nativeWorkersStarted.set(false);
                throw failure;
            }
        }
        long now = now();
        RScoredSortedSet<String> due = this.due.dueIndex();
        int count = due.count(0, true, now, true);
        if (count == 0) return;
        int offset = dueCursor.getAndUpdate(current -> count <= options.maxDuePerPoll()
                ? 0 : (current + options.maxDuePerPoll()) % count);
        List<String> ids = due.valueRange(0, true, now, true, offset % count, options.maxDuePerPoll()).stream().toList();
        for (String id : ids) {
            Registration registration = registrations.get(id);
            if (registration != null) withJobLock(id, () -> coordinate(registration, now));
        }
    }

    private void renewAttempts() {
        // Draining and graceful close stop dispatch, but in-flight business work must keep its lease.
        // A separate control timer prevents slow directory/transport calls from starving renewal.
        if (activeAttempts.isEmpty()) return;
        for (ActiveAttempt active : activeAttempts.values()) {
            if (active.fenced.get()) continue;
            if (!active.owner.getAsBoolean() || System.nanoTime() >= active.deadlineNanos) { active.fenced.set(true); continue; }
            JobInvocation invocation = active.invocation;
            AtomicBoolean renewed = new AtomicBoolean();
            try {
                withJobLock(invocation.jobId(), () -> {
                    RMap<String, String> state = state(invocation.jobId());
                    JobExecution execution = executions.execution(state);
                    long now = now();
                    if (active.owner.getAsBoolean() && owns(execution, invocation)
                            && "RUNNING".equals(execution.status()) && execution.retryAt() > now
                            && reserve(instanceId,identity.runId(),invocation.dispatchToken(),now,true,options.maxConcurrentTasks())) {
                        renewed.set(executions.transition(state,execution,execution.transition("RUNNING",instanceId,identity.runId(),
                                invocation.dispatchToken(),now,now+workLeaseMillis,execution.attempt(),execution.detail()),null,true));
                    }
                });
                if (renewed.get()) { active.renewalFailures.set(0); active.deadlineNanos=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(workLeaseMillis); }
                else if (active.renewalFailures.incrementAndGet()>=3) active.fenced.set(true);
            } catch (RuntimeException failure) {
                if (active.renewalFailures.incrementAndGet()>=3) active.fenced.set(true);
                logDispatchFailure(invocation.jobId(),invocation.jobId(),failure);
            }
        }
    }

    private RExecutorService nativeExecutor(String worker, String run) {
        return client.getExecutorService(ExecutorOptions.name(keys.key("job-worker", worker + ":" + run))
                .codec(JobCodec.create())
                .taskRetryInterval(Duration.ZERO));
    }

    private boolean withJobLock(String jobId, Runnable action) {
        var lock = locks.tryLock("schedule:" + jobId,
                new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO));
        if (lock.isEmpty()) return false;
        try (LockHandle ignored = lock.get()) { action.run(); return true; }
    }

    private boolean owns(JobExecution execution, JobInvocation invocation) {
        return execution != null && instanceId.equals(execution.instanceId()) && invocation.executionId().equals(execution.executionId())
                && invocation.dispatchToken().equals(execution.dispatchToken())
                && identity.runId().equals(execution.runId());
    }

    private void coordinate(Registration registration, long now) {
        if (!identity.acceptingNewWork()) return;
        RMap<String, String> state = state(registration.id);
        ScheduleState definition = ensureScheduleState(registration, state, false);
        if (!definition.active()) return;
        JobExecution execution = executions.execution(state);
        if (execution == null || execution.terminal()) {
            if ("true".equals(state.get("paused")) || definition.next() > now) { due.refreshDueIndex(registration.id, state); return; }
            if (registration.policy.misfirePolicy() == ScheduleMisfirePolicy.SKIP
                    && now - definition.next() > registration.policy.misfireThreshold().toMillis()) {
                state.fastPut("next", Long.toString(next(registration.schedule, registration.cronExpression, definition.next(), now)));
                due.refreshDueIndex(registration.id, state);
                return;
            }
            long scheduledAt = definition.next();
            execution = new JobExecution("scheduled-" + scheduledAt, registration.id, scheduledAt, 0,
                    "READY", "", "", "", now, now, "", 0);
            // The execution and watermark share one Redis command; a crash cannot consume a trigger alone.
            state.putAll(Map.of("execution", json.write(execution), "next", Long.toString(
                    registration.schedule instanceof ScheduleSpec.FixedDelay ? Long.MAX_VALUE
                            : next(registration.schedule, registration.cronExpression, scheduledAt, now))));
            executions.archive(execution);
        }
        if (("DISPATCHED".equals(execution.status()) || "RUNNING".equals(execution.status()))
                && execution.retryAt() <= now) {
            JobExecution recovered = execution.transition("READY", "", "", "", now, now, execution.attempt(),
                    "LEASE_EXPIRED: worker lease expired; awaiting recovery");
            if (!executions.transition(state, execution, recovered, null, false)) return;
            releaseReservation(execution.instanceId(), execution.runId(), execution.dispatchToken());
            execution = recovered;
        }
        if (("READY".equals(execution.status()) || "WAITING".equals(execution.status())) && execution.retryAt() <= now) {
            dispatch(registration, state, execution, now);
        }
        due.refreshDueIndex(registration.id, state);
    }

    private List<RuntimeInstance> candidates(Registration registration, long now) {
        if (now >= directoryRefresh) {
            directory = routing.instances(now);
            directoryRefresh = now + Math.min(5000, workLeaseMillis / 3);
        }
        List<RuntimeInstance> eligible = new ArrayList<>(directory.stream()
                .filter(node -> !node.draining() && !node.pressure().paused() && node.components() != null && node.components().stream()
                        .anyMatch(component -> component.kind().equals("job") && component.id().equals(registration.id)
                                && component.definition().equals(fingerprint(registration))))
                .filter(node -> registration.route != JobRoute.SPECIFIED || node.instanceId().equals(registration.targetInstanceId))
                .toList());
        // Detached programmatic templates have no Spring registry, but still use the same native worker protocol.
        if (eligible.stream().noneMatch(node -> node.runId().equals(identity.runId())) && identity.acceptingNewWork()
                && (registration.route != JobRoute.SPECIFIED || instanceId.equals(registration.targetInstanceId))) {
            eligible.add(new RuntimeInstance(instanceId, identity.runId(), "application", 0, now, workLeaseMillis, 0, options.maxConcurrentTasks(), identity.generation(), false, identity.components(), identity.pressure()));
        }
        if (registration.route == JobRoute.RANDOM) Collections.shuffle(eligible);
        else eligible.sort(Comparator.comparingDouble(node -> node.components().stream()
                .filter(component -> component.kind().equals("job")).mapToInt(RuntimeComponent::active).sum()
                        / (double)Math.max(1,node.scheduleConcurrency())));
        return eligible;
    }

    private void dispatch(Registration registration, RMap<String, String> state, JobExecution execution, long now) {
        for (RuntimeInstance node : candidates(registration, now)) {
            String token = UUID.randomUUID().toString();
            if (!reserve(node.instanceId(), node.runId(), token, now, false, node.scheduleConcurrency())) continue;
            JobExecution assigned = execution.transition("DISPATCHED", node.instanceId(), node.runId(), token,
                    now, now + workLeaseMillis, execution.attempt(), "");
            if (!executions.transition(state, execution, assigned, null, false)) {
                metrics.increment("rdt.schedule.dispatch", "result", "transition_lost");
                releaseReservation(node.instanceId(), node.runId(), token);
                return;
            }
            try {
                // A local worker avoids a network round trip and never runs on a Redisson callback thread.
                JobInvocation invocation = new JobInvocation(registration.id, execution.executionId(), token);
                if (node.runId().equals(identity.runId())) {
                    workers.submit(() -> executeInvocation(invocation));
                } else {
                    // Do not create a native result waiter per execution. RDT persists acceptance/results itself.
                    nativeExecutor(node.instanceId(), node.runId()).execute(invocation);
                }
                metrics.increment("rdt.schedule.dispatch", "result", "ok");
                return;
            } catch (RuntimeException failure) {
                metrics.increment("rdt.schedule.dispatch", "result", "submit_failed");
                try {
                    long rejectedAt = now();
                    if (executions.transition(state, assigned, execution.rejected("READY", rejectedAt,
                            retryAdmission(registration, execution, rejectedAt), "SUBMIT_FAILED: old dispatch token fenced"), null, true)) {
                        metrics.increment("rdt.schedule.dispatch.rollback", "reason", "submit_failed");
                        due.refreshDueIndex(registration.id, state);
                    } else {
                        metrics.increment("rdt.schedule.dispatch", "result", "transition_lost");
                    }
                } finally {
                    releaseReservation(node.instanceId(), node.runId(), token);
                }
                throw failure;
            }
        }
        metrics.increment("rdt.schedule.dispatch", "result", "no_worker");
        if (now>=registration.nextAdmissionWarning) {
            log.warn("No available worker job={}; applying admission backoff",registration.id);
            registration.nextAdmissionWarning=now+30000;
        }
        executions.transition(state, execution, execution.rejected("WAITING", now, retryAdmission(registration, execution, now), "NO_WORKER: no healthy available worker"), null, false);
    }

    private String reservationKey(String worker, String run) { return keys.key("job-reservations", worker + ":" + run); }

    private boolean reserve(String worker, String run, String token, long now, boolean renewal, int limit) {
        Long accepted = client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE, """
                local entries = redis.call('hgetall', KEYS[1])
                for i=1,#entries,2 do
                  if tonumber(entries[i+1]) <= tonumber(ARGV[1]) then redis.call('hdel', KEYS[1], entries[i]) end
                end
                if ARGV[5] == 'true' and redis.call('hexists', KEYS[1], ARGV[3]) == 0 then return 0 end
                if redis.call('hexists', KEYS[1], ARGV[3]) == 1 or redis.call('hlen', KEYS[1]) < tonumber(ARGV[4]) then
                  redis.call('hset', KEYS[1], ARGV[3], ARGV[2])
                  redis.call('pexpire', KEYS[1], math.max(1000,(tonumber(ARGV[2])-tonumber(ARGV[1]))*2))
                  return 1
                end
                return 0
                """, RScript.ReturnType.LONG, List.of(reservationKey(worker, run)),
                now, now + workLeaseMillis, token, limit, Boolean.toString(renewal));
        return Long.valueOf(1).equals(accepted);
    }

    private void releaseReservation(String worker, String run, String token) {
        if (worker != null && !worker.isBlank()) client.getMap(reservationKey(worker, run), StringCodec.INSTANCE).fastRemove(token);
    }

    private long retryAdmission(Registration registration, JobExecution execution, long now) {
        int rejected = execution.dispatchRejections()+1;
        long delay = Math.min(60000, 250L << Math.min(rejected, 8));
        if (rejected >= 3 && registration != null) {
            delay = Math.max(delay, next(registration.schedule, registration.cronExpression, now, now) - now);
        }
        return now + delay + ThreadLocalRandom.current().nextLong(Math.max(1, delay / 4));
    }

    private void reject(JobInvocation invocation, String reason) {
        // Full execution CAS fences delayed invocations without waiting for the dispatcher's job lock.
        // Count only the accepted rollback: stale duplicate invocations must not inflate rejections.
        try {
            var state = state(invocation.jobId());
            var execution = executions.execution(state);
            if (!owns(execution, invocation) || !"DISPATCHED".equals(execution.status())) return;
            long now = now();
            var ready = execution.rejected("READY", now,
                    retryAdmission(registrations.get(invocation.jobId()), execution, now), "REJECTED:" + reason);
            if (executions.transition(state, execution, ready, null, true)) {
                metrics.increment("rdt.schedule.dispatch.rollback", "reason", reason);
                if ("capacity".equals(reason)) {
                    metrics.increment("rdt.schedule.capacity.rejected", "job", invocation.jobId());
                }
                due.refreshDueIndex(invocation.jobId(), state);
            } else {
                metrics.increment("rdt.schedule.dispatch", "result", "transition_lost");
            }
        } finally {
            releaseReservation(instanceId, identity.runId(), invocation.dispatchToken());
        }
    }

    private void leaseLost(JobInvocation invocation, long started) {
        metrics.increment("rdt.schedule.lease.lost", "job", invocation.jobId());
        long completed = now();
        executions.save(invocation.jobId(), invocation.executionId() + ":lease-lost:" + invocation.dispatchToken(), options.resultTtl(),
                new TaskResult(invocation.jobId(), Instant.ofEpochMilli(started).toString(), Instant.ofEpochMilli(started).toString(),
                        Instant.ofEpochMilli(completed).toString(), instanceId, false, "LEASE_LOST: stale completion discarded", identity.runId(),
                        invocation.executionId(), 0, "LEASE_LOST"));
        log.warn("Stale task completion discarded job={} token={}", invocation.jobId(), invocation.dispatchToken());
    }

    private void executeInvocation(JobInvocation invocation) {
        Registration registration = registrations.get(invocation.jobId());
        if (registration == null || !registration.active) { reject(invocation, "unregistered"); return; }
        if (!identity.acceptingNewWork()) { reject(invocation, "draining"); return; }
        Semaphore jobSlot = jobSlots.computeIfAbsent(invocation.jobId(), ignored -> new Semaphore(1, true));
        if (!jobSlot.tryAcquire()) {
            reject(invocation, "capacity"); return;
        }
        if (!capacity.tryAcquire()) {
            jobSlot.release();
            reject(invocation, "capacity"); return;
        }
        BooleanSupplier owner = identity.guard();
        AtomicBoolean accepted = new AtomicBoolean();
        try {
            // Waiting here is bounded by short control transitions, never by the business invocation.
            locks.execute("schedule:" + invocation.jobId(), LockOptions.defaults(), () -> {
                RMap<String, String> state = state(invocation.jobId());
                JobExecution execution = executions.execution(state);
                if (owns(execution, invocation) && "DISPATCHED".equals(execution.status())
                        && owner.getAsBoolean() && execution.retryAt() > now()) {
                    if (executions.transition(state, execution, execution.transition("RUNNING", instanceId, identity.runId(), invocation.dispatchToken(),
                            now(), now() + workLeaseMillis, execution.attempt() + 1, ""), null, true)) {
                        activeAttempts.put(invocation.dispatchToken(), new ActiveAttempt(invocation, owner, workLeaseMillis));
                        jobActive.computeIfAbsent(invocation.jobId(), ignored -> new AtomicInteger()).incrementAndGet();
                        accepted.set(true);
                    }
                }
                return null;
            });
            if (!accepted.get()) return;
            long started = now();
            Object value = null;
            Exception failure = null;
            registration.runner = Thread.currentThread();
            ActiveAttempt active = activeAttempts.get(invocation.dispatchToken());
            var context = new TaskExecutionContext(invocation.jobId(), invocation.executionId(), invocation.dispatchToken(),
                    () -> !active.fenced.get() && active.owner.getAsBoolean() && System.nanoTime() < active.deadlineNanos);
            try { context.assertLease(); value = registration.task.execute(context); }
            catch (Exception error) { failure = error; if (error instanceof InterruptedException) Thread.currentThread().interrupt(); }
            final Exception businessFailure = failure;
            final Object result = value;
            if (!context.lease().getAsBoolean() || Thread.currentThread().isInterrupted()) { leaseLost(invocation, started); return; }
            locks.execute("schedule:" + invocation.jobId(), LockOptions.defaults(), () -> {
                RMap<String, String> state = state(invocation.jobId());
                JobExecution execution = executions.execution(state);
                long completed = now();
                if (!owns(execution, invocation) || !owner.getAsBoolean() || execution.retryAt() <= completed) {
                    leaseLost(invocation, started); return null;
                }
                String detail = businessFailure == null ? json.write(result)
                        : businessFailure.getClass().getSimpleName() + ": " + businessFailure.getMessage();
                detail = detail.substring(0, Math.min(detail.length(), 4000));
                boolean retry = businessFailure != null && retryClassifier.isRetryable(businessFailure)
                        && registration.policy.canRetry(execution.attempt());
                long retryAt = retry ? completed + registration.policy.retry().delayAfter(execution.attempt()).toMillis() : 0;
                JobExecution finished = execution.transition(retry ? "READY" : businessFailure == null ? "SUCCEEDED" : "FAILED",
                        instanceId, identity.runId(), invocation.dispatchToken(), completed, retryAt,
                        execution.attempt(), detail);
                long next = Long.parseLong(state.get("next"));
                if (!retry && !execution.executionId().startsWith("manual-")) {
                    next = next(registration.schedule, registration.cronExpression, execution.scheduledAt(), completed);
                }
                if (!executions.transition(state, execution, finished, next, true)) {
                    leaseLost(invocation, started);
                    return null;
                }
                executions.save(registration.id, execution.executionId() + ":" + execution.attempt(), registration.policy.resultTtl(),
                        new TaskResult(registration.id, Instant.ofEpochMilli(execution.scheduledAt()).toString(),
                                Instant.ofEpochMilli(started).toString(), Instant.ofEpochMilli(completed).toString(),
                                instanceId, businessFailure == null, detail, identity.runId(), execution.executionId(),
                                execution.attempt(), businessFailure == null ? "SUCCEEDED" : "FAILED"));
                due.refreshDueIndex(registration.id, state);
                return null;
            });
        } catch (Exception error) {
            logDispatchFailure(registration, error);
        } finally {
            registration.runner = null;
            if (activeAttempts.remove(invocation.dispatchToken()) != null) jobActive.get(invocation.jobId()).decrementAndGet();
            capacity.release();
            jobSlot.release();
            releaseReservation(instanceId, identity.runId(), invocation.dispatchToken());
        }
    }

    /**
     * Redis 故障不是任务业务失败，不能伪装成任务执行失败记录。
     * 同时对基础设施异常限频，避免 Redis 故障期间每个轮询周期刷屏。
     */
    private void logDispatchFailure(Registration registration, Exception error) {
        logDispatchFailure(registration.id, registration.displayName, error);
    }

    private void logDispatchFailure(String taskId, String displayName, Exception error) {
        if (error instanceof RdtException failure
                && (failure.code() == RdtErrorCode.REDIS_OPERATION
                || failure.code() == RdtErrorCode.REDIS_TIMEOUT)) {
            long now = System.nanoTime();
            long last = lastRedisFailureLogNanos.get();
            boolean firstLog = last == 0 && lastRedisFailureLogNanos.compareAndSet(0, now);
            boolean periodicLog = last != 0
                    && now - last >= REDIS_FAILURE_LOG_INTERVAL_NANOS
                    && lastRedisFailureLogNanos.compareAndSet(last, now);
            if (firstLog || periodicLog) {
                log.warn("Task dispatch paused by Redis failure id={} name={}",
                        taskId, displayName, error);
            }
            return;
        }
        log.error("Task dispatch failed id={} name={}", taskId, displayName, error);
    }

    private long initial(ScheduleSpec schedule, CronSchedule cronExpression, long now) {
        return switch (schedule) {
            case ScheduleSpec.FixedRate(Duration period, Duration delay) -> Math.addExact(now, delay.toMillis());
            case ScheduleSpec.FixedDelay(Duration delay, Duration initial) -> Math.addExact(now, initial.toMillis());
            case ScheduleSpec.Cron cron -> cronNext(cronExpression, cron.zone(), now);
        };
    }

    private long next(ScheduleSpec schedule, CronSchedule cronExpression, long scheduled, long now) {
        return switch (schedule) {
            case ScheduleSpec.FixedRate(Duration period, Duration initial) ->
                    Math.addExact(scheduled, Math.multiplyExact((now - scheduled) / period.toMillis() + 1, period.toMillis()));
            case ScheduleSpec.FixedDelay(Duration delay, Duration initial) -> Math.addExact(now, delay.toMillis());
            case ScheduleSpec.Cron cron -> cronNext(cronExpression, cron.zone(), now);
        };
    }

    /**
     * 使用注册阶段已经解析好的表达式计算下一次触发时间，避免每次任务触发重复解析 cron。
     *
     * @param cronExpression 已在注册阶段解析的表达式
     * @param zone 任务时区
     * @param now 当前 Redis 时间
     * @return 下一次触发时间；没有下一次触发时返回最大时间戳
     */
    private long cronNext(CronSchedule cronExpression, ZoneId zone, long now) {
        return cronExpression.next(now);
    }

    private CronSchedule parseCron(ScheduleSpec schedule) {
        if (!(schedule instanceof ScheduleSpec.Cron cron)) {
            return null;
        }
        try {
            return cronParser.parse(cron.expression(), cron.zone());
        } catch (IllegalArgumentException e) {
            throw new RdtConfigurationException("Invalid cron expression: " + cron.expression(), e);
        }
    }

    private long now() {
        return RedisSupport.redis("read Redis clock", () -> client.getScript(StringCodec.INSTANCE)
                .eval(RScript.Mode.READ_ONLY, REDIS_TIME, RScript.ReturnType.LONG, List.of()));
    }

    /**
     * 确保任务在 Redis 中拥有完整且可解析的持久化状态。
     *
     * <p>任务定义和下一次触发时间属于同一个状态契约。人工清理可能只删除
     * 其中一个字段，因此注册阶段不能只通过 {@code definition} 判断任务是否完整。</p>
     *
     * <p>定义冲突默认拒绝覆盖；配置为 {@code UPDATE} 时，使用新定义重置下一次触发时间。
     * 缺失定义时视为全新任务；定义一致但 {@code next} 缺失或非法时只修复水位，
     * 不影响展示名称和结果记录。</p>
     *
     * @param registration 本地任务注册信息
     * @param state Redis 中的任务状态
     * @return 当前有效的调度状态
     */
    private ScheduleState ensureScheduleState(Registration registration, RMap<String, String> state,
                                              boolean registering) {
        String fingerprint = fingerprint(registration);
        // 只有会改变触发行为的策略参与集群一致性检查；展示名、结果 TTL 和注册时的更新许可不参与。
        String policy = registration.policy.failureMode() + ":" + registration.policy.retry()
                + ":" + registration.policy.misfirePolicy() + ":" + registration.policy.misfireThreshold();
        String policyFingerprint = policy + ":" + registration.route + ":" + registration.targetInstanceId;
        String existing = RedisSupport.redis("read schedule definition", () -> state.get("definition"));
        String existingPolicy = RedisSupport.redis("read schedule policy", () -> state.get("policy"));
        boolean definitionChanged = existing != null && !existing.equals(fingerprint);
        boolean policyChanged = existingPolicy != null && !existingPolicy.equals(policyFingerprint);
        if (definitionChanged || policyChanged) {
            if (!registering) {
                // UPDATE 只发生在注册阶段，旧实例轮询时不能把新定义改回去。
                return new ScheduleState(Long.MAX_VALUE, false);
            }
            if (registration.policy.definitionChangePolicy() == ScheduleDefinitionChangePolicy.FAIL) {
                RdtSupport.require(false,
                        "Conflicting schedule definition; use rdt.schedule.definition-change-policy=UPDATE "
                                + "to refresh task ID: " + registration.id);
            }
            JobExecution active = executions.execution(state);
            RdtSupport.require(active == null || active.terminal(), "Cannot update an active job definition: " + registration.id);
            long now = now();
            long initial = initial(registration.schedule, registration.cronExpression, now);
            RedisSupport.redisRun("update schedule definition", () -> state.putAll(Map.of(
                    "definition", fingerprint,
                    "policy", policyFingerprint,
                    "next", Long.toString(initial))));
            log.info("Task schedule updated id={} name={} oldDefinition={} newDefinition={}",
                    registration.id, registration.displayName, existing, fingerprint);
            return new ScheduleState(initial, true);
        }
        String rawNext = RedisSupport.redis("read schedule watermark", () -> state.get("next"));
        Long next = parseNext(rawNext);
        if (existing == null) {
            long now = now();
            long initial = initial(registration.schedule, registration.cronExpression, now);
            RedisSupport.redisRun("initialize schedule", () -> state.putAll(Map.of(
                    "definition", fingerprint,
                    "policy", policyFingerprint,
                    "next", Long.toString(initial))));
            return new ScheduleState(initial, true);
        }
        if (next == null) {
            long now = now();
            long initial = initial(registration.schedule, registration.cronExpression, now);
            RedisSupport.redisRun("repair schedule watermark", () ->
                    state.fastPut("next", Long.toString(initial)));
            return new ScheduleState(initial, true);
        }
        if (existingPolicy == null) {
            RedisSupport.redisRun("complete schedule policy", () -> state.fastPut("policy", policyFingerprint));
        }
        return new ScheduleState(next, true);
    }

    private String fingerprint(Registration registration) {
        return registration.schedule.toString();
    }

    /** 无效调度水位在注册时修复，避免在调度线程直接抛出解析异常。 */
    private Long parseNext(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private RMap<String, String> state(String taskId) {
        return client.getMap(keys.key("schedule-state", taskId), StringCodec.INSTANCE);
    }

    @Override
    public List<TaskResult> results(String taskId,int offset,int limit) { return executions.results(taskId,offset,limit); }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        timer.shutdownNow();
        registrations.keySet().forEach(id -> identity.unregister("job:" + id));
        registrations.clear();
        if (nativeWorkersStarted.get()) jobWorkers.deregisterWorkers();
        stop(workers);
        leaseTimer.shutdownNow();
    }

    private void stop(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(options.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static final class ActiveAttempt {
        private final JobInvocation invocation;
        private final BooleanSupplier owner;
        private final AtomicInteger renewalFailures = new AtomicInteger();
        private final AtomicBoolean fenced = new AtomicBoolean();
        private volatile long deadlineNanos;
        private ActiveAttempt(JobInvocation invocation, BooleanSupplier owner, long leaseMillis) {
            this.invocation=invocation; this.owner=owner; deadlineNanos=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(leaseMillis);
        }
    }

    private record ScheduleState(long next, boolean active) {
    }

    private static final class Registration {
        private volatile boolean active = true;
        private volatile Thread runner;
        private volatile long nextAdmissionWarning;
        private final String id;
        private final String displayName;
        private final ScheduleSpec schedule;
        private final CronSchedule cronExpression;
        private final SchedulePolicy policy;
        private final ContextualTask task;
        private JobRoute route = JobRoute.HEALTH_FIRST;
        private String targetInstanceId = "";

        private Registration(String id, String displayName, ScheduleSpec schedule,
                             CronSchedule cronExpression, SchedulePolicy policy, ContextualTask task) {
            this.id = id;
            this.displayName = displayName.isBlank() ? id : displayName;
            this.schedule = schedule;
            this.cronExpression = cronExpression;
            this.policy = policy;
            this.task = task;
        }
    }
}
