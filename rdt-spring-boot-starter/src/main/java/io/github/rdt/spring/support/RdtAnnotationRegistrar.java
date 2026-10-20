package io.github.rdt.spring.support;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.cache.RdtCacheEvict;
import io.github.rdt.cache.RdtCachePut;
import io.github.rdt.cache.RdtCacheable;
import io.github.rdt.consistency.ConsumerGuard;
import io.github.rdt.consistency.InboxMode;
import io.github.rdt.consistency.InboxScope;
import io.github.rdt.id.IdMode;
import io.github.rdt.id.RdtId;
import io.github.rdt.lock.RdtLock;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.MessageDescriptor;
import io.github.rdt.queue.ConsumerDescriptor;
import io.github.rdt.schedule.TaskDescriptor;
import org.springframework.beans.BeanUtils;
import org.springframework.util.ClassUtils;
import io.github.rdt.queue.MessageQueueTemplate;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.queue.QueueOptions;
import io.github.rdt.queue.QueueSubscription;
import io.github.rdt.queue.RdtMessageConsumer;
import io.github.rdt.queue.Subscription;
import io.github.rdt.ratelimit.RdtRateLimit;
import io.github.rdt.schedule.RdtScheduledTask;
import io.github.rdt.schedule.ScheduleDefinitionChangePolicy;
import io.github.rdt.schedule.ScheduleFailureMode;
import io.github.rdt.schedule.ScheduleMisfirePolicy;
import io.github.rdt.schedule.ScheduleOptions;
import io.github.rdt.schedule.ScheduleSpec;
import io.github.rdt.schedule.ScheduledTaskTemplate;
import io.github.rdt.spring.annotation.RdtQueueListener;
import io.github.rdt.spring.annotation.RdtScheduled;
import io.github.rdt.spring.config.RdtComponentProperties;
import io.github.rdt.spring.config.RdtProperties;
import io.github.rdt.support.JsonCodec;
import io.github.rdt.support.RdtSupport;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.support.CronExpression;

/** 单例初始化完成后注册消费者与调度任务，并通过 Spring 代理调用固定接口方法。 */
public final class RdtAnnotationRegistrar implements SmartLifecycle {
    private final ConfigurableListableBeanFactory beans;
    private final ObjectProvider<MessageQueueTemplate> queues;
    private final ObjectProvider<ScheduledTaskTemplate> scheduler;
    private final RdtMethodSupport methods;
    private final JsonCodec json;
    private final RdtProperties properties;
    private final RdtComponentProperties components;
    private static <T> T override(T external, T declared) { return external == null ? declared : external; }
    private final List<Subscription> registrations = new ArrayList<>();
    private volatile boolean running;
    private Supplier<BooleanSupplier> executionGuard = () -> () -> true;
    private ExecutionGeneration generation = new ExecutionGeneration();

    public synchronized void setExecutionGuard(Supplier<BooleanSupplier> executionGuard) {
        this.executionGuard = executionGuard;
    }


    public RdtAnnotationRegistrar(ConfigurableListableBeanFactory beans, ObjectProvider<MessageQueueTemplate> queues,
                                  ObjectProvider<ScheduledTaskTemplate> scheduler, RdtMethodSupport methods,
                                  JsonCodec json, RdtProperties properties, RdtComponentProperties components) {
        this.beans = beans;
        this.queues = queues;
        this.scheduler = scheduler;
        this.methods = methods;
        this.json = json;
        this.properties = properties;
        this.components = components;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        generation = new ExecutionGeneration();
        try {
            List<MethodCandidate> methodCandidates = new ArrayList<>();
            List<ScheduledCandidate> scheduledCandidates = new ArrayList<>();
            List<QueueCandidate> queueCandidates = new ArrayList<>();
            for (String name : beans.getBeanDefinitionNames()) {
                Class<?> type = beans.getType(name, false);
                if (type == null || !beans.isSingleton(name)) {
                    continue;
                }
                Map<Method, Boolean> selected = MethodIntrospector.selectMethods(type,
                        (MethodIntrospector.MetadataLookup<Boolean>) method ->
                                annotatedMethod(method) ? Boolean.TRUE : null);
                boolean scheduledType = annotation(type, RdtScheduled.class) != null;
                boolean queueType = annotation(type, RdtQueueListener.class) != null;
                if (selected.isEmpty() && !scheduledType && !queueType) {
                    continue;
                }
                Object bean = beans.getBean(name);
                Class<?> actual = AopProxyUtils.ultimateTargetClass(bean);
                RdtSupport.require(!Modifier.isFinal(actual.getModifiers()),
                        "RDT bean classes must not be final: " + actual);
                Map<Method, Boolean> actualMethods = MethodIntrospector.selectMethods(actual,
                        (MethodIntrospector.MetadataLookup<Boolean>) method ->
                                annotatedMethod(method) ? Boolean.TRUE : null);
                for (Method method : actualMethods.keySet()) {
                    methods.validate(method);
                    RdtSupport.require(!AnnotatedElementUtils.hasAnnotation(method, Async.class)
                                    && !AnnotatedElementUtils.hasAnnotation(actual, Async.class),
                            "@Async cannot be combined with synchronous RDT semantics: " + method);
                    int caching = (annotation(method, RdtCacheable.class) != null ? 1 : 0)
                            + (annotation(method, RdtCachePut.class) != null ? 1 : 0)
                            + (annotation(method, RdtCacheEvict.class) != null ? 1 : 0);
                    RdtSupport.require(caching <= 1, "Use one RDT cache annotation per method: " + method);
                    RdtSupport.require((annotation(method, RdtCacheable.class) == null
                            && annotation(method, RdtCachePut.class) == null) || method.getReturnType() != void.class,
                            "Cacheable/CachePut cannot target void methods");
                    methodCandidates.add(new MethodCandidate(bean, method));
                }
                RdtScheduled scheduled = annotation(actual, RdtScheduled.class);
                RdtQueueListener queue = annotation(actual, RdtQueueListener.class);
                RdtSupport.require(scheduled == null || queue == null,
                        "A bean cannot be both an RDT scheduled task and message consumer: " + actual.getName());
                if (scheduled != null) {
                    scheduledCandidates.add(scheduledCandidate(bean, actual, scheduled));
                }
                if (queue != null) {
                    queueCandidates.add(queueCandidate(bean, actual, queue));
                }
            }
            // 启动可能产生业务副作用的工作线程前，先完成全部契约和配置校验。
            methodCandidates.forEach(this::validateMethodRegistration);
            scheduledCandidates.forEach(this::validateScheduledRegistration);
            queueCandidates.forEach(this::validateQueueRegistration);
            scheduledCandidates.forEach(this::register);
            queueCandidates.forEach(this::register);
            running = true;
        } catch (RuntimeException exception) {
            try {
                stop();
            } catch (RuntimeException cleanupFailure) {
                if (cleanupFailure != exception) exception.addSuppressed(cleanupFailure);
            }
            throw exception;
        }
    }

    private void validateMethodRegistration(MethodCandidate candidate) {
        Method method = candidate.method();
        RdtId id = annotation(method, RdtId.class);
        if (id != null) {
            RdtSupport.require(method.getReturnType() == long.class || method.getReturnType() == Long.class,
                    "@RdtId methods must return long or Long: " + method);
            RdtSupport.require(id.minimum().isBlank() || id.mode() == IdMode.ATOMIC,
                    "@RdtId minimum requires ATOMIC mode: " + method);
            RdtSupport.require(annotation(method, RdtCacheable.class) == null
                            && annotation(method, RdtCachePut.class) == null,
                    "@RdtId cannot be combined with cached return values: " + method);
        }

    }

    private ScheduledCandidate scheduledCandidate(Object bean, Class<?> actual, RdtScheduled scheduled) {
        RdtSupport.require(RdtScheduledTask.class.isAssignableFrom(actual),
                "@RdtScheduled beans must implement RdtScheduledTask: " + actual.getName());
        Method method = implementationMethod(actual, RdtScheduledTask.class, "execute");
        TaskDescriptor descriptor = ((RdtScheduledTask) bean).descriptor();
        RdtSupport.require(descriptor != null, "Scheduled task descriptor is required: " + actual.getName());
        return new ScheduledCandidate(bean, actual, method, scheduled, descriptor);
    }

    private QueueCandidate queueCandidate(Object bean, Class<?> actual, RdtQueueListener queue) {
        RdtSupport.require(RdtMessageConsumer.class.isAssignableFrom(actual),
                "@RdtQueueListener beans must implement RdtMessageConsumer<T>: " + actual.getName());
        ResolvableType consumerType = ResolvableType.forClass(actual).as(RdtMessageConsumer.class);
        ResolvableType payloadType = consumerType.getGeneric(0);
        RdtSupport.require(payloadType != ResolvableType.NONE && payloadType.resolve() != null,
                "RdtMessageConsumer payload type must be concrete: " + actual.getName());
        Method method = implementationMethod(actual, RdtMessageConsumer.class, "consume", MessageDescriptor.class);
        Class<?> payloadClass = payloadType.resolve();
        RdtSupport.require(MessageDescriptor.class.isAssignableFrom(payloadClass),
                "RdtMessageConsumer<T> requires a concrete MessageDescriptor subtype: " + actual.getName());
        var constructor = ClassUtils.getConstructorIfAvailable(payloadClass);
        RdtSupport.require(constructor != null && !Modifier.isAbstract(payloadClass.getModifiers()),
                "MessageDescriptor must be concrete with a public no-argument constructor: " + payloadClass.getName());
        MessageDescriptor<?> descriptor = (MessageDescriptor<?>) BeanUtils.instantiateClass(constructor);
        RdtSupport.require(resolve(queue.topic()).equals(descriptor.topic()),
                "Listener topic must match its message descriptor: " + actual.getName());
        ConsumerDescriptor consumer = descriptor.consumer(resolve(queue.group()));
        RdtSupport.require(consumer != null, "Consumer descriptor is required: " + actual.getName());
        return new QueueCandidate(bean, actual, method, queue, payloadType.getType(), descriptor, consumer);
    }

    private Method implementationMethod(Class<?> actual, Class<?> contract, String name, Class<?>... parameters) {
        try {
            Method interfaceMethod = contract.getMethod(name, parameters);
            return BridgeMethodResolver.findBridgedMethod(AopUtils.getMostSpecificMethod(interfaceMethod, actual));
        } catch (NoSuchMethodException e) {
            throw new RdtConfigurationException("Invalid RDT contract method: " + contract.getName() + "#" + name, e);
        }
    }

    private void validateScheduledRegistration(ScheduledCandidate candidate) {
        methods.validate(candidate.method());
        rejectAsync(candidate.actual(), candidate.method());
        schedule(candidate.descriptor());
        properties.schedule().policy().resolve(scheduleOptions(candidate));
        RdtSupport.component(resolve(candidate.annotation().id()));
        RdtSupport.require(resolve(candidate.descriptor().displayName()).strip().length() <= 200,
                "Task display name must not exceed 200 characters");
    }

    private void validateQueueRegistration(QueueCandidate candidate) {
        methods.validate(candidate.method());
        rejectAsync(candidate.actual(), candidate.method());
        QueueSubscription spec = queueSubscription(candidate);
        properties.queue().policy().resolve(spec.options());
        ConsumerGuard guard = beans.getBeanProvider(ConsumerGuard.class).getIfAvailable();
        boolean inbox = candidate.consumer().inbox() == InboxMode.ENABLED
                || candidate.consumer().inbox() == InboxMode.INHERIT && guard != null && guard.enabledByDefault();
        if (inbox && (guard == null || !guard.supportsInbox())) {
            throw new RdtConfigurationException("inbox=ENABLED requires rdt-consistency-jdbc-starter, rdt.consistency.enabled=true and an Inbox-capable ConsumerGuard: " + candidate.actual().getName());
        }
        if (inbox && spec.mode() == QueueMode.BROADCAST) {
            throw new RdtConfigurationException("Inbox requires a durable queue mode, not BROADCAST");
        }
        if (inbox && spec.mode() == QueueMode.RELIABLE_BROADCAST) {
            RdtSupport.require(properties.instanceId() != null && !properties.instanceId().isBlank(),
                    "Reliable broadcast Inbox requires a stable rdt.instance-id");
        }
    }

    private void rejectAsync(Class<?> actual, Method method) {
        RdtSupport.require(!AnnotatedElementUtils.hasAnnotation(method, Async.class)
                        && !AnnotatedElementUtils.hasAnnotation(actual, Async.class),
                "@Async cannot be combined with synchronous RDT semantics: " + method);
    }

    private void register(QueueCandidate candidate) {
        RdtQueueListener queue = candidate.annotation();
        MessageQueueTemplate transport = queues.getIfAvailable();
        if (transport != null) {
            QueueSubscription spec = queueSubscription(candidate);
            ConsumerGuard guard = beans.getBeanProvider(ConsumerGuard.class).getIfAvailable();
            boolean inbox = candidate.consumer().inbox() == InboxMode.ENABLED
                    || candidate.consumer().inbox() == InboxMode.INHERIT && guard != null && guard.enabledByDefault();
            String group = spec.effectiveGroup(properties.instanceId());
            BooleanSupplier ownership = executionGuard.get();
            ExecutionGeneration current = generation;
            registrations.add(transport.subscribe(spec, message -> current.execute(ownership, () -> {
                MessageDescriptor<?> argument = json.read(message.payload(), candidate.payloadType());
                argument.restoreMetadata(message);
                if (inbox) guard.consume(new InboxScope(properties.namespace(), message.id(), group), message,
                        ignored -> consume(candidate.bean(), argument));
                else consume(candidate.bean(), argument);
                return null;
            })));
        }
    }

    private void register(ScheduledCandidate candidate) {
        RdtScheduled scheduled = candidate.annotation();
        ScheduledTaskTemplate template = scheduler.getIfAvailable();
        if (template != null) {
            String id = resolve(scheduled.id());
            String displayName = resolve(candidate.descriptor().displayName());
            ScheduleSpec cadence = schedule(candidate.descriptor());
            ScheduleOptions options = scheduleOptions(candidate);
            BooleanSupplier ownership = executionGuard.get();
            ExecutionGeneration current = generation;
            registrations.add(template.register(id, displayName, cadence, options,
                    () -> current.execute(ownership, () -> execute(candidate.bean()))));
        }
    }

    private Object execute(Object bean) throws Exception {
        ((RdtScheduledTask) bean).execute();
        return null;
    }

    @SuppressWarnings("unchecked")
    private void consume(Object bean, Object payload) throws Exception {
        ((RdtMessageConsumer<MessageDescriptor<?>>) bean).consume((MessageDescriptor<?>) payload);
    }

    private ScheduleSpec schedule(TaskDescriptor descriptor) {
        ScheduleSpec cadence = descriptor.schedule();
        RdtSupport.require(cadence != null, "Task cadence is required");
        if (cadence instanceof ScheduleSpec.Cron cron) {
            RdtSupport.require(CronExpression.isValidExpression(cron.expression()), "Invalid cron expression: " + cron.expression());
        }
        return cadence;
    }

    private QueueSubscription queueSubscription(QueueCandidate candidate) {
        RdtQueueListener annotation = candidate.annotation();
        ConsumerDescriptor descriptor = candidate.consumer();
        QueueOptions options = descriptor.options();
        String topic = resolve(annotation.topic());
        String group = resolve(annotation.group());
        String listenerId = topic + ":" + group;
        RdtComponentProperties.Listener external = components.listeners().get(listenerId);
        int concurrency = descriptor.concurrency() == -1 ? properties.queue().concurrency() : descriptor.concurrency();
        int partitions = descriptor.orderedPartitions();
        if (external != null) {
            concurrency = override(external.concurrency(), concurrency);
            partitions = override(external.orderedPartitions(), partitions);
            options = new QueueOptions(override(external.maxAttempts(), options.maxAttempts()),
                    override(external.retryDelay(), options.retryDelay()), options.retryMultiplier(), options.maxRetryDelay(),
                    override(external.visibilityTimeout(), options.visibilityTimeout()),
                    override(external.pollTimeout(), options.pollTimeout()), options.autoDeleteAfterAck(), options.deadLetterTtl());
        }
        return new QueueSubscription(topic, candidate.descriptor().mode(), group, concurrency,
                resolve(descriptor.displayName()), options, partitions, listenerId);
    }

    private ScheduleOptions scheduleOptions(ScheduledCandidate candidate) {
        ScheduleOptions declared = candidate.descriptor().options();
        RdtScheduled annotation = candidate.annotation();
        ScheduleOptions policy = new ScheduleOptions(declared.maxAttempts(), declared.retryDelay(), declared.retryMultiplier(),
                declared.maxRetryDelay(), declared.resultTtl(), declared.definitionChangePolicy(), declared.misfirePolicy(),
                declared.misfireThreshold(), annotation.failureMode(), annotation.route(), declared.targetInstanceId());
        RdtComponentProperties.Job external = components.jobs().get(resolve(annotation.id()));
        if (external == null) return policy;
        return new ScheduleOptions(override(external.maxAttempts(), policy.maxAttempts()),
                override(external.retryDelay(), policy.retryDelay()), policy.retryMultiplier(), policy.maxRetryDelay(),
                policy.resultTtl(), policy.definitionChangePolicy(), policy.misfirePolicy(), policy.misfireThreshold(),
                override(external.failureMode(), policy.failureMode()), override(external.route(), policy.route()),
                override(external.targetInstanceId(), policy.targetInstanceId()));
    }

    private boolean annotatedMethod(Method method) {
        return annotation(method, RdtLock.class) != null || annotation(method, RdtRateLimit.class) != null
                || annotation(method, RdtCacheable.class) != null || annotation(method, RdtCachePut.class) != null
                || annotation(method, RdtCacheEvict.class) != null || annotation(method, RdtId.class) != null;
    }

    private <A extends Annotation> A annotation(Method method, Class<A> type) {
        return AnnotatedElementUtils.findMergedAnnotation(method, type);
    }

    private <A extends Annotation> A annotation(Class<?> target, Class<A> type) {
        return AnnotatedElementUtils.findMergedAnnotation(target, type);
    }

    private String resolve(String value) {
        String resolved = beans.resolveEmbeddedValue(value);
        RdtSupport.require(resolved != null && !resolved.contains("${"), "Unresolved property: " + value);
        return resolved;
    }

    @Override
    public synchronized void stop() {
        generation.revoke();
        registrations.forEach(Subscription::close);
        Duration timeout = properties.queue().shutdownTimeout().compareTo(properties.schedule().shutdownTimeout()) >= 0
                ? properties.queue().shutdownTimeout() : properties.schedule().shutdownTimeout();
        generation.awaitStopped(timeout);
        registrations.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    /** Every recovery has its own work set; old callbacks cannot join the next generation. */
    private static final class ExecutionGeneration {
        private final Map<Thread, Integer> executing = new HashMap<>();
        private boolean active = true;

        private <T> T execute(BooleanSupplier ownership, Callable<T> work) throws Exception {
            RdtSupport.require(ownership.getAsBoolean(), "Node identity no longer owns this registration");
            Thread thread = Thread.currentThread();
            synchronized (this) {
                RdtSupport.require(active, "This registration has been stopped");
                executing.merge(thread, 1, Integer::sum);
            }
            try {
                return work.call();
            } finally {
                synchronized (this) {
                    executing.compute(thread, (ignored, count) -> count == 1 ? null : count - 1);
                    notifyAll();
                }
            }
        }

        private synchronized void revoke() {
            active = false;
            executing.keySet().forEach(Thread::interrupt);
        }

        private synchronized void awaitStopped(Duration timeout) {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (!executing.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new RdtConfigurationException("Previous RDT callbacks have not stopped; recovery must wait");
                try {
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new RdtConfigurationException("Interrupted while stopping RDT callbacks", exception);
                }
            }
        }
    }

    private record MethodCandidate(Object bean, Method method) { }

    private record ScheduledCandidate(Object bean, Class<?> actual, Method method, RdtScheduled annotation, TaskDescriptor descriptor) { }

    private record QueueCandidate(Object bean, Class<?> actual, Method method, RdtQueueListener annotation,
                                  Type payloadType, MessageDescriptor<?> descriptor, ConsumerDescriptor consumer) { }
}
