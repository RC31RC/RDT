package io.github.rdt.spring.annotation;

import io.github.rdt.schedule.JobRoute;
import io.github.rdt.schedule.ScheduleFailureMode;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.stereotype.Component;

/** Stable task identity and dispatch/failure policy; task data belongs to TaskDescriptor. */
@Component
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RdtScheduled {
    String id();
    JobRoute route() default JobRoute.HEALTH_FIRST;
    ScheduleFailureMode failureMode() default ScheduleFailureMode.CONTINUE;
}
