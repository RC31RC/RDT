package io.github.rdt.id;

import java.lang.annotation.*;

/** Allocates an ID after a synchronous long/Long method succeeds, replacing its return value. */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RdtId {
    /** SpEL sequence name. */
    String sequence();
    IdMode mode() default IdMode.ATOMIC;
    /** Optional inclusive minimum, evaluated with #result; only supported by ATOMIC. */
    String minimum() default "";
}
