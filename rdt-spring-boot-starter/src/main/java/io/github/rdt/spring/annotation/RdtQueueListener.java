package io.github.rdt.spring.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.stereotype.Component;

/** Type-level consumer identity; all operational policy comes from its concrete MessageDescriptor. */
@Component
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RdtQueueListener {
    String topic();
    String group();
}
