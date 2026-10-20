package io.github.rdt.cache;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 默认在成功执行后清除缓存，也可显式配置为调用前清除。 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RdtCacheEvict {
    String region() default "";

    /**
     * 返回SpEL 键。
     * @return SpEL 键
     */
    String key() default "";

    /**
     * 返回是否在调用前清除缓存，且不受事务提交结果影响。
     * @return 是否在调用前清除缓存，且不受事务提交结果影响
     */
    boolean beforeInvocation() default false;
}
