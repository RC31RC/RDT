package io.github.rdt.cache;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 返回缓存的方法结果，并通过分布式互斥保证同一键单次回源。 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RdtCacheable {
    String region() default "";

    /**
     * 返回必填 SpEL 键。
     * @return 必填 SpEL 键
     */
    String key();
    /**
     * 返回ISO-8601 TTL，空字符串使用配置默认值。
     * @return ISO-8601 TTL，空字符串使用配置默认值
     */
    String ttl() default "";
    /**
     * 返回是否使用较短的空值缓存 TTL 保存 null 结果。
     * @return 是否使用较短的空值缓存 TTL 保存 null 结果
     */
    boolean cacheNull() default true;
}
