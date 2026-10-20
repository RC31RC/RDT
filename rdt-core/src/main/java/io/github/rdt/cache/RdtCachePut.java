package io.github.rdt.cache;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 缓存成功的方法结果；启用事务同步时延迟到提交后写入。 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RdtCachePut {
    String region() default "";

    /**
     * 返回SpEL 键，可引用 #result。
     * @return SpEL 键，可引用 #result
     */
    String key();
    /**
     * 返回ISO-8601 TTL，空字符串使用配置默认值。
     * @return ISO-8601 TTL，空字符串使用配置默认值
     */
    String ttl() default "";
}
