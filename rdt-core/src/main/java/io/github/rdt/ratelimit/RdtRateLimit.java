package io.github.rdt.ratelimit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 对同步 Spring 方法执行全局速率限制或带租约的并发限制。 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RdtRateLimit {
    /**
     * 返回SpEL 资源键。
     * @return SpEL 资源键
     */
    String key();
    /**
     * 返回限流模式。
     * @return 限流模式
     */
    RateMode mode() default RateMode.QPS;
    /**
     * 返回每个时间间隔的许可数或并发容量。
     * @return 每个时间间隔的许可数或并发容量
     */
    int permits() default 10;
    /**
     * 返回ISO-8601 QPS 间隔，空字符串继承默认值。
     * @return ISO-8601 QPS 间隔，空字符串继承默认值
     */
    String interval() default "";
    /**
     * 返回ISO-8601 最长等待时间，空字符串继承默认值。
     * @return ISO-8601 最长等待时间，空字符串继承默认值
     */
    String waitTime() default "";
    /**
     * 返回并发租约，空字符串继承默认值；业务必须在租约到期前完成。
     * @return 并发租约，空字符串继承默认值；业务必须在租约到期前完成
     */
    String leaseTime() default "";
}
