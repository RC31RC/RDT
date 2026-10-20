package io.github.rdt.lock;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 为 Spring Bean 的公开同步方法开启锁作用域，不拦截同类内部自调用。
 *
 * <p>该注解只描述方法的锁生命周期，不描述具体资源。业务代码通过
 * {@link DistributedLockManager} 显式追加需要保护的资源锁。</p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RdtLock {
}
