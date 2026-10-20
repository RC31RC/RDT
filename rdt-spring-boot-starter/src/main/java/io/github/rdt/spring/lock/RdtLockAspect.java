package io.github.rdt.spring.lock;

import io.github.rdt.lock.DistributedLockManager;
import io.github.rdt.lock.LockScope;
import io.github.rdt.lock.RdtLock;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.annotation.Order;

/**
 * 锁切面只负责建立同步调用作用域。
 *
 * <p>具体资源锁由业务方法注入 {@link DistributedLockManager} 后显式追加，
 * 因此一个方法可以根据实际处理过程追加多把锁，且不会把业务 key 隐藏在注解
 * SpEL 中。若当前调用已加入 Spring 事务，锁由事务同步器在提交或回滚完成后释放；
 * 没有事务时仍在同步方法作用域结束时释放。</p>
 */
@Aspect
@Order(-200)
public final class RdtLockAspect {
    private final DistributedLockManager lockManager;

    /**
     * 创建锁切面。
     *
     * @param lockManager 作用域锁管理器
     */
    public RdtLockAspect(DistributedLockManager lockManager) {
        this.lockManager = lockManager;
    }

    /**
     * 建立锁作用域，并保证方法异常时清理；有事务时延迟到事务完成后释放。
     * @param point 方法调用
     * @return 业务结果
     * @throws Throwable 原始业务异常；释放异常在存在主异常时作为受抑制异常保留
     */
    @Around("@annotation(annotation)")
    public Object around(ProceedingJoinPoint point, RdtLock annotation) throws Throwable {
        try (LockScope ignored = lockManager.beginScope()) {
            return point.proceed();
        }
    }
}
