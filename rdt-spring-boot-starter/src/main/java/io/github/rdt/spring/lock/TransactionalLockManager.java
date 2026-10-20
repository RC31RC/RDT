package io.github.rdt.spring.lock;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtLockException;
import io.github.rdt.lock.DeferredLockScope;
import io.github.rdt.lock.DistributedLockManager;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockScope;
import org.springframework.core.Ordered;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 为核心锁管理器补充 Spring 事务生命周期。
 *
 * <p>业务代码仍然通过 {@link DistributedLockManager#lock(String)} 添加资源锁。
 * 只有当资源锁实际被添加且当前线程存在事务时，才注册事务同步回调；
 * 没有事务时保留普通方法作用域的释放行为。</p>
 */
public final class TransactionalLockManager implements DistributedLockManager {
    private final DistributedLockManager delegate;
    private final ThreadLocal<TransactionalLockScope> current = new ThreadLocal<>();

    /**
     * 创建事务感知的锁管理器。
     *
     * @param delegate 核心锁管理器
     */
    public TransactionalLockManager(DistributedLockManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public LockOptions defaultOptions() { return delegate.defaultOptions(); }

    @Override
    public LockScope beginScope() {
        TransactionalLockScope previous = current.get();
        TransactionalLockScope scope = new TransactionalLockScope(previous, delegate.beginScope());
        current.set(scope);
        return scope;
    }

    @Override
    public void lock(String key, LockOptions options) {
        TransactionalLockScope scope = requireCurrentScope();
        scope.lock(key, options);
    }

    @Override
    public LockScope currentScope() {
        return current.get();
    }

    private TransactionalLockScope requireCurrentScope() {
        TransactionalLockScope scope = current.get();
        if (scope == null) {
            throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "No active transactional lock scope");
        }
        return scope;
    }

    private final class TransactionalLockScope implements LockScope {
        private final TransactionalLockScope previous;
        private final LockScope delegateScope;
        private final Thread owner = Thread.currentThread();
        private boolean closed;
        private boolean transactionBound;

        private TransactionalLockScope(TransactionalLockScope previous, LockScope delegateScope) {
            this.previous = previous;
            this.delegateScope = delegateScope;
        }

        @Override
        public LockOptions defaultOptions() { return delegate.defaultOptions(); }

        @Override
        public void lock(String key, LockOptions options) {
            requireCurrent();
            delegateScope.lock(key, options);
            bindToTransactionIfNecessary();
        }

        @Override
        public boolean holds(String key) {
            requireCurrentOwner();
            return delegateScope.holds(key);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            requireCurrent();
            closed = true;
            delegateScope.close();
            restore();
        }

        private void bindToTransactionIfNecessary() {
            if (transactionBound
                    || !TransactionSynchronizationManager.isActualTransactionActive()
                    || !TransactionSynchronizationManager.isSynchronizationActive()) {
                return;
            }
            if (!(delegateScope instanceof DeferredLockScope deferredScope)) {
                return;
            }
            deferredScope.deferClose();
            transactionBound = true;
            TransactionSynchronizationManager.registerSynchronization(new LockTransactionSynchronization(this));
        }

        private void completeAfterTransaction() {
            try {
                if (delegateScope instanceof DeferredLockScope deferredScope) {
                    deferredScope.completeClose();
                } else {
                    delegateScope.close();
                }
            } finally {
                closed = true;
                restore();
            }
        }

        private void requireCurrent() {
            requireCurrentOwner();
            if (current.get() != this) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scopes must be used and closed in nesting order");
            }
        }

        private void requireCurrentOwner() {
            if (Thread.currentThread() != owner) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope must only be used by its creating thread");
            }
        }

        private void restore() {
            if (current.get() == this) {
                if (previous == null) {
                    current.remove();
                } else {
                    current.set(previous);
                }
            }
        }
    }

    /**
     * 在事务完成后释放锁；使用最低优先级尽量让业务的完成回调先记录结果。
     */
    private final class LockTransactionSynchronization implements TransactionSynchronization, Ordered {
        private final TransactionalLockScope scope;

        private LockTransactionSynchronization(TransactionalLockScope scope) {
            this.scope = scope;
        }

        @Override
        public void afterCompletion(int status) {
            scope.completeAfterTransaction();
        }

        @Override
        public int getOrder() {
            return Ordered.LOWEST_PRECEDENCE;
        }
    }
}
