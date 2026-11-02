package io.github.rdt.spring.lock;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtLockException;
import io.github.rdt.lock.DeferredLockScope;
import io.github.rdt.lock.DistributedLockManager;
import io.github.rdt.lock.LockKey;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockScope;
import io.github.rdt.lock.LockType;
import java.util.Collection;
import org.springframework.core.Ordered;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 为核心锁管理器补充 Spring 事务生命周期。
 *
 * <p>
 * 业务代码仍然通过 {@link DistributedLockManager#lock(LockKey)} 添加资源锁。 仅当作用域首次实际取锁时已有同线程活跃事务且底层支持延迟关闭，才注册
 * 事务同步回调；没有事务或首次取锁先于事务时，仍由普通方法作用域释放。 跨 REQUIRES_NEW 事务须使用不同的作用域；已延迟关闭的锁仍参与顺序校验。
 * </p>
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
    public LockOptions defaultOptions() {
        return delegate.defaultOptions();
    }

    @Override
    public LockScope beginScope() {
        TransactionalLockScope previous = current.get();
        TransactionalLockScope scope = new TransactionalLockScope(previous, delegate.beginScope());
        current.set(scope);
        return scope;
    }

    @Override
    public void lock(LockKey key, LockOptions options) {
        requireCurrentScope().lock(key, options);
    }

    @Override
    public void lockAll(Collection<LockKey> keys, LockOptions options) {
        requireCurrentScope().lockAll(keys, options);
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
        private boolean firstAcquisitionSeen;
        private TransactionSynchronization synchronization;

        private TransactionalLockScope(TransactionalLockScope previous, LockScope delegateScope) {
            this.previous = previous;
            this.delegateScope = delegateScope;
        }

        @Override
        public LockOptions defaultOptions() {
            return delegate.defaultOptions();
        }

        @Override
        public void lock(LockKey key, LockOptions options) {
            requireCurrent();
            checkTransactionContext();
            delegateScope.lock(key, options);
            afterAcquisition();
        }

        @Override
        public void lockAll(Collection<LockKey> keys, LockOptions options) {
            requireCurrent();
            checkTransactionContext();
            delegateScope.lockAll(keys, options);
            if (!keys.isEmpty()) {
                afterAcquisition();
            }
        }

        @Override
        public boolean holds(LockKey key, LockType type) {
            requireCurrentOwner();
            return delegateScope.holds(key, type);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            requireCurrent();
            closed = true;
            try {
                delegateScope.close();
            } finally {
                restore();
            }
        }

        private void checkTransactionContext() {
            if (synchronization != null && (!TransactionSynchronizationManager.isSynchronizationActive()
                || !TransactionSynchronizationManager.getSynchronizations().contains(synchronization))) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                    "Cannot add to a scope bound to another (suspended) transaction");
            }
        }

        private void afterAcquisition() {
            if (firstAcquisitionSeen) {
                return;
            }
            firstAcquisitionSeen = true;
            if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.isSynchronizationActive()
                || !(delegateScope instanceof DeferredLockScope deferredScope)) {
                return;
            }
            try {
                deferredScope.deferClose();
                TransactionSynchronization callback = new LockTransactionSynchronization(this);
                TransactionSynchronizationManager.registerSynchronization(callback);
                synchronization = callback;
            } catch (RuntimeException failure) {
                try {
                    delegateScope.close();
                    deferredScope.completeClose();
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                closed = true;
                restore();
                throw failure;
            }
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
                synchronization = null;
                restore();
            }
        }

        private void requireCurrent() {
            requireCurrentOwner();
            if (current.get() != this) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                    "Lock scopes must be used and closed in nesting order");
            }
        }

        private void requireCurrentOwner() {
            if (Thread.currentThread() != owner) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                    "Lock scope must only be used by its creating thread");
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
