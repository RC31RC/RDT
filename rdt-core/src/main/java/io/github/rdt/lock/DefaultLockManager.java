package io.github.rdt.lock;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.exception.RdtLockException;
import io.github.rdt.exception.RdtErrorCode;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 基于 {@link DistributedLockTemplate} 的作用域锁管理器。
 *
 * <p>作用域内重复加同一把锁会校验所有权，不重复获取锁。
 * 某一把新锁获取失败时，会回滚本次作用域已经获取的全部锁。</p>
 */
public final class DefaultLockManager implements DistributedLockManager {
    private final DistributedLockTemplate locks;
    private final ThreadLocal<LockScopeImpl> current = new ThreadLocal<>();

    public DefaultLockManager(DistributedLockTemplate locks) {
        this.locks = locks;
    }

    @Override
    public LockOptions defaultOptions() { return locks.defaultOptions(); }

    @Override
    public LockScope beginScope() {
        LockScopeImpl previous = current.get();
        LockScopeImpl scope = new LockScopeImpl(previous);
        current.set(scope);
        return scope;
    }

    @Override
    public void lock(String key, LockOptions options) {
        LockScopeImpl scope = current.get();
        if (scope == null) {
            throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "No active lock scope; use @RdtLock or beginScope first");
        }
        scope.lock(key, options);
    }

    @Override
    public LockScope currentScope() {
        return current.get();
    }

    private final class LockScopeImpl implements DeferredLockScope {
        private final LockScopeImpl previous;
        private final Thread owner = Thread.currentThread();
        private final Map<String, HeldLock> held = new LinkedHashMap<>();
        private final Deque<String> acquisitionOrder = new ArrayDeque<>();
        private boolean deferred;
        private boolean closeRequested;
        private boolean completed;
        private boolean closed;

        private LockScopeImpl(LockScopeImpl previous) {
            this.previous = previous;
        }

        @Override
        public LockOptions defaultOptions() { return locks.defaultOptions(); }

        @Override
        public void lock(String key, LockOptions options) {
            requireCurrentScope();
            if (closed) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope is already closed");
            }
            if (key == null || key.isBlank() || options == null) {
                throw new RdtConfigurationException("Lock key and options are required");
            }
            HeldLock existing = held.get(key);
            if (existing != null) {
                if (existing.options.type() != options.type()
                        || !existing.options.leaseTime().equals(options.leaseTime())) {
                    throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Cannot change lock type or lease within the same scope");
                }
                if (!existing.handle.isHeld()) {
                    throw rollback(new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock ownership was lost before reentry"));
                }
                return;
            }
            Optional<LockHandle> handle;
            try {
                handle = locks.tryLock(key, options);
            } catch (RuntimeException exception) {
                throw rollback(exception);
            }
            if (handle.isEmpty()) {
                throw rollback(new RdtLockException(RdtErrorCode.LOCK_TIMEOUT, "Distributed lock deadline exceeded"));
            }
            held.put(key, new HeldLock(handle.orElseThrow(), options));
            acquisitionOrder.addLast(key);
        }

        @Override
        public boolean holds(String key) {
            requireOwner();
            HeldLock lock = held.get(key);
            return !closeRequested && !completed && lock != null && lock.handle.isHeld();
        }

        @Override
        public void deferClose() {
            requireCurrentScope();
            if (closed || completed) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope is already closed");
            }
            deferred = true;
        }

        @Override
        public void close() {
            requireOwner();
            if (closed || completed) {
                return;
            }
            requireCurrentScope();
            closeRequested = true;
            restore();
            if (deferred) {
                return;
            }
            release();
        }

        @Override
        public void completeClose() {
            requireOwner();
            if (completed) {
                return;
            }
            if (!closeRequested && !deferred) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope must be closed before transaction completion");
            }
            release();
        }

        private void release() {
            if (closed || completed) {
                return;
            }
            closed = true;
            RuntimeException failure = null;
            while (!acquisitionOrder.isEmpty()) {
                String key = acquisitionOrder.removeLast();
                HeldLock lock = held.remove(key);
                if (lock == null) {
                    continue;
                }
                try {
                    lock.handle.close();
                } catch (RuntimeException exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
            completed = true;
            restore();
            if (failure != null) {
                throw failure;
            }
        }

        private RuntimeException rollback(RuntimeException failure) {
            try {
                requireCurrentScope();
                closeRequested = true;
                restore();
                release();
            } catch (RuntimeException cleanupFailure) {
                if (cleanupFailure != failure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            return failure;
        }

        private void requireOwner() {
            if (Thread.currentThread() != owner) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope must only be used by its creating thread");
            }
        }

        private void requireCurrentScope() {
            requireOwner();
            if (current.get() != this) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scopes must be used and closed in nesting order");
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

    private static final class HeldLock {
        private final LockHandle handle;
        private final LockOptions options;

        private HeldLock(LockHandle handle, LockOptions options) {
            this.handle = handle;
            this.options = options;
        }
    }
}
