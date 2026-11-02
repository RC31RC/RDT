package io.github.rdt.lock;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtLockException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Scope-based lock manager backed by a single {@link DistributedLockTemplate}. Tracks physical holdings across nested
 * scopes and scopes deferred until transaction completion. Same-type ID order is always enforced; cross-type order is
 * optional.
 */
public final class DefaultLockManager implements DistributedLockManager {
    private final DistributedLockTemplate locks;
    private final LockOrder order;
    private final ThreadLocal<Tracking> tracking = new ThreadLocal<>();

    public DefaultLockManager(DistributedLockTemplate locks) {
        this(locks, null);
    }

    public DefaultLockManager(DistributedLockTemplate locks, LockOrder order) {
        this.locks = Objects.requireNonNull(locks);
        this.order = order;
    }

    @Override
    public LockOptions defaultOptions() {
        return locks.defaultOptions();
    }

    @Override
    public LockScope beginScope() {
        Tracking state = tracking.get();
        if (state == null) {
            state = new Tracking();
            tracking.set(state);
        }
        if (state.failedRelease != null) {
            throw new RdtLockException(RdtErrorCode.LOCK_OWNERSHIP,
                "Lock release outcome unknown; cannot open another scope on this thread", state.failedRelease);
        }
        LockScopeImpl scope = new LockScopeImpl(state, state.current);
        state.current = scope;
        return scope;
    }

    @Override
    public void lock(LockKey key, LockOptions options) {
        requireScope().lock(key, options);
    }

    @Override
    public void lockAll(Collection<LockKey> keys, LockOptions options) {
        requireScope().lockAll(keys, options);
    }

    @Override
    public LockScope currentScope() {
        Tracking state = tracking.get();
        return state == null ? null : state.current;
    }

    private LockScopeImpl requireScope() {
        Tracking state = tracking.get();
        if (state == null || state.current == null) {
            throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                "No active lock scope; use @RdtLock or beginScope first");
        }
        return state.current;
    }

    private static Identity identity(LockKey key, LockType type) {
        String space = switch (type) {
            case REENTRANT -> "lock";
            case FAIR -> "fairlock";
            case READ, WRITE -> "rwlock";
        };
        return new Identity(space, key.value());
    }

    private static final class Tracking {
        private LockScopeImpl current;
        private final List<Acquired> active = new ArrayList<>();
        private RuntimeException failedRelease;
    }

    private record Identity(String keySpace, String value) {
    }

    private record Acquired(Identity identity, LockKey key, LockOptions options, LockHandle handle) {
    }

    private final class LockScopeImpl implements DeferredLockScope {
        private final Tracking state;
        private final LockScopeImpl previous;
        private final Thread owner = Thread.currentThread();
        private final Map<Identity, Acquired> held = new LinkedHashMap<>();
        private final Deque<Identity> acquisitionOrder = new ArrayDeque<>();
        private boolean deferred;
        private boolean closeRequested;
        private boolean completed;

        private LockScopeImpl(Tracking state, LockScopeImpl previous) {
            this.state = state;
            this.previous = previous;
        }

        @Override
        public LockOptions defaultOptions() {
            return locks.defaultOptions();
        }

        @Override
        public void lock(LockKey key, LockOptions options) {
            requireCurrentScope();
            if (closeRequested || completed) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope is already closed");
            }
            requireKnownOwnership();
            if (key == null || options == null) {
                throw new RdtConfigurationException("Lock key and options are required");
            }
            Identity id = identity(key, options.type());
            Acquired existing = held.get(id);
            if (existing != null) {
                if (existing.options.type() != options.type()
                    || !existing.options.leaseTime().equals(options.leaseTime())) {
                    throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                        "Cannot change lock type or lease within the same scope");
                }
                if (!existing.handle.isHeld()) {
                    throw rollback(
                        new RdtLockException(RdtErrorCode.LOCK_OWNERSHIP, "Lock ownership was lost before reentry"));
                }
                return;
            }
            try {
                checkOrder(key, id, options);
                Optional<LockHandle> handle = locks.tryLock(key.value(), options);
                if (handle.isEmpty()) {
                    throw new RdtLockException(RdtErrorCode.LOCK_TIMEOUT, "Distributed lock deadline exceeded");
                }
                Acquired acquired = new Acquired(id, key, options, handle.orElseThrow());
                held.put(id, acquired);
                acquisitionOrder.addLast(id);
                state.active.add(acquired);
            } catch (RuntimeException failure) {
                throw rollback(failure);
            }
        }

        @Override
        public void lockAll(Collection<LockKey> keys, LockOptions options) {
            requireCurrentScope();
            if (closeRequested || completed) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope is already closed");
            }
            requireKnownOwnership();
            if (keys == null || options == null) {
                throw new RdtConfigurationException("Lock keys and options are required");
            }
            Map<Identity, LockKey> distinct = new LinkedHashMap<>();
            Map<String, Integer> groups = new LinkedHashMap<>();
            for (LockKey key : keys) {
                if (key == null) {
                    throw new RdtConfigurationException("Lock key is required");
                }
                if (order != null) {
                    order.position(key.resourceType());
                }
                distinct.putIfAbsent(identity(key, options.type()), key);
                groups.putIfAbsent(key.resourceType(), groups.size());
            }
            List<LockKey> sorted = new ArrayList<>(distinct.values());
            sorted.sort(Comparator.comparingInt(
                (LockKey key) -> order == null ? groups.get(key.resourceType()) : order.position(key.resourceType()))
                .thenComparing(LockKey::scopeId));
            for (LockKey key : sorted) {
                try {
                    lock(key, options);
                } catch (RuntimeException failure) {
                    throw rollback(failure);
                }
            }
        }

        private void requireKnownOwnership() {
            if (state.failedRelease != null) {
                throw new RdtLockException(RdtErrorCode.LOCK_OWNERSHIP,
                    "Lock release outcome unknown; cannot acquire more locks on this thread", state.failedRelease);
            }
        }

        private void checkOrder(LockKey key, Identity id, LockOptions options) {
            int target = order == null ? -1 : order.position(key.resourceType());
            for (Acquired acquired : state.active) {
                if (acquired.identity.equals(id)) {
                    if (acquired.options.type() != options.type()
                        || !acquired.options.leaseTime().equals(options.leaseTime())) {
                        throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                            "Cannot change lock type or lease while the physical lock is held");
                    }
                    if (!acquired.handle.isHeld()) {
                        throw new RdtLockException(RdtErrorCode.LOCK_OWNERSHIP,
                            "Lock ownership was lost before nested reentry");
                    }
                    return; // Nested, physical reentry is allowed and released by the nested scope.
                }
            }
            for (Acquired acquired : state.active) {
                LockKey heldKey = acquired.key;
                if (order != null && target < order.position(heldKey.resourceType())) {
                    throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                        "Lock order inversion: held " + heldKey.value() + " before " + key.value());
                }
                if (heldKey.resourceType().equals(key.resourceType())
                    && heldKey.scopeId().compareTo(key.scopeId()) > 0) {
                    throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                        "Lock ID order inversion: held " + heldKey.value() + " before " + key.value());
                }
            }
        }

        @Override
        public boolean holds(LockKey key, LockType type) {
            requireOwner();
            if (key == null || type == null) {
                throw new RdtConfigurationException("Lock key and type are required");
            }
            Acquired acquired = held.get(identity(key, type));
            return !closeRequested && !completed && acquired != null && acquired.options.type() == type
                && acquired.handle.isHeld();
        }

        @Override
        public void deferClose() {
            requireCurrentScope();
            if (closeRequested || completed) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE, "Lock scope is already closed");
            }
            deferred = true;
        }

        @Override
        public void close() {
            requireOwner();
            if (closeRequested || completed) {
                return;
            }
            requireCurrentScope();
            closeRequested = true;
            restore();
            if (!deferred) {
                release();
            }
        }

        @Override
        public void completeClose() {
            requireOwner();
            if (completed) {
                return;
            }
            if (!closeRequested && !deferred) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                    "Lock scope must be closed before transaction completion");
            }
            release();
        }

        private void release() {
            if (completed) {
                return;
            }
            completed = true;
            RuntimeException failure = null;
            while (!acquisitionOrder.isEmpty()) {
                Acquired acquired = held.remove(acquisitionOrder.removeLast());
                try {
                    acquired.handle.close();
                    for (int i = state.active.size() - 1; i >= 0; i--) {
                        if (state.active.get(i) == acquired) {
                            state.active.remove(i);
                            break;
                        }
                    }
                } catch (RuntimeException exception) {
                    state.failedRelease = exception; // Ownership may still exist after a failed unlock.
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
            restore();
            if (state.current == null && state.active.isEmpty()) {
                tracking.remove();
            }
            if (failure != null) {
                throw failure;
            }
        }

        private RuntimeException rollback(RuntimeException failure) {
            try {
                closeRequested = true;
                restore();
                release(); // A failed acquisition releases this scope, even when transaction-bound.
            } catch (RuntimeException cleanupFailure) {
                if (cleanupFailure != failure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            return failure;
        }

        private void requireOwner() {
            if (Thread.currentThread() != owner) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                    "Lock scope must only be used by its creating thread");
            }
        }

        private void requireCurrentScope() {
            requireOwner();
            if (state.current != this) {
                throw new RdtLockException(RdtErrorCode.LOCK_SCOPE,
                    "Lock scopes must be used and closed in nesting order");
            }
        }

        private void restore() {
            if (state.current == this) {
                state.current = previous;
            }
        }
    }
}
