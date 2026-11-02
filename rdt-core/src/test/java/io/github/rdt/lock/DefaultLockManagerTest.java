package io.github.rdt.lock;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.exception.RdtException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class DefaultLockManagerTest {
    private static final class ResourceA {}

    private static final class ResourceB {}

    private static final class ResourceC {}

    private final DistributedLockTemplate template = mock(DistributedLockTemplate.class);
    private final DefaultLockManager manager = new DefaultLockManager(template);
    private final LockOptions options = LockOptions.defaults();

    @BeforeEach
    void defaults() {
        when(template.defaultOptions()).thenReturn(options);
    }

    private static LockKey key(String id) {
        return LockKey.forResource(ResourceA.class, id);
    }

    @Test
    void buildsStableKeysWithoutClassSuffixAndRejectsInvalidNames() {
        assertThat(key("123").value()).isEqualTo("ResourceA:123");
        assertThat(LockKey.forOperation(ResourceA.class, "claim", "request-42").value())
            .isEqualTo("ResourceA:claim:request-42");
        assertThat(LockKey.forOperation(ResourceA.class, "submit", null).value()).isEqualTo("ResourceA:submit");
        assertThat(LockKey.named(ResourceB.class, "refresh").value()).isEqualTo("ResourceB:refresh");
        assertThatThrownBy(() -> LockKey.forResource(ResourceA.class, " "))
            .isInstanceOf(RdtConfigurationException.class);
    }

    @Test
    void requiresScopeAndReleasesDistinctLocksInReverseOrder() {
        assertThatThrownBy(() -> manager.lock(key("a"))).isInstanceOf(RdtException.class);
        LockHandle first = handle(key("a"));
        LockHandle second = handle(key("b"));
        when(first.isHeld()).thenReturn(true);
        try (LockScope scope = manager.beginScope()) {
            manager.lock(key("a"));
            manager.lock(key("a"));
            scope.lock(key("b"));
            assertThat(scope.holds(key("a"))).isTrue();
        }
        verify(template, times(1)).tryLock(key("a").value(), options);
        var order = inOrder(first, second);
        order.verify(second).close();
        order.verify(first).close();
        assertThat(manager.currentScope()).isNull();
    }

    @Test
    void rejectsForeignThreadWithoutLosingHandles() {
        LockHandle handle = handle(key("a"));
        try (LockScope scope = manager.beginScope()) {
            manager.lock(key("a"));
            CompletableFuture.runAsync(() -> {
                assertThatThrownBy(scope::close).isInstanceOf(RdtException.class);
                assertThatThrownBy(() -> scope.lock(key("b"))).isInstanceOf(RdtException.class);
                assertThatThrownBy(() -> manager.lock(key("b"))).isInstanceOf(RdtException.class);
            }).join();
            verify(handle, never()).close();
        }
        verify(handle).close();
    }

    @Test
    void rejectsOutOfOrderCloseAndRestoresParent() {
        try (LockScope outer = manager.beginScope()) {
            try (LockScope inner = manager.beginScope()) {
                assertThatThrownBy(outer::close).isInstanceOf(RdtException.class);
                assertThat(manager.currentScope()).isSameAs(inner);
            }
            assertThat(manager.currentScope()).isSameAs(outer);
        }
        assertThat(manager.currentScope()).isNull();
    }

    @Test
    void releasesOnlyNestedReentrantLevelAndRejectsInversionAcrossScopes() {
        LockHandle outerHandle = handle(key("b"));
        when(outerHandle.isHeld()).thenReturn(true);
        LockHandle innerHandle = mock(LockHandle.class);
        when(template.tryLock(key("b").value(), options)).thenReturn(Optional.of(outerHandle),
            Optional.of(innerHandle));
        try (LockScope outer = manager.beginScope()) {
            manager.lock(key("b"));
            try (LockScope inner = manager.beginScope()) {
                manager.lock(key("b")); // physical reentry even though b is already held
            }
            verify(innerHandle).close();
            verify(outerHandle, never()).close();
            try (LockScope inner = manager.beginScope()) {
                assertThatThrownBy(() -> inner.lock(key("a"))).hasMessageContaining("order inversion");
            }
        }
        verify(outerHandle).close();
    }

    @Test
    void rollsBackOnAcquisitionFailureAndRetainsCleanupFailure() {
        LockHandle first = handle(key("a"));
        IllegalStateException failure = new IllegalStateException("transport");
        IllegalStateException cleanup = new IllegalStateException("release");
        when(template.tryLock(key("b").value(), options)).thenThrow(failure);
        doThrow(cleanup).when(first).close();
        try (LockScope ignored = manager.beginScope()) {
            manager.lock(key("a"));
            assertThatThrownBy(() -> manager.lock(key("b"))).isSameAs(failure)
                .satisfies(error -> assertThat(error.getSuppressed()).containsExactly(cleanup));
        }
        assertThat(manager.currentScope()).isNull();
        assertThatThrownBy(manager::beginScope).hasMessageContaining("release outcome unknown")
            .hasCause(failure.getSuppressed()[0]);
    }

    @Test
    void batchDeduplicatesAndSortsByTypeThenId() {
        List<String> acquired = new ArrayList<>();
        when(template.tryLock(any(), any())).thenAnswer(call -> {
            acquired.add(call.getArgument(0));
            return Optional.of(mock(LockHandle.class));
        });
        DefaultLockManager ordered =
            new DefaultLockManager(template, new LockOrder(List.of(ResourceA.class, ResourceB.class)));
        try (LockScope scope = ordered.beginScope()) {
            ordered.lockAll(List.of(LockKey.forResource(ResourceB.class, "2"), key("9"), key("1"), key("1"),
                LockKey.forResource(ResourceB.class, "1")));
        }
        assertThat(acquired).containsExactly("ResourceA:1", "ResourceA:9", "ResourceB:1", "ResourceB:2");
    }

    @Test
    void noCrossTypeConfigurationAllowsReverseTypeAcquisition() {
        handle(LockKey.forResource(ResourceB.class, "1"));
        handle(key("1"));
        try (LockScope scope = manager.beginScope()) {
            manager.lock(LockKey.forResource(ResourceB.class, "1"));
            manager.lock(key("1"));
        }
    }

    @Test
    void batchWithoutCrossTypeConfigurationPreservesTypeGroupOrder() {
        List<String> acquired = new ArrayList<>();
        when(template.tryLock(any(), any())).thenAnswer(call -> {
            acquired.add(call.getArgument(0));
            return Optional.of(mock(LockHandle.class));
        });
        try (LockScope ignored = manager.beginScope()) {
            manager.lockAll(List.of(LockKey.forResource(ResourceB.class, "9"), key("2"),
                LockKey.forResource(ResourceB.class, "1"), key("1")));
        }
        assertThat(acquired).containsExactly("ResourceB:1", "ResourceB:9", "ResourceA:1", "ResourceA:2");
    }

    @Test
    void batchFailureReleasesAcquiredLocksInReverseOrder() {
        LockHandle first = handle(key("1"));
        LockHandle second = handle(key("2"));
        when(template.tryLock(key("3").value(), options)).thenReturn(Optional.empty());
        try (LockScope ignored = manager.beginScope()) {
            assertThatThrownBy(() -> manager.lockAll(List.of(key("3"), key("2"), key("1"))))
                .hasMessageContaining("deadline");
        }
        var order = inOrder(first, second);
        order.verify(second).close();
        order.verify(first).close();
        assertThat(manager.currentScope()).isNull();
    }

    @Test
    void configuredOrderRejectsTypeInversionUnknownTypeAndDuplicateNames() {
        DefaultLockManager ordered =
            new DefaultLockManager(template, new LockOrder(List.of(ResourceA.class, ResourceB.class)));
        LockHandle held = handle(LockKey.forResource(ResourceB.class, "1"));
        try (LockScope scope = ordered.beginScope()) {
            ordered.lock(LockKey.forResource(ResourceB.class, "1"));
            assertThatThrownBy(() -> ordered.lock(key("1"))).hasMessageContaining("order inversion");
        }
        verify(held).close();
        assertThat(new LockOrder(List.of(ResourceA.class, ResourceB.class)).signature())
            .isEqualTo("ResourceA->ResourceB");
        assertThatThrownBy(() -> new LockOrder(List.of(ResourceA.class, ResourceA.class)))
            .hasMessageContaining("Duplicate");
        try (LockScope scope = ordered.beginScope()) {
            assertThatThrownBy(() -> ordered.lock(LockKey.named(ResourceC.class, "x")))
                .hasMessageContaining("Unknown lock order resource type");
        }
    }

    @Test
    void deferredCloseStillParticipatesInThreadOrderUntilCompleted() {
        LockHandle held = handle(key("z"));
        DeferredLockScope deferred = (DeferredLockScope)manager.beginScope();
        manager.lock(key("z"));
        deferred.deferClose();
        deferred.close();
        assertThat(manager.currentScope()).isNull();
        try (LockScope ignored = manager.beginScope()) {
            assertThatThrownBy(() -> manager.lock(key("a"))).hasMessageContaining("order inversion");
        }
        verify(held, never()).close();
        deferred.completeClose();
        verify(held).close();
        handle(key("a"));
        try (LockScope ignored = manager.beginScope()) {
            manager.lock(key("a"));
        }
    }

    @Test
    void distinctPhysicalKeySpacesAreNotDeduplicatedButRwModesConflict() {
        LockHandle reentrant = handle(key("1"));
        LockOptions fair = new LockOptions(LockType.FAIR, Duration.ZERO, Duration.ZERO);
        LockHandle fairHandle = mock(LockHandle.class);
        when(template.tryLock(key("1").value(), fair)).thenReturn(Optional.of(fairHandle));
        when(fairHandle.isHeld()).thenReturn(true);
        try (LockScope scope = manager.beginScope()) {
            manager.lock(key("1"));
            manager.lock(key("1"), fair);
            assertThat(scope.holds(key("1"), LockType.FAIR)).isTrue();
        }
        verify(reentrant).close();
        verify(fairHandle).close();
        LockOptions read = new LockOptions(LockType.READ, Duration.ZERO, Duration.ZERO);
        LockOptions write = new LockOptions(LockType.WRITE, Duration.ZERO, Duration.ZERO);
        LockHandle readHandle = mock(LockHandle.class);
        when(template.tryLock(key("1").value(), read)).thenReturn(Optional.of(readHandle));
        try (LockScope ignored = manager.beginScope()) {
            manager.lock(key("1"), read);
            assertThatThrownBy(() -> manager.lock(key("1"), write)).hasMessageContaining("Cannot change lock type");
        }
        verify(readHandle).close();
        verify(template, never()).tryLock(key("1").value(), write);
    }

    private LockHandle handle(LockKey key) {
        LockHandle handle = mock(LockHandle.class);
        when(template.tryLock(key.value(), options)).thenReturn(Optional.of(handle));
        return handle;
    }
}
