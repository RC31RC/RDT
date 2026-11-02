package io.github.rdt.redisson.lock;

import io.github.rdt.lock.DefaultLockManager;
import io.github.rdt.lock.LockHandle;
import io.github.rdt.lock.LockKey;
import io.github.rdt.lock.LockOptions;
import io.github.rdt.lock.LockOrder;
import io.github.rdt.lock.LockScope;
import io.github.rdt.lock.LockType;
import io.github.rdt.support.KeySpace;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real Redis verification; run with -Drdt.it.redis=redis://localhost:6379. */
@EnabledIfSystemProperty(named = "rdt.it.redis", matches = ".+")
class LockManagerIT {
    private static final class Account {}

    private static final class Transfer {}

    private static Config config() {
        Config config = new Config();
        config.useSingleServer().setAddress(System.getProperty("rdt.it.redis"));
        return config;
    }

    @Test
    void namespaceAndPhysicalKeySpaceMatchRealRedissonContention() {
        RedissonClient first = Redisson.create(config());
        RedissonClient second = Redisson.create(config());
        try {
            KeySpace space = new KeySpace("lock-key-it:" + UUID.randomUUID());
            LockKey key = LockKey.forResource(Account.class, "7");
            var firstTemplate = new RedissonLockTemplate(first, space);
            var secondTemplate = new RedissonLockTemplate(second, space);
            var otherNamespace = new RedissonLockTemplate(second, new KeySpace("lock-key-other:" + UUID.randomUUID()));
            var manager = new DefaultLockManager(firstTemplate, new LockOrder(List.of(Account.class, Transfer.class)));
            LockOptions immediate = new LockOptions(LockType.REENTRANT, Duration.ZERO, Duration.ZERO);
            LockOptions fair = new LockOptions(LockType.FAIR, Duration.ZERO, Duration.ZERO);
            try (LockScope scope = manager.beginScope()) {
                manager.lockAll(List.of(key, key), immediate);
                assertThat(secondTemplate.tryLock(key.value(), immediate)).isEmpty();
                try (LockHandle independent = secondTemplate.tryLock(key.value(), fair).orElseThrow();
                    LockHandle isolated = otherNamespace.tryLock(key.value(), immediate).orElseThrow()) {
                    assertThat(independent.isHeld()).isTrue();
                    assertThat(isolated.isHeld()).isTrue();
                }
                assertThatThrownBy(() -> manager.lock(LockKey.forResource(Account.class, "6")))
                    .hasMessageContaining("order inversion");
            }
            try (LockHandle afterRelease = secondTemplate.tryLock(key.value(), immediate).orElseThrow()) {
                assertThat(afterRelease.isHeld()).isTrue();
            }
        } finally {
            first.shutdown();
            second.shutdown();
        }
    }

    @Test
    void readAndWriteSharePhysicalLockButFairAndNormalDoNot() {
        RedissonClient first = Redisson.create(config());
        RedissonClient second = Redisson.create(config());
        try {
            KeySpace space = new KeySpace("lock-rw-it:" + UUID.randomUUID());
            var firstTemplate = new RedissonLockTemplate(first, space);
            var secondTemplate = new RedissonLockTemplate(second, space);
            LockKey key = LockKey.named(Transfer.class, "global");
            LockOptions read = new LockOptions(LockType.READ, Duration.ZERO, Duration.ZERO);
            LockOptions write = new LockOptions(LockType.WRITE, Duration.ZERO, Duration.ZERO);
            try (LockHandle firstReader = firstTemplate.tryLock(key.value(), read).orElseThrow();
                LockHandle secondReader = secondTemplate.tryLock(key.value(), read).orElseThrow()) {
                assertThat(secondTemplate.tryLock(key.value(), write)).isEmpty();
            }
            try (LockHandle writer = secondTemplate.tryLock(key.value(), write).orElseThrow()) {
                assertThat(writer.isHeld()).isTrue();
            }
        } finally {
            first.shutdown();
            second.shutdown();
        }
    }
}
