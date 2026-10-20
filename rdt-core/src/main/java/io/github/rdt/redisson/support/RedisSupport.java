package io.github.rdt.redisson.support;

import io.github.rdt.exception.RdtRedisException;
import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;
import org.redisson.client.RedisTimeoutException;

public final class RedisSupport {
    private RedisSupport() { }
    /**
     * 转换基础设施运行时异常；不得使用此方法包装业务回调。
     * @param operation 用于诊断的操作名称
     * @param action Redis 操作
     * @param <T> 结果类型
     * @return 操作结果
     */
    public static <T> T redis(String operation, Supplier<T> action) {
        try {
            return action.get();
        } catch (RuntimeException e) {
            throw failure(operation, e);
        }
    }

    /** Translate once at an infrastructure boundary, preserving an already classified failure. */
    public static RdtException failure(String operation, RuntimeException cause) {
        if (cause instanceof RdtException rdt) return rdt;
        boolean timeout = isRedisTimeout(cause);
        return new RdtRedisException(timeout ? RdtErrorCode.REDIS_TIMEOUT : RdtErrorCode.REDIS_OPERATION,
                operation + (timeout ? " timed out" : " failed"), cause);
    }

    /**
     * 判断异常链中是否存在 Redisson Redis 超时。
     *
     * <p>Redisson 可能直接抛出超时，也可能把它包在其他运行时异常中。
     * 统一识别后，调度器才能把基础设施故障与业务执行失败区分开。</p>
     *
     * @param error 待检查异常
     * @return 是否为 Redis 超时
     */
    public static boolean isRedisTimeout(Throwable error) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        while (current != null && visited.add(current)) {
            if (current instanceof RedisTimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * 转换无返回值 Redis 操作的异常。
     * @param operation 用于诊断的参数名称
     * @param action Redis 操作
     */
    public static void redisRun(String operation, Runnable action) {
        redis(operation, () -> { action.run(); return null; });
    }
}
