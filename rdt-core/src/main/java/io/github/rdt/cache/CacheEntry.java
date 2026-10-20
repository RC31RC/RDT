package io.github.rdt.cache;

/**
 * 区分缓存未命中和已缓存的 null。
 * @param <T> 值类型
 */
public sealed interface CacheEntry<T> permits CacheEntry.Hit, CacheEntry.Miss {
    /**
     * 缓存命中，包括空值缓存命中。
     * @param value 可为 null 的缓存值
     * @param <T> 值类型
     */
    record Hit<T>(T value) implements CacheEntry<T> { }

    /**
     * 不存在或已过期的缓存条目。
     * @param <T> 预期值类型
     */
    record Miss<T>() implements CacheEntry<T> { }
}
