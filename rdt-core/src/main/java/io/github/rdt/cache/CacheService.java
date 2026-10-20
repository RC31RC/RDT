package io.github.rdt.cache;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

/** 显式类型的可丢弃 JSON 桶缓存；共享状态、Map 和计数器使用独立存储接口。 */
public interface CacheService {
    CacheService region(String name, CachePolicy policy);
    CacheService region(String name);

    /**
     * 读取桶缓存，区分未命中和已缓存的 null。
     * @param key 逻辑键
     * @param type 受信任的目标类型，支持参数化泛型
     * @param <T> 目标值类型
     * @return 命中或未命中结果
     */
    <T> CacheEntry<T> get(String key, Type type);

    /**
     * 写入桶缓存；存在一级缓存订阅者时发布失效事件。
     * @param key 逻辑键
     * @param value 可为 null 的值
     * @param ttl 大于零的 TTL，null 表示使用默认配置
     */
    void put(String key, Object value, Duration ttl);

    /**
     * 删除桶缓存；存在一级缓存订阅者时发布失效事件。
     * @param key 逻辑键
     */
    void evict(String key);

    /**
     * 在看门狗分布式锁内二次检查缓存，未命中时执行回源。
     * @param key 逻辑键
     * @param type 目标类型
     * @param ttl 大于零的 TTL，null 表示使用默认值
     * @param cacheNull 是否缓存回源得到的 null
     * @param loader 同步业务回源函数
     * @param <T> 值类型
     * @return 缓存命中值或回源结果
     * @throws Exception 原始回源异常
     */
    <T> T getOrLoad(String key, Type type, Duration ttl, boolean cacheNull, Callable<T> loader) throws Exception;

    /**
     * 批量读取桶缓存，支持可选一级缓存，不保证多键原子快照。
     * @param keys 逻辑键集合
     * @param type 目标值类型
     * @param <T> 值类型
     * @return 每个请求键对应的命中或未命中结果
     */
    <T> Map<String, CacheEntry<T>> getAll(Collection<String> keys, Type type);

    /**
     * 先流水线写入，再按一级缓存订阅情况发布失效事件；断线时可能仅部分完成。
     * @param values 逻辑键及可为 null 的值
     * @param ttl TTL，null 表示使用默认值
     */
    void putAll(Map<String, ?> values, Duration ttl);
}
