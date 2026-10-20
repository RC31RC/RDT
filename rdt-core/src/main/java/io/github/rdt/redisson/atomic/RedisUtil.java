package io.github.rdt.redisson.atomic;

import io.github.rdt.redisson.support.RedisSupport;
import io.github.rdt.support.KeySpace;
import io.github.rdt.support.RdtSupport;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/** 可注入、带命名空间的原子操作工具，不使用静态全局客户端，不提供无约束清库接口。 */
public final class RedisUtil {
    private final RedissonClient client;
    private final KeySpace keys;

    /**
     * 创建原子操作工具。
     * @param client 共享客户端
     * @param keys 命名空间
     */
    public RedisUtil(RedissonClient client, KeySpace keys) {
        this.client = client;
        this.keys = keys;
    }

    /**
     * 对长整型值执行原子递增。
     * @param key 逻辑键
     * @param delta 可正可负的增量
     * @return 新值
     */
    public long addLong(String key, long delta) {
        return RedisSupport.redis("add long", () -> client.getAtomicLong(keys.key("atomic-long", key)).addAndGet(delta));
    }

    /**
     * 原子比较并更新长整型值。
     * @param key 逻辑键
     * @param expected 预期值
     * @param update 新值
     * @return 更新是否成功
     */
    public boolean compareAndSet(String key, long expected, long update) {
        return RedisSupport.redis("compare and set", () -> client.getAtomicLong(keys.key("atomic-long", key)).compareAndSet(expected, update));
    }

    /**
     * 原子累加近似浮点值，不适用于金额计算。
     * @param key 逻辑键
     * @param delta 有限浮点增量
     * @return 新值
     */
    public double addDouble(String key, double delta) {
        RdtSupport.require(Double.isFinite(delta), "Delta must be finite");
        return RedisSupport.redis("add double", () -> client.getAtomicDouble(keys.key("atomic-double", key)).addAndGet(delta));
    }

    /**
     * 更新一个二进制位。
     * @param key 逻辑位图名称
     * @param index 非负位索引
     * @param value 新的二进制位状态
     */
    public void setBit(String key, long index, boolean value) {
        RdtSupport.require(index >= 0, "Bit index must not be negative");
        RedisSupport.redisRun("set bit", () -> client.getBitSet(keys.key("bits", key)).set(index, value));
    }

    /**
     * 统计值为一的位数量。
     * @param key 逻辑位图名称
     * @return 值为一的位数量
     */
    public long bitCount(String key) {
        return RedisSupport.redis("count bits", () -> client.getBitSet(keys.key("bits", key)).cardinality());
    }

    /**
     * 向 HyperLogLog 添加观测值。
     * @param key 逻辑估算器名称
     * @param value 观测到的标识
     * @return 内部寄存器是否发生变化
     */
    public boolean addDistinct(String key, String value) {
        return RedisSupport.redis("add distinct", () -> client.<String>getHyperLogLog(keys.key("hll", key), StringCodec.INSTANCE).add(value));
    }

    /**
     * 估算去重后的基数。
     * @param key 逻辑估算器名称
     * @return 近似去重数量
     */
    public long distinctCount(String key) {
        return RedisSupport.redis("count distinct", () -> client.getHyperLogLog(keys.key("hll", key), StringCodec.INSTANCE).count());
    }

    /**
     * 仅初始化一次布隆过滤器，已有过滤器必须使用一致的参数定义。
     * @param key 逻辑过滤器名称
     * @param expectedInsertions 大于零的预期元素数量
     * @param falseProbability 介于零和一之间的误判概率
     * @return 本次初始化是否创建了过滤器
     */
    public boolean initializeBloom(String key, long expectedInsertions, double falseProbability) {
        RdtSupport.require(expectedInsertions > 0 && falseProbability > 0 && falseProbability < 1, "Invalid Bloom parameters");
        return RedisSupport.redis("initialize bloom", () -> bloom(key).tryInit(expectedInsertions, falseProbability));
    }

    /**
     * 向已初始化的布隆过滤器添加元素。
     * @param key 逻辑过滤器名称
     * @param value 标识
     * @return 过滤器位数据是否发生变化
     */
    public boolean bloomAdd(String key, String value) {
        return RedisSupport.redis("add bloom", () -> bloom(key).add(value));
    }

    /**
     * 判断元素是否可能存在，返回 true 不能证明元素实际存在。
     * @param key 逻辑过滤器名称
     * @param value 标识
     * @return 标识是否可能存在
     */
    public boolean bloomMightContain(String key, String value) {
        return RedisSupport.redis("query bloom", () -> bloom(key).contains(value));
    }

    private RBloomFilter<String> bloom(String key) {
        return client.getBloomFilter(keys.key("bloom", key), StringCodec.INSTANCE);
    }
}
