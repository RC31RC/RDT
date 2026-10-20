package io.github.rdt.id;

/** ID 依赖共享的持久化 Redis 状态；删除计数器或恢复旧数据可能导致 ID 重复发放。 */
public interface DistributedIdGenerator {
    /**
     * 按配置的模式分配 ID。
     * @param sequence 逻辑业务序列名
     * @return 本次唯一分配值
     */
    long next(String sequence);

    /**
     * 在所选模式的独立命名空间中分配 ID。
     * @param sequence 逻辑业务序列名
     * @param mode 号段模式或原子递增模式
     * @return 分配结果
     */
    long next(String sequence, IdMode mode);

    /** Allocates from the atomic sequence without going below an inclusive imported-data minimum. */
    long nextAtLeast(String sequence, long minimum);

    /**
     * 组合业务前缀、可选日期和前导零补齐的序列值。
     * 序列不会按日重置，以避免时钟回拨造成重复。
     * @param sequence 逻辑序列名
     * @param prefix 业务前缀字面值
     * @param datePattern DateTimeFormatter 格式，空字符串表示不包含日期
     * @param width 序列最小宽度，超长时不截断
     * @return 格式化后的 ID
     */
    String nextFormatted(String sequence, String prefix, String datePattern, int width);
}
