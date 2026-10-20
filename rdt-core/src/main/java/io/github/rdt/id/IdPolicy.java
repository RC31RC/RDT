package io.github.rdt.id;

import io.github.rdt.support.RdtSupport;
import java.time.ZoneId;

/**
 * ID 分配默认配置。
 * @param mode 分配模式
 * @param start 起始 ID，至少为一
 * @param allocationSize 每个号段的 ID 数量
 * @param zone 日期格式化时区，各节点必须一致
 */
public record IdPolicy(IdMode mode, long start, long allocationSize, ZoneId zone) {
    /** 校验 ID 分配配置。 */
    public IdPolicy {
        RdtSupport.require(mode != null && zone != null && start > 0 && allocationSize > 0, "Invalid ID policy");
    }
}
