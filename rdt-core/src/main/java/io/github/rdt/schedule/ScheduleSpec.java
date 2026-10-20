package io.github.rdt.schedule;

import io.github.rdt.support.RdtSupport;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.stream.Collectors;

/** 持久化调度定义；任务 ID 保持稳定，定义变更由任务策略决定拒绝或更新。 */
public sealed interface ScheduleSpec permits ScheduleSpec.FixedRate, ScheduleSpec.FixedDelay, ScheduleSpec.Cron {
    /**
     * 按时钟固定频率触发；合并错过的周期，不集中补偿执行。
     * @param period 时间间隔
     * @param initialDelay 首次集群注册后的延迟
     */
    record FixedRate(Duration period, Duration initialDelay) implements ScheduleSpec {
        /** 校验调度时长。 */
        public FixedRate {
            RdtSupport.positive(period, "fixed rate");
            RdtSupport.require(initialDelay != null && !initialDelay.isNegative(), "Invalid initial delay");
        }
    }

    /**
     * 集群范围内从本次完成到下次开始的固定延迟。
     * @param delay 完成后的延迟
     * @param initialDelay 首次集群注册后的延迟
     */
    record FixedDelay(Duration delay, Duration initialDelay) implements ScheduleSpec {
        /** 校验调度时长。 */
        public FixedDelay {
            RdtSupport.positive(delay, "fixed delay");
            RdtSupport.require(initialDelay != null && !initialDelay.isNegative(), "Invalid initial delay");
        }
    }

    /**
     * Spring cron 调度。
     *
     * <p>支持常见的五字段表达式（分、时、日、月、星期）和 Spring 六字段表达式
     * （秒、分、时、日、月、星期）。五字段表达式会自动补充秒字段 {@code 0}，
     * 六字段表达式保持原有语义。Quartz 七字段表达式不在 RDT 的支持范围内。</p>
     *
     * @param expression 五字段或六字段 cron 表达式
     * @param zone 明确指定的时区
     */
    record Cron(String expression, ZoneId zone) implements ScheduleSpec {
        /** 规范化字段数量和空白，具体语法由 Spring 调度器在注册时校验。 */
        public Cron {
            expression = normalize(expression);
            RdtSupport.require(zone != null, "Invalid cron schedule");
        }

        /**
         * 将五字段 cron 转换为 Spring 六字段 cron，并统一字段间空白。
         *
         * @param expression 原始 cron 表达式
         * @return 可交给 Spring {@code CronExpression} 解析的六字段表达式
         */
        private static String normalize(String expression) {
            RdtSupport.require(expression != null && !expression.isBlank(), "Invalid cron schedule");
            String normalized = Arrays.stream(expression.strip().split("\\s+"))
                    .collect(Collectors.joining(" "));
            String[] fields = normalized.split(" ");
            if (fields.length == 5) {
                return "0 " + normalized;
            }
            RdtSupport.require(fields.length == 6,
                    "Cron must contain five fields (minute hour day month weekday) or six fields "
                            + "(second minute hour day month weekday)");
            return normalized;
        }
    }
}
