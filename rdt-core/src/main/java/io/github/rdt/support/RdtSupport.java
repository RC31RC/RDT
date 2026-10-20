package io.github.rdt.support;

import io.github.rdt.exception.RdtConfigurationException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;

/** 参数校验、无歧义键编码和异常转换。 */
public final class RdtSupport {
    private RdtSupport() { }

    /**
     * 校验必要条件。
     * @param condition 必须满足的条件
     * @param message 错误说明
     */
    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new RdtConfigurationException(message);
        }
    }

    /**
     * 校验毫秒精度下大于零的时长。
     * @param value 时长
     * @param name 用于诊断的参数名称
     * @return 传入的时长
     */
    public static Duration positive(Duration value, String name) {
        require(value != null && value.toMillis() > 0, name + " must be at least 1ms");
        return value;
    }

    /**
     * 编码逻辑名称，避免注入 Redis 哈希标签。
     * @param value 非空白逻辑名称
     * @return URL 安全且可逆的编码片段
     */
    public static String component(String value) {
        require(value != null && !value.isBlank(), "Key component must not be blank");
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 解码由 {@link #component(String)} 生成的逻辑名称。
     *
     * @param value 编码片段
     * @return 解码结果；输入不是合法编码时返回空
     */
    public static Optional<String> decodeComponent(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

}
