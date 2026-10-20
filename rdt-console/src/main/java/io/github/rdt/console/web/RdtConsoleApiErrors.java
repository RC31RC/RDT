package io.github.rdt.console.web;

import io.github.rdt.exception.RdtErrorCode;
import io.github.rdt.exception.RdtException;
import java.util.Map;
import org.redisson.client.RedisException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

/** 将受控的 RDT 参数和 Redis 错误映射为不含堆栈的 HTTP 响应。 */
@RestControllerAdvice
public class RdtConsoleApiErrors {
    /** @param error RDT 异常 @return API 错误响应 */
    @ExceptionHandler(RdtException.class)
    public ResponseEntity<Map<String, String>> rdtError(RdtException error) {
        HttpStatus status = switch (error.code()) {
            case QUEUE_CAPACITY -> HttpStatus.TOO_MANY_REQUESTS;
            case MESSAGE_TOO_LARGE -> HttpStatus.PAYLOAD_TOO_LARGE;
            case LOCK_TIMEOUT -> HttpStatus.CONFLICT;
            case CONFIGURATION, EXPRESSION -> HttpStatus.BAD_REQUEST;
            default -> HttpStatus.SERVICE_UNAVAILABLE;
        };
        return ResponseEntity.status(status).body(Map.of("code", error.code().name(), "message", error.getMessage()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, String>> statusError(ResponseStatusException error) {
        return ResponseEntity.status(error.getStatusCode()).body(Map.of("code", "CONSOLE_STATE",
                "message", error.getReason() == null ? "Console request rejected" : error.getReason()));
    }

    @ExceptionHandler(RedisException.class)
    public ResponseEntity<Map<String, String>> redisError(RedisException error) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("code", "REDIS_OPERATION",
                "message", "Redis request failed; check the selected runtime connection and ACL permissions"));
    }

    /** @param error 参数错误 @return API 错误响应 */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> argumentError(IllegalArgumentException error) {
        return ResponseEntity.badRequest().body(Map.of("code", RdtErrorCode.CONFIGURATION.name(),
                "message", error.getMessage()));
    }
}
