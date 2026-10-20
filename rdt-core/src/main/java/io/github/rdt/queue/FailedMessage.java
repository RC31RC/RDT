package io.github.rdt.queue;

import io.github.rdt.support.RdtSupport;
import java.util.Objects;

/** Failure budget excludes process crashes. Broadcast identity includes the receiving instance. */
public record FailedMessage(String destination, String group, Message message, long failureCount,
                            String error, String failedAt, QueueMode mode, String instanceId,
                            String originalMessageId, String firstFailedAt) {
    public FailedMessage {
        Objects.requireNonNull(mode); Objects.requireNonNull(instanceId);
        Objects.requireNonNull(originalMessageId); Objects.requireNonNull(firstFailedAt);
    }
    public String field() {
        return field(group, message.id(), mode == QueueMode.BROADCAST ? instanceId : "");
    }
    public static String field(String group, String messageId, String broadcastInstance) {
        String common = RdtSupport.component(group) + ":" + RdtSupport.component(messageId);
        return broadcastInstance == null || broadcastInstance.isBlank() ? common : common + ":" + RdtSupport.component(broadcastInstance);
    }
    public static FailedMessage stream(String destination, String group, Message message, long failures, String error, String at) {
        return new FailedMessage(destination,group,message,failures,error,at,QueueMode.STREAM,"",message.id(),at);
    }
}
