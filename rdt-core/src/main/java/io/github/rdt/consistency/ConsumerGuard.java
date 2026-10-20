package io.github.rdt.consistency;

import io.github.rdt.queue.Message;
import io.github.rdt.queue.MessageHandler;

/** Synchronous transaction boundary; returning successfully permits transport ACK. */
public interface ConsumerGuard {
    /** Whether this guard atomically commits Inbox identity with synchronous business writes. */
    boolean supportsInbox();
    boolean enabledByDefault();
    void consume(InboxScope scope, Message message, MessageHandler business) throws Exception;
}
