package io.github.rdt.queue;

/** Message transport and local concurrency contracts; all handlers are synchronous. */
public enum QueueMode {
    /** Shared consumer group; one batched reader per ordinary subscription, concurrency bounds local handlers. */
    STREAM,
    /** Online Topic only; local callbacks are bounded by listener/global capacity and may be dropped. */
    BROADCAST,
    /** One durable group per stable instance; one ordinary reader with bounded concurrent local handlers. */
    RELIABLE_BROADCAST
}
