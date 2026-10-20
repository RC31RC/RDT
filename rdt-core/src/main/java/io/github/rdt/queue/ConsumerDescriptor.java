package io.github.rdt.queue;

import io.github.rdt.consistency.InboxMode;

/** Local subscription policy. It is never accepted from an incoming message. */
public abstract class ConsumerDescriptor {
    private static final ConsumerDescriptor DEFAULTS = new ConsumerDescriptor() { };

    public static ConsumerDescriptor defaults() { return DEFAULTS; }
    public String displayName() { return ""; }
    public int concurrency() { return -1; }
    public int orderedPartitions() { return 0; }
    public InboxMode inbox() { return InboxMode.INHERIT; }
    public QueueOptions options() { return QueueOptions.defaults(); }
}
