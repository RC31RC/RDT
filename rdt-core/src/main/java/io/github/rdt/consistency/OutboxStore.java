package io.github.rdt.consistency;

import java.time.Duration;
import java.util.List;

public interface OutboxStore {
    /** Joins the active writable business transaction; must never independently commit. */
    String append(OutboxAppend append);
    /** Short transaction commits before records are returned. Expired claims are rebound atomically. */
    List<OutboxRecord> claim(String namespace, String owner, int limit, Duration lease);
    void markPublished(List<OutboxRecord> records);
    void reschedule(OutboxRecord record, Duration delay, String error);
    void markDead(OutboxRecord record, String error);
    OutboxStats stats(String namespace);
}
