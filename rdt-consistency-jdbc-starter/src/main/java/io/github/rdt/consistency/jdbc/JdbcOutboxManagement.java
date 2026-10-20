package io.github.rdt.consistency.jdbc;

import io.github.rdt.support.RdtSupport;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Namespace-scoped administration; DEAD records require an explicit, versioned decision. */
public final class JdbcOutboxManagement {
    private static final String COLUMNS = "id,destination,queue_mode,status,failure_count,version,created_at,published_at,dead_at,last_error";
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final String namespace;
    private final String table;

    public JdbcOutboxManagement(DataSource source, PlatformTransactionManager manager, JdbcDialect dialect,
                                String namespace, String schema) {
        RdtSupport.require(namespace != null && !namespace.isBlank(), "Outbox namespace is required");
        RdtSupport.require(schema != null && (schema.isEmpty() || schema.matches("[A-Za-z_][A-Za-z0-9_]*")),
                "Invalid SQL schema identifier");
        this.jdbc = new JdbcTemplate(source);
        jdbc.setQueryTimeout(5);
        this.transaction = new TransactionTemplate(manager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setTimeout(5);
        this.namespace = namespace;
        this.table = dialect.table(schema, "rdt_outbox");
    }

    public enum Status { PENDING, CLAIMED, PUBLISHED, DEAD }

    /** Keyset pages avoid COUNT(*) and unbounded OFFSET scans; payloads are fetched individually. */
    public Page page(Status status, String cursor, int limit) {
        RdtSupport.require(status != null && limit > 0 && limit <= 200, "Outbox page limit must be 1..200");
        var parameters = new ArrayList<Object>();
        parameters.add(namespace);
        parameters.add(status.name());
        String after = "";
        if (cursor != null && !cursor.isEmpty()) {
            RdtSupport.require(cursor.length() <= 256, "Invalid Outbox cursor");
            String[] parts = RdtSupport.decodeComponent(cursor).orElseThrow(
                    () -> new IllegalArgumentException("Invalid Outbox cursor")).split("\\|", -1);
            RdtSupport.require(parts.length == 2, "Invalid Outbox cursor");
            Instant time;
            try {
                time = Instant.parse(parts[0]);
            } catch (DateTimeParseException invalid) {
                throw new IllegalArgumentException("Invalid Outbox cursor", invalid);
            }
            validateId(parts[1]);
            after = " AND (created_at<? OR (created_at=? AND id<?))";
            parameters.add(Timestamp.from(time));
            parameters.add(Timestamp.from(time));
            parameters.add(parts[1]);
        }
        parameters.add(limit + 1);
        String sql = "SELECT " + COLUMNS + " FROM " + table + " WHERE namespace=? AND status=?" + after
                + " ORDER BY created_at DESC,id DESC LIMIT ?";
        return JdbcSupport.execute("page Outbox", () -> {
            List<Entry> rows = jdbc.query(sql, (rs, index) -> entry(rs), parameters.toArray());
            boolean more = rows.size() > limit;
            List<Entry> items = List.copyOf(rows.subList(0, Math.min(rows.size(), limit)));
            Entry last = items.isEmpty() ? null : items.getLast();
            String next = more ? RdtSupport.component(last.createdAt() + "|" + last.id()) : null;
            return new Page(items, next);
        });
    }

    public Optional<Detail> detail(String id) {
        validateId(id);
        return JdbcSupport.execute("read Outbox", () -> jdbc.query("SELECT " + COLUMNS + ",envelope FROM " + table
                        + " WHERE namespace=? AND id=?", (rs, index) -> new Detail(entry(rs), rs.getString("envelope")),
                namespace, id).stream().findFirst());
    }

    /** Preserve the envelope/message ID; grant a new retry budget and let the ordinary relay publish. */
    public boolean retryDead(String id, long expectedVersion, boolean confirmed) {
        validateAction(id, expectedVersion, confirmed);
        return JdbcSupport.execute("retry DEAD Outbox", () -> transaction.execute(status -> jdbc.update(
                "UPDATE " + table + " SET status='PENDING',version=version+1,failure_count=0,available_at=CURRENT_TIMESTAMP,"
                        + "dead_at=NULL,last_error=NULL,published_at=NULL,owner=NULL,claim_token=NULL,claim_until=NULL"
                        + " WHERE namespace=? AND id=? AND status='DEAD' AND version=?",
                namespace, id, expectedVersion) == 1));
    }

    /** Only the exact DEAD version reviewed by the operator can be permanently discarded. */
    public boolean deleteDead(String id, long expectedVersion, boolean confirmed) {
        validateAction(id, expectedVersion, confirmed);
        return JdbcSupport.execute("delete DEAD Outbox", () -> transaction.execute(status -> jdbc.update(
                "DELETE FROM " + table + " WHERE namespace=? AND id=? AND status='DEAD' AND version=?",
                namespace, id, expectedVersion) == 1));
    }

    private void validateAction(String id, long version, boolean confirmed) {
        validateId(id);
        RdtSupport.require(version >= 0 && confirmed, "Outbox action requires a version and explicit confirmation");
    }

    private void validateId(String id) {
        RdtSupport.require(id != null && id.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"),
                "Invalid Outbox ID");
    }

    private Entry entry(ResultSet row) throws SQLException {
        return new Entry(row.getString("id"), row.getString("destination"), row.getString("queue_mode"),
                Status.valueOf(row.getString("status")), row.getInt("failure_count"), row.getLong("version"),
                time(row, "created_at"), time(row, "published_at"), time(row, "dead_at"), row.getString("last_error"));
    }

    private Instant time(ResultSet row, String column) throws SQLException {
        Timestamp value = row.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    public record Page(List<Entry> items, String nextCursor) { }
    public record Entry(String id, String destination, String queueMode, Status status, int failureCount, long version,
                        Instant createdAt, Instant publishedAt, Instant deadAt, String lastError) { }
    public record Detail(Entry entry, String envelope) { }
}
