package io.github.rdt.consistency.jdbc;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.consistency.OutboxAppend;
import io.github.rdt.consistency.OutboxRecord;
import io.github.rdt.consistency.OutboxStats;
import io.github.rdt.consistency.OutboxStore;
import io.github.rdt.queue.Message;
import io.github.rdt.queue.QueueMode;
import io.github.rdt.support.JsonCodec;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcOutboxStore implements OutboxStore {
    private final JdbcTemplate jdbc;
    private final DataSource source;
    private final TransactionTemplate shortTransaction;
    private final JdbcDialect dialect;
    private final JsonCodec json;
    private final String table;
    public JdbcOutboxStore(DataSource source, PlatformTransactionManager manager, JdbcDialect dialect,
                           JsonCodec json, String schema) {
        this.source = source; this.jdbc = new JdbcTemplate(source); this.dialect = dialect; this.json = json;
        table = dialect.table(schema, "rdt_outbox");
        shortTransaction = new TransactionTemplate(manager);
        shortTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }
    @Override
    public String append(OutboxAppend append) {
        return JdbcSupport.execute("append JdbcOutboxStore", () -> {
            if (!TransactionSynchronizationManager.isActualTransactionActive()
                    || TransactionSynchronizationManager.isCurrentTransactionReadOnly()
                    || !TransactionSynchronizationManager.hasResource(source)) {
                throw new RdtConfigurationException("Outbox must join the writable application transaction on the configured DataSource");
            }
            String id = UUID.randomUUID().toString();
            jdbc.update("INSERT INTO " + table + " (id,namespace,destination,queue_mode,envelope,status) VALUES (?,?,?,?,?,'PENDING')",
                    id, append.namespace(), append.destination(), append.queueMode().name(), json.write(append.message()));
            return id;
        
        });
    }
    @Override
    public List<OutboxRecord> claim(String namespace, String owner, int limit, Duration lease) {
        return JdbcSupport.execute("claim JdbcOutboxStore", () -> {
            if (limit < 1 || limit > 500 || lease.isZero() || lease.isNegative()) throw new RdtConfigurationException("Invalid claim budget");
            return shortTransaction.execute(status -> {
                List<OutboxRecord> rows = jdbc.query("SELECT id,namespace,destination,queue_mode,envelope,failure_count FROM " + table
                        + " WHERE namespace=? AND ((status='PENDING' AND available_at<=CURRENT_TIMESTAMP) OR (status='CLAIMED' AND claim_until<=CURRENT_TIMESTAMP)) ORDER BY created_at,id LIMIT ? FOR UPDATE SKIP LOCKED",
                        (rs, index) -> new OutboxRecord(rs.getString("id"), rs.getString("namespace"), rs.getString("destination"),
                                QueueMode.valueOf(rs.getString("queue_mode")), json.read(rs.getString("envelope"), Message.class),
                                rs.getInt("failure_count"), UUID.randomUUID().toString()), namespace, limit);
                for (OutboxRecord row : rows) {
                    jdbc.update("UPDATE " + table + " SET status='CLAIMED',owner=?,claim_token=?,claim_until=" + dialect.plusMillis()
                            + " WHERE namespace=? AND id=?", owner, row.claimToken(), lease.toMillis(), namespace, row.id());
                }
                return rows;
            });
        
        });
    }
    @Override
    public void markPublished(List<OutboxRecord> rows) {
        JdbcSupport.execute("markPublished JdbcOutboxStore", () -> {
            if (rows.isEmpty()) return;
            shortTransaction.executeWithoutResult(status -> jdbc.batchUpdate("UPDATE " + table
                    + " SET status='PUBLISHED',published_at=CURRENT_TIMESTAMP,claim_until=NULL,owner=NULL,claim_token=NULL WHERE namespace=? AND id=? AND status='CLAIMED' AND claim_token=? AND claim_until>CURRENT_TIMESTAMP",
                    rows, rows.size(), (ps, row) -> { ps.setString(1, row.namespace()); ps.setString(2, row.id()); ps.setString(3, row.claimToken()); }));
        
        });
    }
    @Override
    public void reschedule(OutboxRecord row, Duration delay, String error) {
        JdbcSupport.execute("reschedule JdbcOutboxStore", () -> {
            shortTransaction.executeWithoutResult(status -> jdbc.update("UPDATE " + table
                    + " SET status='PENDING',failure_count=failure_count+1,available_at=" + dialect.plusMillis()
                    + ",last_error=?,claim_until=NULL,owner=NULL,claim_token=NULL WHERE namespace=? AND id=? AND status='CLAIMED' AND claim_token=? AND claim_until>CURRENT_TIMESTAMP",
                    delay.toMillis(), bounded(error), row.namespace(), row.id(), row.claimToken()));
        
        });
    }
    @Override
    public void markDead(OutboxRecord row, String error) {
        JdbcSupport.execute("markDead JdbcOutboxStore", () -> {
            shortTransaction.executeWithoutResult(status -> jdbc.update("UPDATE " + table
                    + " SET status='DEAD',failure_count=failure_count+1,version=version+1,dead_at=CURRENT_TIMESTAMP,last_error=?,claim_until=NULL,owner=NULL,claim_token=NULL WHERE namespace=? AND id=? AND status='CLAIMED' AND claim_token=? AND claim_until>CURRENT_TIMESTAMP",
                    bounded(error), row.namespace(), row.id(), row.claimToken()));
        
        });
    }
    @Override
    public OutboxStats stats(String namespace) {
        return JdbcSupport.execute("stats JdbcOutboxStore", () -> {
            var counts = jdbc.queryForList("SELECT status,COUNT(*) total FROM " + table + " WHERE namespace=? GROUP BY status", namespace);
            long pending=0,claimed=0,published=0,dead=0;
            for (var row : counts) {
                long value=((Number)row.get("total")).longValue();
                switch (row.get("status").toString()) {
                    case "PENDING" -> pending=value; case "CLAIMED" -> claimed=value;
                    case "PUBLISHED" -> published=value; case "DEAD" -> dead=value;
                    default -> throw new RdtConfigurationException("Unknown outbox state");
                }
            }
            return new OutboxStats(pending,claimed,published,dead);
        
        });
    }
    public int cleanup(String namespace, Duration retention, int limit) {
        return JdbcSupport.execute("cleanup JdbcOutboxStore", () -> {
            return shortTransaction.execute(status -> {
                List<String> ids = jdbc.queryForList("SELECT id FROM " + table
                        + " WHERE namespace=? AND status='PUBLISHED' AND published_at<" + dialect.plusMillis()
                        + " ORDER BY published_at LIMIT ? FOR UPDATE SKIP LOCKED", String.class, namespace, -retention.toMillis(), limit);
                if (ids.isEmpty()) return 0;
                var parameters = new ArrayList<Object>();
                parameters.add(namespace);
                parameters.addAll(ids);
                return jdbc.update("DELETE FROM " + table + " WHERE namespace=? AND status='PUBLISHED' AND id IN ("
                        + String.join(",", Collections.nCopies(ids.size(), "?")) + ")", parameters.toArray());
            });
        
        });
    }
    private String bounded(String error) { return error.substring(0, Math.min(error.length(), 2000)); }
}
