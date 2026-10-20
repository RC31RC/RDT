package io.github.rdt.consistency.jdbc;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.consistency.InboxScope;
import io.github.rdt.consistency.InboxStore;
import java.time.Duration;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcInboxStore implements InboxStore {
    private final JdbcTemplate jdbc;
    private final DataSource source;
    private final JdbcDialect dialect;
    private final String table;
    private final TransactionTemplate cleanupTransaction;
    public JdbcInboxStore(DataSource source, PlatformTransactionManager manager, JdbcDialect dialect, String schema) {
        this.source=source; jdbc=new JdbcTemplate(source); this.dialect=dialect;
        table=dialect.table(schema,"rdt_inbox"); cleanupTransaction=new TransactionTemplate(manager);
    }
    @Override
    public boolean register(InboxScope scope) {
        return JdbcSupport.execute("register JdbcInboxStore", () -> {
            if (!TransactionSynchronizationManager.isActualTransactionActive() || !TransactionSynchronizationManager.hasResource(source)
                    || TransactionSynchronizationManager.isCurrentTransactionReadOnly()) {
                throw new RdtConfigurationException("Inbox requires the writable application transaction and the same DataSource as business writes");
            }
            if (dialect == JdbcDialect.MYSQL) {
                String marker=UUID.randomUUID().toString();
                jdbc.update(dialect.insertInbox(table),scope.namespace(),scope.messageId(),scope.consumerGroup(),marker);
                return marker.equals(jdbc.queryForObject("SELECT claim_marker FROM " + table
                        + " WHERE namespace=? AND message_id=? AND consumer_group=?",String.class,scope.namespace(),scope.messageId(),scope.consumerGroup()));
            }
            if (dialect == JdbcDialect.H2) {
                jdbc.queryForObject("SELECT id FROM " + table.replace("rdt_inbox", "rdt_inbox_guard") + " WHERE id=? FOR UPDATE",
                        Integer.class, Math.floorMod(scope.hashCode(),64));
            }
            return jdbc.update(dialect.insertInbox(table),scope.namespace(),scope.messageId(),scope.consumerGroup()) == 1;
        
        });
    }
    public int cleanup(String namespace, Duration retention, int limit) {
        return JdbcSupport.execute("cleanup JdbcInboxStore", () -> {
            return cleanupTransaction.execute(status -> {
                var ids=jdbc.query("SELECT message_id,consumer_group FROM " + table + " WHERE namespace=? AND consumed_at<"
                        + dialect.plusMillis() + " ORDER BY consumed_at LIMIT ? FOR UPDATE SKIP LOCKED",
                        (rs,i) -> new InboxScope(namespace,rs.getString(1),rs.getString(2)),namespace,-retention.toMillis(),limit);
                int count=0;
                for (InboxScope id:ids) count+=jdbc.update("DELETE FROM " + table
                        + " WHERE namespace=? AND message_id=? AND consumer_group=?",namespace,id.messageId(),id.consumerGroup());
                return count;
            });
        
        });
    }
}
