package io.github.rdt.consistency.jdbc;

import io.github.rdt.exception.RdtConfigurationException;
import io.github.rdt.queue.FailedMessage;
import io.github.rdt.queue.FailedMessageStore;
import io.github.rdt.support.JsonCodec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Optional durable DLQ adapter, isolated from the Redis-only starter and consumer transaction. */
public final class JdbcFailedMessageStore implements FailedMessageStore {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final JdbcDialect dialect;
    private final JsonCodec json;
    private final String namespace;
    private final String table;
    public JdbcFailedMessageStore(DataSource source,PlatformTransactionManager manager,JdbcDialect dialect,
                                  JsonCodec json,String namespace,String schema) {
        jdbc=new JdbcTemplate(source);transaction=new TransactionTemplate(manager);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.dialect=dialect;this.json=json;this.namespace=namespace;table=dialect.table(schema,"rdt_failed_message");
    }
    private String id(FailedMessage failure) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(
                    json.write(List.of(namespace,failure.destination(),failure.field())).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException unavailable) { throw new RdtConfigurationException("SHA-256 unavailable", unavailable); }
    }
    @Override
    public void save(FailedMessage failure) {
        JdbcSupport.execute("save JdbcFailedMessageStore", () -> {
            String suffix=switch(dialect) {
                case POSTGRESQL -> " ON CONFLICT (id) DO NOTHING";
                case MYSQL -> " ON DUPLICATE KEY UPDATE id=id";
                case H2 -> "";
            };
            transaction.executeWithoutResult(status -> {
                String sql="INSERT INTO " + table + " (id,namespace,destination,failure_key,envelope,mode,instance_id,original_message_id,first_failed_at,failure_count) VALUES (?,?,?,?,?,?,?,?,?,?)"+suffix;
                if(dialect==JdbcDialect.H2) sql="MERGE INTO " + table + " KEY(id) VALUES (?,?,?,?,?,?,?,?,?,?)";
                jdbc.update(sql,id(failure),namespace,failure.destination(),failure.field(),json.write(failure),failure.mode().name(),
                        failure.instanceId(),failure.originalMessageId(),failure.firstFailedAt(),failure.failureCount());
            });
        
        });
    }
}
