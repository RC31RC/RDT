package io.github.rdt.consistency.jdbc;

import io.github.rdt.exception.RdtConsistencyException;
import io.github.rdt.exception.RdtConfigurationException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DataSourceUtils;

/** SQL varies because H2 MERGE, MySQL and PostgreSQL do not share conflict semantics. */
public enum JdbcDialect {
    POSTGRESQL, MYSQL, H2;
    public static JdbcDialect detect(DataSource source) {
        Connection connection = DataSourceUtils.getConnection(source);
        try {
            String product = connection.getMetaData().getDatabaseProductName();
            return switch (product) {
                case "PostgreSQL" -> POSTGRESQL;
                case "MySQL" -> MYSQL;
                case "H2" -> H2;
                default -> throw new RdtConfigurationException("Unsupported RDT consistency database: " + product);
            };
        } catch (SQLException error) { throw new RdtConsistencyException("Cannot detect RDT SQL dialect", error); }
        finally { DataSourceUtils.releaseConnection(connection, source); }
    }
    public String plusMillis() {
        return switch (this) {
            case POSTGRESQL -> "CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')";
            case MYSQL -> "TIMESTAMPADD(MICROSECOND, ? * 1000, CURRENT_TIMESTAMP(3))";
            case H2 -> "DATEADD('MILLISECOND', ?, CURRENT_TIMESTAMP)";
        };
    }
    public String insertInbox(String table) {
        return switch (this) {
            case POSTGRESQL -> "INSERT INTO " + table + " (namespace,message_id,consumer_group) VALUES (?,?,?) ON CONFLICT (namespace,message_id,consumer_group) DO NOTHING";
            // The no-op update returns one on drivers using CLIENT_FOUND_ROWS. A unique claim marker is
            // therefore checked by the store rather than trusting MySQL's connection-dependent row count.
            case MYSQL -> "INSERT INTO " + table + " (namespace,message_id,consumer_group,claim_marker) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE claim_marker=claim_marker";
            case H2 -> "MERGE INTO " + table + " t USING (VALUES (?,?,?)) s(namespace,message_id,consumer_group) ON t.namespace=s.namespace AND t.message_id=s.message_id AND t.consumer_group=s.consumer_group WHEN NOT MATCHED THEN INSERT (namespace,message_id,consumer_group) VALUES (s.namespace,s.message_id,s.consumer_group)";
        };
    }
    public String table(String schema, String name) { return schema.isEmpty() ? name : schema + "." + name; }
}
