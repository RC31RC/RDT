-- MySQL 8.0.17+: NO PAD binary identity, including significant trailing spaces.
-- Create only when rdt.consistency.jdbc-dead-letters=true. Redis Console and archive do not query this table.
CREATE TABLE rdt_failed_message (
 id VARCHAR(64) PRIMARY KEY COMMENT 'Failure record identifier',
 namespace VARCHAR(160) NOT NULL COMMENT 'Application environment boundary',
 destination VARCHAR(256) NOT NULL COMMENT 'Original transport destination',
 failure_key VARCHAR(512) NOT NULL COMMENT 'Mode-specific unique dead letter field',
 envelope LONGTEXT NOT NULL COMMENT 'Complete failed message JSON',
 mode VARCHAR(32) COMMENT 'Original queue mode',
 instance_id VARCHAR(160) COMMENT 'Receiving instance for broadcast uniqueness',
 original_message_id VARCHAR(160) COMMENT 'First publication identity',
 first_failed_at VARCHAR(64) COMMENT 'First observed failure timestamp',
 failure_count BIGINT COMMENT 'Confirmed failure budget consumed'
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT='RDT optional JDBC dead-letter store';
