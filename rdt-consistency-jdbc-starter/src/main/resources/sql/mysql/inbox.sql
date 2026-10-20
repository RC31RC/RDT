-- MySQL 8.0.17+: NO PAD binary identity, including significant trailing spaces.
CREATE TABLE rdt_inbox (
 namespace VARCHAR(160) NOT NULL COMMENT 'Application and environment boundary',
 message_id VARCHAR(160) NOT NULL COMMENT 'Stable original envelope ID',
 consumer_group VARCHAR(384) NOT NULL COMMENT 'Effective group including reliable broadcast instance',
 claim_marker VARCHAR(36) COMMENT 'MySQL affected-row ambiguity guard',
 consumed_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'Database transaction consumption time',
 -- Group is part of identity, including instance suffix for reliable broadcast.
 PRIMARY KEY(namespace,message_id,consumer_group)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT='RDT transactional consistency';
-- Bound namespace-scoped retention scans without scanning other applications.
CREATE INDEX rdt_inbox_retention ON rdt_inbox(namespace,consumed_at);
