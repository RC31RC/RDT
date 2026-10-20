-- MySQL 8.0.17+: NO PAD binary identity, including significant trailing spaces.
CREATE TABLE rdt_outbox (
 id VARCHAR(36) PRIMARY KEY COMMENT 'Stable outbox record identity',
 namespace VARCHAR(160) NOT NULL COMMENT 'Application and environment boundary',
 destination VARCHAR(512) NOT NULL COMMENT 'Logical Redis destination',
 queue_mode VARCHAR(32) NOT NULL COMMENT 'STREAM or RELIABLE_BROADCAST transport',
 envelope LONGTEXT NOT NULL COMMENT 'Complete immutable JSON message envelope',
 status VARCHAR(16) NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING CLAIMED PUBLISHED DEAD',
 failure_count INTEGER NOT NULL DEFAULT 0 COMMENT 'Confirmed send failures only',
 available_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'Next relay time from database clock',
 owner VARCHAR(512) COMMENT 'Claiming instance and process run',
 claim_token VARCHAR(36) COMMENT 'Fences stale relay acknowledgements',
 claim_until TIMESTAMP(3) COMMENT 'Database-clock claim expiry',
 last_error VARCHAR(2000) COMMENT 'Bounded failure diagnostic',
 created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'Database creation time',
 published_at TIMESTAMP(3) COMMENT 'Successful relay acknowledgement time',
 CONSTRAINT rdt_outbox_status CHECK (status IN ('PENDING','CLAIMED','PUBLISHED','DEAD'))
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin COMMENT='RDT transactional consistency';
-- Namespace-leading indexes bound claiming, expired lease recovery and terminal retention scans.
CREATE INDEX rdt_outbox_due ON rdt_outbox(namespace,status,available_at);
CREATE INDEX rdt_outbox_claim ON rdt_outbox(namespace,status,claim_until);
CREATE INDEX rdt_outbox_retention ON rdt_outbox(namespace,status,published_at);
