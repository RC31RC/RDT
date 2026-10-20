CREATE TABLE rdt_outbox (
 id VARCHAR(36) PRIMARY KEY,
 namespace VARCHAR(160) NOT NULL,
 destination VARCHAR(512) NOT NULL,
 queue_mode VARCHAR(32) NOT NULL,
 envelope TEXT NOT NULL,
 status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
 failure_count INTEGER NOT NULL DEFAULT 0,
 available_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
 owner VARCHAR(512),
 claim_token VARCHAR(36),
 claim_until TIMESTAMP WITH TIME ZONE,
 last_error VARCHAR(2000),
 created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
 published_at TIMESTAMP WITH TIME ZONE,
 CONSTRAINT rdt_outbox_status CHECK (status IN ('PENDING','CLAIMED','PUBLISHED','DEAD'))
);
-- Namespace-leading indexes bound claiming, expired lease recovery and terminal retention scans.
CREATE INDEX rdt_outbox_due ON rdt_outbox(namespace,status,available_at);
CREATE INDEX rdt_outbox_claim ON rdt_outbox(namespace,status,claim_until);
CREATE INDEX rdt_outbox_retention ON rdt_outbox(namespace,status,published_at);
COMMENT ON TABLE rdt_outbox IS 'RDT transactional publication log';
COMMENT ON COLUMN rdt_outbox.id IS 'Stable outbox record identity';
COMMENT ON COLUMN rdt_outbox.namespace IS 'Application and environment boundary';
COMMENT ON COLUMN rdt_outbox.destination IS 'Logical Redis destination';
COMMENT ON COLUMN rdt_outbox.queue_mode IS 'STREAM or RELIABLE_BROADCAST transport';
COMMENT ON COLUMN rdt_outbox.envelope IS 'Complete immutable JSON message envelope';
COMMENT ON COLUMN rdt_outbox.status IS 'PENDING CLAIMED PUBLISHED DEAD';
COMMENT ON COLUMN rdt_outbox.failure_count IS 'Confirmed send failures only';
COMMENT ON COLUMN rdt_outbox.available_at IS 'Next relay time from database clock';
COMMENT ON COLUMN rdt_outbox.owner IS 'Claiming instance and process run';
COMMENT ON COLUMN rdt_outbox.claim_token IS 'Fences stale relay acknowledgements';
COMMENT ON COLUMN rdt_outbox.claim_until IS 'Database-clock claim expiry';
COMMENT ON COLUMN rdt_outbox.last_error IS 'Bounded failure diagnostic';
COMMENT ON COLUMN rdt_outbox.created_at IS 'Database creation time';
COMMENT ON COLUMN rdt_outbox.published_at IS 'Successful relay acknowledgement time';
