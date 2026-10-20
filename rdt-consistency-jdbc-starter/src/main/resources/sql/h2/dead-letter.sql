-- Create only when rdt.consistency.jdbc-dead-letters=true. Redis Console and archive do not query this table.
CREATE TABLE rdt_failed_message (
 id VARCHAR(64) PRIMARY KEY,
 namespace VARCHAR(160) NOT NULL,
 destination VARCHAR(256) NOT NULL,
 failure_key VARCHAR(512) NOT NULL,
 envelope TEXT NOT NULL,
 mode VARCHAR(32),
 instance_id VARCHAR(160),
 original_message_id VARCHAR(160),
 first_failed_at VARCHAR(64),
 failure_count BIGINT
);
COMMENT ON TABLE rdt_failed_message IS 'RDT optional JDBC dead-letter store';
COMMENT ON COLUMN rdt_failed_message.id IS 'Failure record identifier';
COMMENT ON COLUMN rdt_failed_message.namespace IS 'Application environment boundary';
COMMENT ON COLUMN rdt_failed_message.destination IS 'Original transport destination';
COMMENT ON COLUMN rdt_failed_message.failure_key IS 'Mode-specific unique dead letter field';
COMMENT ON COLUMN rdt_failed_message.envelope IS 'Complete failed message JSON';
COMMENT ON COLUMN rdt_failed_message.mode IS 'Original queue mode';
COMMENT ON COLUMN rdt_failed_message.instance_id IS 'Receiving instance for broadcast uniqueness';
COMMENT ON COLUMN rdt_failed_message.original_message_id IS 'First publication identity';
COMMENT ON COLUMN rdt_failed_message.first_failed_at IS 'First observed failure timestamp';
COMMENT ON COLUMN rdt_failed_message.failure_count IS 'Confirmed failure budget consumed';
