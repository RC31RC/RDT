CREATE TABLE rdt_inbox (
 namespace VARCHAR(160) NOT NULL,
 message_id VARCHAR(160) NOT NULL,
 consumer_group VARCHAR(384) NOT NULL,
 claim_marker VARCHAR(36),
 consumed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
 -- Group is part of identity, including instance suffix for reliable broadcast.
 PRIMARY KEY(namespace,message_id,consumer_group)
);
-- Bound namespace-scoped retention scans without scanning other applications.
CREATE INDEX rdt_inbox_retention ON rdt_inbox(namespace,consumed_at);
COMMENT ON TABLE rdt_inbox IS 'RDT transactional consumer deduplication';
COMMENT ON COLUMN rdt_inbox.namespace IS 'Application and environment boundary';
COMMENT ON COLUMN rdt_inbox.message_id IS 'Stable original envelope ID';
COMMENT ON COLUMN rdt_inbox.consumer_group IS 'Effective group including reliable broadcast instance';
COMMENT ON COLUMN rdt_inbox.claim_marker IS 'MySQL affected-row ambiguity guard';
COMMENT ON COLUMN rdt_inbox.consumed_at IS 'Database transaction consumption time';

-- H2 MERGE USING can race on an absent target. Test dialect serializes equal identity stripes until commit.
CREATE TABLE rdt_inbox_guard (id INTEGER PRIMARY KEY);
COMMENT ON TABLE rdt_inbox_guard IS 'H2-only transaction-bound insertion stripes';
COMMENT ON COLUMN rdt_inbox_guard.id IS 'Stable 0 to 63 deduplication lock stripe';
INSERT INTO rdt_inbox_guard SELECT X FROM SYSTEM_RANGE(0,63);
