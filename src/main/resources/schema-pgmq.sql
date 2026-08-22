-- PGMQ Extension
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;

-- Idempotency tracking table
CREATE TABLE IF NOT EXISTS pgmq_idempotency (
    queue_name VARCHAR(255) NOT NULL,
    msg_id BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (queue_name, msg_id)
);

-- Short cross-instance leases used to suppress duplicate immediate reads when
-- PostgreSQL broadcasts the same queue notification to every application node.
CREATE TABLE IF NOT EXISTS pgmq_listener_wakeup_lease (
    queue_name VARCHAR(255) PRIMARY KEY,
    owner_id VARCHAR(36) NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL
);
