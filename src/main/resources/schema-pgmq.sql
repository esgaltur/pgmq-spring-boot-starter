-- PGMQ Extension
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;

-- Idempotency tracking table
CREATE TABLE IF NOT EXISTS pgmq_idempotency (
    queue_name VARCHAR(255) NOT NULL,
    msg_id BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (queue_name, msg_id)
);
