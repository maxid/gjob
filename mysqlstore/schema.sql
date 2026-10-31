-- Receipt store for gjob. Every column is needed by the state machine:
--   execution_id   primary key: the de-duplication point
--   fencing_token  checked on every write so a late worker cannot overwrite
--   lease_until    drives recovery without scanning the whole table
--   result/error   bounded diagnostics, never a payload dump
CREATE TABLE IF NOT EXISTS gjob_receipt
(
    execution_id  VARCHAR(191) NOT NULL,
    job_name      VARCHAR(191) NOT NULL,
    trigger_type  VARCHAR(32)  NOT NULL,
    scheduled_at  DATETIME(3)  NOT NULL,
    attempt       INT          NOT NULL DEFAULT 1,
    state         VARCHAR(16)  NOT NULL,
    fencing_token BIGINT UNSIGNED NOT NULL,
    lease_until   DATETIME(3)  NOT NULL,
    updated_at    DATETIME(3)  NOT NULL,
    result        JSON         NULL,
    error         VARCHAR(1024) NOT NULL DEFAULT '',
    PRIMARY KEY (execution_id),
    KEY idx_lease (lease_until),
    KEY idx_job (job_name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
