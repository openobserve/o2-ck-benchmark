-- ClickHouse table for the OpenObserve-vs-ClickHouse observability benchmark.
--
-- Columns mirror the canonical fields emitted by `datagen` (see datagen/src/main.rs),
-- a clean, typed observability schema focused on the "needle" query workload.
--
-- `_timestamp` is epoch MICROSECONDS (UInt64), exactly as OpenObserve stores it, so the
-- same generated JSON feeds both backends unchanged. Time-range filters use integer
-- microsecond bounds on both sides for a fair comparison.
--
-- Apply with:
--   ./bin/clickhouse client --queries-file schemas/clickhouse.sql
--   (or pipe via the HTTP interface)

CREATE DATABASE IF NOT EXISTS default;

DROP TABLE IF EXISTS default.k8s_logs;

CREATE TABLE default.k8s_logs
(
    `_timestamp`                 UInt64,
    `level`                      LowCardinality(String),
    `message`                    String,
    `host`                       String,
    `stream`                     LowCardinality(String),
    `kubernetes_namespace_name`  LowCardinality(String),
    `kubernetes_pod_name`        String,
    `kubernetes_container_name`  LowCardinality(String),
    `kubernetes_node_name`       LowCardinality(String),
    `kubernetes_labels_app`      LowCardinality(String),
    `kubernetes_labels_version`  LowCardinality(String),
    `kubernetes_pod_ip`          String,
    `trace_id`                   String,
    `span_id`                    String,
    `request_id`                 String,
    `http_method`                LowCardinality(String),
    `http_path`                  LowCardinality(String),
    `http_status`                UInt16,
    `http_latency_ms`            UInt32,
    `http_bytes_out`             UInt32,
    `client_ip`                  String,
    `ja3`                        String,
    `user_id`                    String,
    `session_id`                 String,
    `region`                     LowCardinality(String),
    `zone`                       LowCardinality(String),
    `cluster`                    LowCardinality(String),

    -- High-cardinality id lookups: bloom filters
    -- let the granule skip-index prune nearly everything for an exact-match WHERE.
    INDEX idx_trace_id   trace_id   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_id    span_id    TYPE bloom_filter(0.01) GRANULARITY 1,

    -- Selective structured filter (query q7): kubernetes_pod_name is high-cardinality
    -- (~24k pod values, see datagen/src/main.rs), so an exact-match WHERE hits only a
    -- small fraction of rows. bloom_filter skip-index mirrors OpenObserve's secondary
    -- index on kubernetes_pod_name. (Not the ORDER BY prefix, so it prunes by granule
    -- like the id bloom filters do.)
    INDEX idx_pod_name kubernetes_pod_name TYPE bloom_filter(0.01) GRANULARITY 1,

    -- Full-text inverted index over `message` (queries q2, q3, q5, q6 and the q10
    -- filtered histogram), mirroring the OpenObserve tantivy full-text index — both
    -- engines get a real inverted index, so token search compares FTS vs FTS.
    -- Queries use hasAnyTokens(), which is served by this index.
    -- `preprocessor = lower(...)` mirrors tantivy's lowercasing, so the same single
    -- lowercase needle returns identical results on both engines. NOTE: array-form
    -- hasAnyTokens needles bypass the preprocessor, so they must be passed in
    -- lowercase. (Text indexes always use one index granule per part; an explicit
    -- GRANULARITY would be ignored.)
    INDEX idx_message_text message TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message))
)
ENGINE = MergeTree
-- Time-ordered, mirroring OpenObserve's time-partitioned layout. A ClickHouse
-- query only gets a primary-key (sorting-key) speed benefit when it filters on a
-- LEADING PREFIX of ORDER BY. By sorting on `_timestamp` alone, NO structured
-- column the benchmark filters on (kubernetes_container_name in q4/q5, etc.) sits
-- in the sort-key prefix, so CH gains no access-path advantage that OpenObserve
-- can't match — all needle selectivity comes from the bloom / text skip indexes
-- above, exactly like OpenObserve's secondary + full-text indexes. `_timestamp`
-- is unique and monotonic here, so no further sort columns would ever apply.
-- (Earlier revisions led with namespace/container/stream; container is filtered
-- by q4/q5, which gave CH sort-key pruning OpenObserve does not have — removed.)
ORDER BY (_timestamp)
SETTINGS index_granularity = 8192;
