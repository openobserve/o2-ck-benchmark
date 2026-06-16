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

    INDEX idx_trace_id   trace_id   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_id    span_id    TYPE bloom_filter(0.01) GRANULARITY 1,

    INDEX idx_pod_name kubernetes_pod_name TYPE bloom_filter(0.01) GRANULARITY 1,

    INDEX idx_message_text message TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(message))
)
ENGINE = MergeTree
ORDER BY (_timestamp)
SETTINGS index_granularity = 8192;
