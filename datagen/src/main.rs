use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::Engine;
use chrono::Utc;
use clap::{Parser, ValueEnum};
use flate2::Compression;
use flate2::write::GzEncoder;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::{Map, Value, json};
use tokio::sync::Semaphore;

mod templates;

/// Which backend(s) to ship the generated logs to.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// OpenObserve only
    Openobserve,
    /// ClickHouse only
    Clickhouse,
    /// Both backends, from the same generated batch
    Both,
}

impl Target {
    fn openobserve(self) -> bool {
        matches!(self, Target::Openobserve | Target::Both)
    }
    fn clickhouse(self) -> bool {
        matches!(self, Target::Clickhouse | Target::Both)
    }
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "benchmark-data",
    about = "Generate k8s-style JSON logs and ship them to OpenObserve and/or ClickHouse",
    version
)]
struct Args {
    /// Which backend(s) to write to
    #[arg(long, value_enum, default_value_t = Target::Both)]
    target: Target,

    /// OpenObserve base URL (no trailing slash needed)
    #[arg(long, default_value = "http://localhost:5080")]
    o2_url: String,

    /// Organization id
    #[arg(long, default_value = "default")]
    org: String,

    /// Target stream name (also used as the ClickHouse table name)
    #[arg(long, default_value = "k8s_logs")]
    stream: String,

    /// Login email
    #[arg(long, default_value = "root@example.com")]
    username: String,

    /// Login password
    #[arg(long, default_value = "Complexpass#123")]
    password: String,

    /// ClickHouse HTTP URL (no trailing slash needed)
    #[arg(long, default_value = "http://localhost:8123")]
    ch_url: String,

    /// ClickHouse database
    #[arg(long, default_value = "default")]
    ch_database: String,

    /// ClickHouse user
    #[arg(long, default_value = "default")]
    ch_user: String,

    /// ClickHouse password (empty by default)
    #[arg(long, default_value = "")]
    ch_password: String,

    /// Total number of log records to send
    #[arg(long, default_value_t = 100_000_000)]
    total: usize,

    /// Records per HTTP request (one bulk JSON array)
    #[arg(long, default_value_t = 8_000)]
    batch_size: usize,

    /// Parallel in-flight HTTP requests
    #[arg(long, default_value_t = 6)]
    concurrency: usize,

    /// Gzip request bodies (Content-Encoding: gzip). Big win over a network;
    /// negligible benefit on localhost. Pass `--compress false` to disable.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    compress: bool,

    /// PRNG seed for reproducible payloads
    #[arg(long, default_value_t = 0xC0FFEE)]
    seed: u64,

    /// Print one sample record to stdout and exit without sending
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Optional path to write a JSON ingest-stats summary (records, raw bytes, rate)
    #[arg(long)]
    stats_out: Option<String>,
}

const LEVELS: &[&str] = &["INFO", "WARN", "ERROR", "DEBUG", "TRACE"];
// Matches LEVELS order: ~70% INFO, 10% WARN, 4% ERROR, 15% DEBUG, 1% TRACE.
const LEVEL_WEIGHTS: &[u32] = &[70, 10, 4, 15, 1];
const NAMESPACES: &[&str] = &[
    "kube-system",
    "default",
    "monitoring",
    "ingress-nginx",
    "logging",
    "payments",
    "checkout",
    "search",
    "auth",
];
const APPS: &[&str] = &[
    "api-gateway",
    "user-service",
    "order-service",
    "payment-service",
    "search-service",
    "notification-service",
    "cart-service",
    "recommendation-service",
];
const NODES: &[&str] = &[
    "ip-10-0-1-12.ec2.internal",
    "ip-10-0-1-87.ec2.internal",
    "ip-10-0-2-44.ec2.internal",
    "ip-10-0-2-93.ec2.internal",
    "ip-10-0-3-15.ec2.internal",
];
// Bounded pool of pod identifiers per app: a real deployment has 1-3
// ReplicaSets (versions) × ~10 pods, not a fresh hash per log line.
const REPLICASET_HASHES: &[&str] = &["7d4f5b9c8", "9a2c1e6f4", "5b8d3a7c2"];
const STREAMS: &[&str] = &["stdout", "stderr"];
const STREAM_WEIGHTS: &[u32] = &[85, 15]; // most logs go to stdout
const METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];
const METHOD_WEIGHTS: &[u32] = &[60, 22, 8, 5, 5];
const PATHS: &[&str] = &[
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/cart",
    "/api/v1/checkout",
    "/api/v1/search",
    "/api/v1/auth/login",
    "/api/v1/health",
    "/metrics",
];
const STATUS: &[u16] = &[200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503];
// Heavy tail toward 200. Matches STATUS order.
const STATUS_WEIGHTS: &[u32] = &[600, 60, 30, 20, 50, 25, 15, 80, 15, 8, 7];
// Bounded "client identity" universe — same identity → same IP. Zipf
// concentrates traffic on a hot head (chatty bots / power users) with a long
// tail. Drawn from 16 plausible first octets × ~625k lower-bit combos to
// give an effective cardinality of ~10M.
const CLIENT_FIRST_OCTETS: &[u8] = &[
    3, 8, 13, 18, 23, 35, 40, 52, 54, 65, 73, 76, 99, 104, 108, 142,
];

// Version label per app per ReplicaSet. rs_idx 0 = current/hot, 1 = previous,
// 2 = older draining version. Each app maintains its own release line, so
// cluster-wide cardinality is APPS.len() × REPLICASET_HASHES.len() = 24.
// Order matches APPS.
const APP_VERSIONS: &[[&str; 3]] = &[
    ["v3.4.1", "v3.4.0", "v3.3.7"],
    ["v2.1.5", "v2.1.4", "v2.0.9"],
    ["v1.8.2", "v1.8.1", "v1.7.3"],
    ["v4.0.0", "v3.9.6", "v3.9.5"],
    ["v2.6.3", "v2.6.2", "v2.5.8"],
    ["v1.3.7", "v1.3.6", "v1.2.4"],
    ["v2.0.1", "v2.0.0", "v1.9.5"],
    ["v0.9.2", "v0.9.1", "v0.8.4"],
];

/// Gzip a request body. Fast compression level keeps CPU cost low — these
/// payloads are highly compressible text, so even level 1 cuts most of the bytes.
fn gzip(body: &[u8]) -> Vec<u8> {
    let mut enc = GzEncoder::new(Vec::with_capacity(body.len() / 4), Compression::fast());
    enc.write_all(body).expect("gzip write");
    enc.finish().expect("gzip finish")
}

fn pick<'a, T>(rng: &mut ChaCha8Rng, xs: &'a [T]) -> &'a T {
    &xs[rng.gen_range(0..xs.len())]
}

/// Weighted pick — `weights[i]` is the relative probability of `xs[i]`.
/// Slices must be the same length; weight sum must be > 0.
fn weighted_pick<'a, T>(rng: &mut ChaCha8Rng, xs: &'a [T], weights: &[u32]) -> &'a T {
    debug_assert_eq!(xs.len(), weights.len());
    let total: u32 = weights.iter().sum();
    let mut r = rng.gen_range(0..total);
    for (i, w) in weights.iter().enumerate() {
        if r < *w {
            return &xs[i];
        }
        r -= *w;
    }
    &xs[xs.len() - 1]
}

/// Power-law / Zipf-ish index sampler over `[0, n)`.
/// `skew > 1` concentrates mass near index 0 (the "hot" head);
/// `skew = 1` is uniform; values like 1.5..3.0 are typical for real traffic.
fn zipf_index(rng: &mut ChaCha8Rng, n: usize, skew: f64) -> usize {
    debug_assert!(n > 0);
    let u: f64 = rng.gen_range(0.0..1.0_f64);
    let i = (u.powf(skew) * n as f64) as usize;
    i.min(n - 1)
}

/// Zipf-weighted pick from a slice (slot 0 is hottest).
fn pick_zipf<'a, T>(rng: &mut ChaCha8Rng, xs: &'a [T], skew: f64) -> &'a T {
    &xs[zipf_index(rng, xs.len(), skew)]
}

fn hex_id(rng: &mut ChaCha8Rng, bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rng.fill(&mut buf[..]);
    let mut s = String::with_capacity(bytes * 2);
    for b in buf {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn make_record(
    rng: &mut ChaCha8Rng,
    timestamp_us: u64,
    trace_universe: usize,
    ip_universe: usize,
    ja3_universe: usize,
) -> Value {
    // Skewed picks: a couple of hot apps/namespaces dominate, a few hot URLs eat
    // most traffic, INFO/200/stdout dominate, etc. Nodes stay roughly uniform
    // because real clusters load-balance pods across nodes.
    let app_idx = zipf_index(rng, APPS.len(), 1.8);
    let rs_idx = zipf_index(rng, REPLICASET_HASHES.len(), 1.8);
    // High-cardinality pod universe (~24k pods): 1024 churned suffixes per
    // (app, replicaset), zipf-hot so a few pods carry most traffic.
    let pod_idx = zipf_index(rng, 1024, 1.5);
    let app = APPS[app_idx].to_string();
    let pod_suffix = &uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        format!("{app_idx}/{rs_idx}/{pod_idx}").as_bytes(),
    )
    .simple()
    .to_string()[..5];
    let pod_name = format!("{app}-{}-{pod_suffix}", REPLICASET_HASHES[rs_idx]);
    let container = format!("{app}-container");
    let namespace = pick_zipf(rng, NAMESPACES, 1.6).to_string();
    let node = pick(rng, NODES).to_string();
    let level = weighted_pick(rng, LEVELS, LEVEL_WEIGHTS).to_string();
    let log_stream = weighted_pick(rng, STREAMS, STREAM_WEIGHTS).to_string();
    let method = weighted_pick(rng, METHODS, METHOD_WEIGHTS).to_string();
    let path = pick_zipf(rng, PATHS, 1.7).to_string();
    let status = *weighted_pick(rng, STATUS, STATUS_WEIGHTS);
    let latency_ms = rng.gen_range(1..1500);
    let bytes_out = rng.gen_range(64..32_768);
    let host = format!("{pod_name}.{namespace}.svc.cluster.local");
    // Bounded trace universe (so log lines group into traces); trace_id is a
    // UUID v5 (SHA-1 of namespace + trace index) — realistic-looking 32-hex ids
    // instead of mostly-leading-zero hex counters. span_id is W3C/OTel style:
    // 8 random bytes = 16 hex chars.
    let trace_id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        rng.gen_range(0..trace_universe).to_string().as_bytes(),
    )
    .simple()
    .to_string();
    let span_id = hex_id(rng, 8);
    let client_id = rng.gen_range(0..ip_universe);
    let client_first = CLIENT_FIRST_OCTETS[client_id % CLIENT_FIRST_OCTETS.len()];
    let client_rem = (client_id / CLIENT_FIRST_OCTETS.len()) as u32;
    let client_ip = format!(
        "{client_first}.{}.{}.{}",
        (client_rem / 65536) & 0xff,
        (client_rem >> 8) & 0xff,
        client_rem & 0xff,
    );
    let ja3 = format!("{:032x}", rng.gen_range(0u128..ja3_universe as u128));
    let req_id = hex_id(rng, 16);
    // message is drawn from a pool of openobserve-derived log-line templates so
    // tantivy sees a much wider vocabulary than the old fixed format produced.
    // For WARN/ERROR records we tack on a structured exception suffix to keep
    // stack-trace tokens in circulation. The record's tracing context (trace id,
    // span id, request id) is appended the way request-scoped logging does it,
    // so text queries have real needles to find inside `message`: the full ids
    // are whole tokens (token-index lookups), and the request id is the rare
    // whole-token needle for the match_all/hasAnyTokens search (q2).
    let long_message = {
        let mut line = templates::generate_log_line(rng);
        if level == "ERROR" || level == "WARN" {
            line.push_str(" | ");
            line.push_str(&templates::tpl_exception(rng));
        }
        // Pad with extra template lines to ~1200 chars — a realistic multi-part
        // application log line.
        while line.len() < 1100 {
            line.push_str(" | ");
            line.push_str(&templates::generate_log_line(rng));
        }
        line.push_str(" | trace_id=");
        line.push_str(&trace_id);
        line.push_str(" span_id=");
        line.push_str(&span_id);
        line.push_str(" x_request_id=");
        line.push_str(&req_id);
        line
    };

    let canonical: Vec<(&str, Value)> = vec![
        ("_timestamp", json!(timestamp_us)),
        ("level", json!(level)),
        ("message", json!(long_message)),
        ("host", json!(host)),
        ("stream", json!(log_stream)),
        ("kubernetes_namespace_name", json!(namespace)),
        ("kubernetes_pod_name", json!(pod_name)),
        ("kubernetes_container_name", json!(container)),
        ("kubernetes_node_name", json!(node)),
        ("kubernetes_labels_app", json!(app)),
        (
            "kubernetes_labels_version",
            json!(APP_VERSIONS[app_idx][rs_idx]),
        ),
        (
            "kubernetes_pod_ip",
            json!(format!(
                "10.{}.{}.{}",
                app_idx + 1,
                rs_idx + 1,
                pod_idx + 10
            )),
        ),
        ("trace_id", json!(trace_id)),
        ("span_id", json!(span_id)),
        ("request_id", json!(req_id)),
        ("http_method", json!(method)),
        ("http_path", json!(path)),
        ("http_status", json!(status)),
        ("http_latency_ms", json!(latency_ms)),
        ("http_bytes_out", json!(bytes_out)),
        ("client_ip", json!(client_ip)),
        ("ja3", json!(ja3)),
        ("user_id", json!(format!("u_{}", rng.gen_range(1..100_000)))),
        ("session_id", json!(hex_id(rng, 8))),
        (
            "region",
            json!(*weighted_pick(
                rng,
                &["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"],
                &[60, 25, 12, 3],
            )),
        ),
        (
            "zone",
            json!(format!(
                "az-{}",
                weighted_pick(rng, &[1u8, 2, 3], &[55, 30, 15])
            )),
        ),
        (
            "cluster",
            json!(*weighted_pick(
                rng,
                &["prod-a", "prod-b", "stage-a"],
                &[70, 25, 5],
            )),
        ),
    ];

    let mut obj = Map::with_capacity(canonical.len());
    for (k, v) in canonical {
        obj.insert(k.to_string(), v);
    }
    Value::Object(obj)
}

fn make_batch(
    seed: u64,
    start_record_idx: u64,
    size: usize,
    run_start_timestamp_us: u64,
    trace_universe: usize,
    ip_universe: usize,
    ja3_universe: usize,
) -> Vec<Value> {
    (0..size)
        .map(|i| {
            let record_idx = start_record_idx + i as u64;
            let mut rng = ChaCha8Rng::seed_from_u64(splitmix64(seed ^ record_idx));
            make_record(
                &mut rng,
                run_start_timestamp_us.wrapping_add(record_idx),
                trace_universe,
                ip_universe,
                ja3_universe,
            )
        })
        .collect()
}

/// Everything a single batch send needs: the HTTP client, both sink
/// endpoints/credentials, the generation parameters, and the shared progress
/// counters. Held behind an `Arc` and shared across every spawned task.
struct Sink {
    client: reqwest::Client,
    oo_endpoint: Arc<String>,
    oo_auth: Arc<String>,
    oo_enabled: bool,
    ch_base: Arc<String>,
    ch_insert: Arc<String>,
    ch_user: Arc<String>,
    ch_password: Arc<String>,
    ch_enabled: bool,
    compress: bool,
    seed: u64,
    run_start_timestamp_us: u64,
    trace_universe: usize,
    ip_universe: usize,
    ja3_universe: usize,
    sent: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
    oo_failed: Arc<AtomicU64>,
    ch_failed: Arc<AtomicU64>,
    bytes_ok: Arc<AtomicU64>,
}

impl Sink {
    /// Generate `this_batch` records starting at global record index
    /// `start_record_idx` and ship them to every enabled target. Updates the
    /// shared counters and returns `true` only if all enabled targets accepted
    /// the batch.
    async fn ship(&self, batch_idx: usize, start_record_idx: u64, this_batch: usize) -> bool {
        let seed = self.seed;
        let run_start_timestamp_us = self.run_start_timestamp_us;
        let trace_universe = self.trace_universe;
        let ip_universe = self.ip_universe;
        let ja3_universe = self.ja3_universe;
        let oo_enabled = self.oo_enabled;
        let ch_enabled = self.ch_enabled;
        let compress = self.compress;

        // Per-record seed keeps content reproducible regardless of completion
        // order or batch size. The global row index also drives _timestamp.
        // Generating the payload inside spawn_blocking lets tokio worker threads
        // run batch generation in parallel with in-flight HTTP sends. Each record
        // is serialized ONCE; the two HTTP bodies — a JSON array (OpenObserve) and
        // newline-delimited rows (ClickHouse JSONEachRow) — are assembled from the
        // same per-record bytes, so dual-write pays no double-serialization cost.
        let (oo_body, ch_body, raw_len) = tokio::task::spawn_blocking(move || {
            let payload = make_batch(
                seed,
                start_record_idx,
                this_batch,
                run_start_timestamp_us,
                trace_universe,
                ip_universe,
                ja3_universe,
            );
            let rows: Vec<Vec<u8>> = payload
                .iter()
                .map(|rec| serde_json::to_vec(rec).expect("serialize record"))
                .collect();
            let total_bytes: usize = rows.iter().map(|r| r.len()).sum();
            let oo = if oo_enabled {
                let mut buf = Vec::with_capacity(total_bytes + rows.len() + 1);
                buf.push(b'[');
                for (i, r) in rows.iter().enumerate() {
                    if i > 0 {
                        buf.push(b',');
                    }
                    buf.extend_from_slice(r);
                }
                buf.push(b']');
                Some(buf)
            } else {
                None
            };
            let ch = if ch_enabled {
                let mut buf = Vec::with_capacity(total_bytes + rows.len());
                for r in &rows {
                    buf.extend_from_slice(r);
                    buf.push(b'\n');
                }
                Some(buf)
            } else {
                None
            };
            // Raw (uncompressed) volume for this batch — NDJSON preferred, else
            // the JSON array. This is the logical ingest volume the MB/s stat
            // reports, independent of on-the-wire compression.
            let raw_len = ch
                .as_ref()
                .map(|b| b.len())
                .or_else(|| oo.as_ref().map(|b| b.len()))
                .unwrap_or(0) as u64;
            // Compress on the worker thread (CPU work overlaps in-flight sends).
            let (oo, ch) = if compress {
                (oo.as_deref().map(gzip), ch.as_deref().map(gzip))
            } else {
                (oo, ch)
            };
            (oo, ch, raw_len)
        })
        .await
        .expect("batch generation task panicked");

        // Fire both sinks concurrently; each returns Ok(()) or an error string.
        let oo_fut = async {
            let Some(body) = oo_body else { return Ok(()) };
            let mut req = self
                .client
                .post(self.oo_endpoint.as_str())
                .header(reqwest::header::AUTHORIZATION, self.oo_auth.as_str())
                .header(reqwest::header::CONTENT_TYPE, "application/json");
            if compress {
                req = req.header(reqwest::header::CONTENT_ENCODING, "gzip");
            }
            let res = req.body(body).send().await;
            match res {
                Ok(r) if r.status().is_success() => Ok(()),
                Ok(r) => {
                    let status = r.status();
                    let b = r.text().await.unwrap_or_default();
                    Err(format!("openobserve {status} {b}"))
                }
                Err(e) => Err(format!("openobserve {e}")),
            }
        };
        let ch_fut = async {
            let Some(body) = ch_body else { return Ok(()) };
            let mut req = self
                .client
                .post(self.ch_base.as_str())
                .query(&[("query", self.ch_insert.as_str())])
                .header("X-ClickHouse-User", self.ch_user.as_str())
                .header("X-ClickHouse-Key", self.ch_password.as_str())
                .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson");
            if compress {
                req = req.header(reqwest::header::CONTENT_ENCODING, "gzip");
            }
            let res = req.body(body).send().await;
            match res {
                Ok(r) if r.status().is_success() => Ok(()),
                Ok(r) => {
                    let status = r.status();
                    let b = r.text().await.unwrap_or_default();
                    Err(format!("clickhouse {status} {b}"))
                }
                Err(e) => Err(format!("clickhouse {e}")),
            }
        };

        let (oo_res, ch_res) = tokio::join!(oo_fut, ch_fut);
        let mut ok = true;
        if let Err(e) = oo_res {
            ok = false;
            self.oo_failed.fetch_add(this_batch as u64, Ordering::Relaxed);
            eprintln!("batch {batch_idx} {e}");
        }
        if let Err(e) = ch_res {
            ok = false;
            self.ch_failed.fetch_add(this_batch as u64, Ordering::Relaxed);
            eprintln!("batch {batch_idx} {e}");
        }
        if ok {
            self.sent.fetch_add(this_batch as u64, Ordering::Relaxed);
            self.bytes_ok.fetch_add(raw_len, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(this_batch as u64, Ordering::Relaxed);
        }
        ok
    }
}

fn spawn_progress_reporter(
    sent: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
    total: u64,
    start: Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let bar_width: usize = 30;
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let s = sent.load(Ordering::Relaxed);
            let f = failed.load(Ordering::Relaxed);
            let processed = (s + f).min(total);
            let elapsed = start.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                processed as f64 / elapsed
            } else {
                0.0
            };
            let frac = if total > 0 {
                processed as f64 / total as f64
            } else {
                1.0
            };
            let filled = ((frac * bar_width as f64) as usize).min(bar_width);
            let mut bar = String::with_capacity(bar_width);
            bar.extend(std::iter::repeat_n('=', filled));
            bar.extend(std::iter::repeat_n(' ', bar_width - filled));
            let eta = if rate > 0.0 && processed < total {
                format!("{:5.0}s", (total - processed) as f64 / rate)
            } else {
                "  ---".to_string()
            };
            let finished = done.load(Ordering::Relaxed);
            let mut err = std::io::stderr().lock();
            let _ = write!(
                err,
                "\r[{bar}] {pct:5.1}% {processed}/{total} sent={s} failed={f} {rate:.0} rec/s ETA {eta}   ",
                pct = frac * 100.0,
            );
            let _ = err.flush();
            if finished {
                let _ = writeln!(err);
                break;
            }
        }
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.total == 0 {
        bail!("--total must be > 0");
    }
    if args.batch_size == 0 {
        bail!("--batch-size must be > 0");
    }
    if args.concurrency == 0 {
        bail!("--concurrency must be > 0");
    }

    if args.dry_run {
        let mut rng = ChaCha8Rng::seed_from_u64(splitmix64(args.seed));
        let run_start_timestamp_us = Utc::now().timestamp_micros() as u64;
        let trace_universe = (args.total as f64 / 7.8).round() as usize;
        let ip_universe = (args.total as f64 * 0.005127018383746945).round() as usize;
        let ja3_universe = (args.total as f64 * (6.173306600579376 / 100.0)).round() as usize;
        let sample = make_record(
            &mut rng,
            run_start_timestamp_us,
            trace_universe,
            ip_universe,
            ja3_universe,
        );
        println!("{}", serde_json::to_string_pretty(&sample)?);
        return Ok(());
    }

    let oo_enabled = args.target.openobserve();
    let ch_enabled = args.target.clickhouse();

    // OpenObserve sink: bulk JSON-array ingest endpoint + basic auth header.
    let oo_endpoint = Arc::new(format!(
        "{}/api/{}/{}/_json",
        args.o2_url.trim_end_matches('/'),
        args.org,
        args.stream
    ));
    let oo_auth = Arc::new({
        let raw = format!("{}:{}", args.username, args.password);
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        format!("Basic {b64}")
    });

    // ClickHouse sink: HTTP interface, query carries the INSERT, body carries the
    // newline-delimited rows.
    let ch_base = Arc::new(format!("{}/", args.ch_url.trim_end_matches('/')));
    let ch_insert = Arc::new(format!(
        "INSERT INTO `{}`.`{}` FORMAT JSONEachRow",
        args.ch_database, args.stream
    ));
    let ch_user = Arc::new(args.ch_user.clone());
    let ch_password = Arc::new(args.ch_password.clone());

    let client = reqwest::Client::builder()
        .gzip(true)
        .build()
        .context("building http client")?;

    let total_batches = args.total.div_ceil(args.batch_size);
    let mut destinations = Vec::new();
    if oo_enabled {
        destinations.push(oo_endpoint.to_string());
    }
    if ch_enabled {
        destinations.push(format!(
            "{}`{}`.`{}`",
            ch_base.as_str(),
            args.ch_database,
            args.stream
        ));
    }
    println!(
        "shipping {} records -> [{}] in {} batch(es) of up to {}",
        args.total,
        destinations.join(", "),
        total_batches,
        args.batch_size
    );

    let sem = Arc::new(Semaphore::new(args.concurrency));
    // `sent`/`failed` count records delivered to *all* enabled targets vs. records
    // that failed on at least one target. `oo_failed`/`ch_failed` break it down.
    let sent = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let oo_failed = Arc::new(AtomicU64::new(0));
    let ch_failed = Arc::new(AtomicU64::new(0));
    // Raw JSON bytes of successfully-delivered batches (one record's worth counted
    // once, regardless of how many targets it went to) — used for ingest MB/s.
    let bytes_ok = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(total_batches);

    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_handle = spawn_progress_reporter(
        sent.clone(),
        failed.clone(),
        progress_done.clone(),
        args.total as u64,
        start,
    );

    let mut remaining = args.total;
    let run_start_timestamp_us = Utc::now().timestamp_micros() as u64;
    let trace_universe = (args.total as f64 / 7.8).round() as usize;
    let ip_universe = (args.total as f64 * 0.005127018383746945).round() as usize;
    let ja3_universe = (args.total as f64 * (6.173306600579376 / 100.0)).round() as usize;

    let sink = Arc::new(Sink {
        client,
        oo_endpoint,
        oo_auth,
        oo_enabled,
        ch_base,
        ch_insert,
        ch_user,
        ch_password,
        ch_enabled,
        compress: args.compress,
        seed: args.seed,
        run_start_timestamp_us,
        trace_universe,
        ip_universe,
        ja3_universe,
        sent: sent.clone(),
        failed: failed.clone(),
        oo_failed: oo_failed.clone(),
        ch_failed: ch_failed.clone(),
        bytes_ok: bytes_ok.clone(),
    });

    // Prime the stream BEFORE fanning out. Batch 0 holds the global-minimum
    // `_timestamp` (record index 0). Ship it alone and wait for the server to
    // accept it: until this returns there is exactly one request in flight, so
    // OpenObserve is guaranteed to create the stream from the earliest record and
    // set created_at to the true min_ts. (The concurrent loop below completes
    // batches out of order, so without this an out-of-order batch could create
    // the stream first and leave created_at later than the real min_ts.) If
    // batch 0 cannot land we abort, rather than let a later batch define
    // created_at.
    {
        let this_batch = remaining.min(args.batch_size);
        remaining -= this_batch;
        if !sink.ship(0, 0, this_batch).await {
            bail!(
                "priming batch (batch 0) failed; aborting so the stream's created_at \
                 is not set from a later, out-of-order batch"
            );
        }
    }

    for batch_idx in 1..total_batches {
        let this_batch = remaining.min(args.batch_size);
        let start_record_idx = (batch_idx * args.batch_size) as u64;
        remaining -= this_batch;

        let permit = sem.clone().acquire_owned().await.unwrap();
        let sink = sink.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            sink.ship(batch_idx, start_record_idx, this_batch).await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    progress_done.store(true, Ordering::Relaxed);
    let _ = progress_handle.await;

    let elapsed = start.elapsed().as_secs_f64();
    let sent = sent.load(Ordering::Relaxed);
    let failed = failed.load(Ordering::Relaxed);
    let oo_failed = oo_failed.load(Ordering::Relaxed);
    let ch_failed = ch_failed.load(Ordering::Relaxed);
    let bytes_ok = bytes_ok.load(Ordering::Relaxed);
    let rps = if elapsed > 0.0 {
        sent as f64 / elapsed
    } else {
        0.0
    };
    let mb_s = if elapsed > 0.0 {
        bytes_ok as f64 / 1_048_576.0 / elapsed
    } else {
        0.0
    };
    println!(
        "done: sent={sent} failed={failed} (openobserve_failed={oo_failed} clickhouse_failed={ch_failed}) elapsed={:.2}s rate={:.0} rec/s ({:.1} MB/s)",
        elapsed, rps, mb_s
    );

    if let Some(path) = args.stats_out.as_deref() {
        let targets: Vec<&str> = [
            oo_enabled.then_some("openobserve"),
            ch_enabled.then_some("clickhouse"),
        ]
        .into_iter()
        .flatten()
        .collect();
        let stats = json!({
            "targets": targets,
            "records_sent": sent,
            "records_failed": failed,
            "openobserve_failed": oo_failed,
            "clickhouse_failed": ch_failed,
            "raw_bytes": bytes_ok,
            "elapsed_s": (elapsed * 1000.0).round() / 1000.0,
            "rate_rec_s": rps.round(),
            "rate_mb_s": (mb_s * 100.0).round() / 100.0,
        });
        if let Err(e) = std::fs::write(path, serde_json::to_vec_pretty(&stats).unwrap()) {
            eprintln!("warning: could not write --stats-out {path}: {e}");
        } else {
            println!("wrote ingest stats -> {path}");
        }
    }

    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}
