// Log line templates harvested from openobserve test fixtures
// (tests/test-data/logs_data.json, tests/test-data/70_fields.json, etc.).
//
// Each `tpl_*` function returns a single log line built from a fixed skeleton
// plus randomly drawn slot values, producing a wide vocabulary of tokens for
// downstream tantivy indexing benchmarks.

use chrono::{DateTime, Duration, Utc};
use rand::Rng;
use rand_chacha::ChaCha8Rng;

pub const LEVELS_LOWER: &[&str] = &["info", "warn", "error", "debug", "trace"];
pub const LEVELS_UPPER: &[&str] = &["INFO ", "WARN ", "ERROR", "DEBUG", "TRACE"];
pub const GLOG_PREFIX: &[char] = &['I', 'W', 'E', 'F'];

pub const COMPONENTS: &[&str] = &[
    "k8s_client_runtime",
    "controller-manager",
    "scheduler",
    "etcd-server",
    "csi-driver",
    "prometheus-operator",
    "ingress-controller",
    "fluent-bit",
    "fluent-bit::output",
    "kube-proxy",
    "kubelet",
    "containerd",
    "cri-o",
    "calico-node",
    "coredns",
    "node-exporter",
    "vector",
    "loki-promtail",
];

pub const KLOG_FUNCS: &[&str] = &[
    "Warningf",
    "Errorf",
    "Infof",
    "Debugf",
    "ErrorDepth",
    "FatalDepth",
    "V(2).Infof",
];

pub const GO_MODULE_PATHS: &[&str] = &[
    "github.com/kubernetes-csi/external-snapshotter/client/v6/informers/externalversions/factory.go:117",
    "pkg/mod/k8s.io/client-go@v0.25.1/tools/cache/reflector.go:169",
    "k8s.io/apimachinery/pkg/util/wait/wait.go:155",
    "vendor/k8s.io/apiserver/pkg/server/options/etcd.go:208",
    "github.com/prometheus/client_golang/prometheus/registry.go:312",
    "google.golang.org/grpc/server.go:1145",
    "github.com/fsnotify/fsnotify@v1.6.0/inotify.go:120",
    "github.com/spf13/cobra@v1.7.0/command.go:944",
    "github.com/coreos/etcd/etcdserver/api/v3rpc/lease.go:113",
    "go.etcd.io/raft/v3/raft.go:1066",
];

pub const RUST_MODULE_PATHS: &[&str] = &[
    "zinc_enl::handlers::search",
    "zinc_enl::grpc::search",
    "zinc_enl::service::storage::s3",
    "zinc_enl::service::search::cache",
    "zinc_enl::service::search::sql",
    "zinc_enl::core::cache",
    "zinc_enl::handlers::ingest::bulk",
    "zinc_enl::handlers::ingest::json",
    "zinc_enl::handlers::dashboards",
    "zinc_enl::meta::stream",
    "zinc_enl::common::auth",
    "zinc_enl::infra::wal",
    "zinc_enl::infra::cluster",
    "zinc_enl::service::compact",
    "zinc_enl::service::file_list",
    "aws_config::default_provider::credentials",
    "tokio::runtime::scheduler",
    "h2::proto::streams",
    "tower::buffer::worker",
];

pub const K8S_RESOURCES: &[&str] = &[
    "*v1.Pod",
    "*v1.Service",
    "*v1.ConfigMap",
    "*v1.Secret",
    "*v1.Deployment",
    "*v1.ReplicaSet",
    "*v1.StatefulSet",
    "*v1.DaemonSet",
    "*v1.Node",
    "*v1.PersistentVolumeClaim",
    "*v1.VolumeSnapshotClass",
    "*v1.VolumeSnapshotContent",
    "*v1.Endpoints",
    "*v1.Ingress",
    "*v1.NetworkPolicy",
    "*v1.ServiceAccount",
    "*v1.Job",
    "*v1.CronJob",
];

pub const K8S_PLURALS: &[&str] = &[
    "pods",
    "services",
    "configmaps",
    "secrets",
    "deployments",
    "replicasets",
    "statefulsets",
    "daemonsets",
    "nodes",
    "persistentvolumeclaims",
    "volumesnapshotclasses.snapshot.storage.k8s.io",
    "volumesnapshotcontents.snapshot.storage.k8s.io",
    "endpoints",
    "ingresses.networking.k8s.io",
    "networkpolicies.networking.k8s.io",
    "serviceaccounts",
    "jobs.batch",
    "cronjobs.batch",
];

pub const NAMESPACES: &[&str] = &[
    "monitoring",
    "kube-system",
    "default",
    "ingress-nginx",
    "logging",
    "payments",
    "checkout",
    "search",
    "auth",
    "observability",
    "platform",
    "data",
    "operators",
    "argocd",
    "cert-manager",
    "vault",
    "istio-system",
    "linkerd",
];

pub const SERVICE_ACCOUNTS: &[&str] = &[
    "prometheus-k8s",
    "kube-proxy",
    "default",
    "fluent-bit",
    "argocd-application-controller",
    "vault",
    "external-dns",
    "csi-snapshotter",
    "ingress-nginx",
    "cert-manager",
    "node-exporter",
    "alertmanager",
];

pub const ORG_NAMES: &[&str] = &[
    "Ashish_organization_39",
    "Bhargav_organization_29",
    "demo_org1_n976k98gUMT17m3",
    "Acme_organization_07",
    "Initech_organization_13",
    "GlobalCorp_organization_56",
    "WidgetCo_organization_44",
    "Cyberdyne_organization_88",
    "Hooli_organization_21",
    "Pied_Piper_organization_03",
    "Stark_organization_99",
    "Wayne_organization_42",
];

pub const STREAM_NAMES: &[&str] = &[
    "olympics",
    "Sample",
    "test",
    "default",
    "olympics_schema",
    "app_logs",
    "metrics",
    "traces",
    "events",
    "audit",
    "k8s_logs",
    "nginx_access",
    "billing",
    "ingest_pipeline",
];

pub const API_PATH_TEMPLATES: &[&str] = &[
    "/api/{org}/_bulk",
    "/api/{org}/streams?type=logs",
    "/api/{org}/streams?type=metrics",
    "/api/{org}/streams?type=traces",
    "/api/{org}/transform",
    "/api/{org}/_search?type=logs",
    "/api/{org}/{stream}/schema",
    "/api/{org}/{stream}/_json",
    "/api/{org}/{stream}/_multi",
    "/api/{org}/dashboards",
    "/api/{org}/functions",
    "/api/{org}/alerts",
    "/api/{org}/syslog-routes",
    "/api/org_users/{org}",
    "/api/organizations",
    "/api/organizations/passcode/{org}",
    "/api/organizations?page_num=0&page_size=1000&sort_by=id&desc=false&name=",
    "/api/users",
    "/api/users/verifyuser/*****@domain.com",
    "/api/auth/refresh_token",
    "/api/auth/callback?code={uuid}",
    "/api/auth/login",
    "/api/billings/quota_threshold/{org}",
    "/api/billings/invoice/{org}/2024",
    "/api/clusters",
    "/api/nodes/online",
    "/healthz",
    "/readyz",
    "/metrics",
];

pub const HTTP_VERSIONS: &[&str] = &["HTTP/1.1", "HTTP/2.0", "HTTP/1.0"];

pub const NGINX_USER_AGENTS: &[&str] = &[
    "Fluent-Bit",
    "PostmanRuntime/7.29.0",
    "go-resty/2.7.0 (https://github.com/go-resty/resty)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "curl/8.6.0",
    "kube-probe/1.29",
    "Go-http-client/1.1",
    "Go-http-client/2.0",
    "python-requests/2.31.0",
    "okhttp/4.12.0",
    "Java/17.0.9",
    "Vector/0.34.0",
];

pub const NGINX_REFERERS: &[&str] = &[
    "-",
    "https://cloud.zincsearch.com/",
    "https://accounts.google.com/",
    "https://app.example.com/dashboard",
    "https://app.example.com/orders/list",
    "https://app.example.com/search?q=widgets",
    "https://partner.example.com/integrations",
    "https://www.google.com/",
    "https://github.com/openobserve/openobserve",
];

pub const HTTP_METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "HEAD"];

pub const HTTP_STATUSES: &[u16] = &[
    200, 201, 202, 204, 301, 302, 304, 400, 401, 403, 404, 408, 409, 422, 429, 500, 502, 503, 504,
];

pub const GRPC_SERVICES: &[&str] = &[
    "search.v1.SearchService",
    "ingest.v1.Ingester",
    "auth.v1.AuthService",
    "stream.v1.StreamService",
    "metadata.v1.SchemaService",
    "compactor.v1.CompactService",
    "router.v1.RouteService",
    "wal.v1.WalService",
];

pub const GRPC_METHODS: &[&str] = &[
    "Search",
    "Ingest",
    "Authenticate",
    "ListStreams",
    "DescribeStream",
    "Compact",
    "Route",
    "Append",
    "Flush",
    "Subscribe",
];

pub const GRPC_CODES: &[&str] = &[
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

pub const DB_TABLES: &[&str] = &[
    "users",
    "orders",
    "payments",
    "carts",
    "sessions",
    "audit_log",
    "invoices",
    "subscriptions",
    "webhooks",
    "feature_flags",
    "tenants",
    "api_keys",
    "billing_events",
    "schema_history",
    "dashboards",
    "alerts",
];

pub const DB_OPS: &[&str] = &["SELECT", "UPDATE", "DELETE", "INSERT", "UPSERT"];

pub const DB_NAMES: &[&str] = &["postgres", "mysql", "sqlite", "cockroach", "clickhouse"];

pub const SIMPLE_MESSAGES: &[&str] = &[
    "User login successful",
    "User logout successful",
    "Password reset initiated",
    "Password reset completed",
    "KYC verification completed",
    "Payment processed",
    "Payment failed",
    "Theme preferences updated",
    "System metrics report",
    "Error verifying token: Token is expired",
    "FindOrganizationByIdentifier: Fetch from the database",
    "FindUserByEmail: Fetch from the database",
    "GetMonthlyIngestQuotaByOrgIdentifier: Fetch from the database",
    "GetMonthlySearchQuotaByOrgIdentifier: Fetch from the database",
    "Cache miss; populating from upstream store",
    "Background worker tick: processed zero jobs",
    "Bootstrapping schema registry from disk",
    "Compaction merged seven segments into one",
    "Detected leader change in raft cluster",
    "Regular log entry without sensitive data",
    "Streaming subscriber connected",
    "Streaming subscriber disconnected",
    "Reaping orphaned tasks older than sixty seconds",
    "Reloading TLS certificate from secret",
    "Heartbeat sent to coordinator",
    "Migration applied successfully",
    "Scheduler enqueued backfill batch",
    "Queue depth nominal",
    "Rate limit threshold approached for tenant",
    "Webhook delivery acknowledged",
    "Webhook delivery retried after backoff",
    "Snapshot restore finished cleanly",
    "Permission denied while accessing resource",
    "Configuration reloaded from configmap",
    "Skipping malformed record during decode",
    "Async pipeline drained successfully",
    "Idempotency key recognized; reusing prior response",
    "Cluster membership changed",
    "S3 multipart upload completed",
    "Cron job triggered manual catchup",
];

pub const QUERIER_KINDS: &[&str] = &["querier", "ingester", "router", "compactor", "alertmanager"];
pub const ROLES: &[&str] = &["Querier", "Ingester", "Compactor", "Router", "Alertmanager"];

pub const EXCEPTIONS: &[&str] = &[
    "java.net.SocketTimeoutException: Read timed out after 5000ms",
    "io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: deadline exceeded after 4.999s",
    "psycopg2.OperationalError: server closed the connection unexpectedly",
    "redis.exceptions.ConnectionError: Error 111 connecting to redis:6379. Connection refused.",
    "context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
    "tokio::time::error::Elapsed: deadline has elapsed",
    "anyhow::Error: failed to acquire database connection from pool",
    "kafka.common.errors.NotLeaderForPartitionException: This server is not the leader for that topic-partition.",
    "java.lang.NullPointerException: Cannot invoke method on null reference",
    "OSError: [Errno 28] No space left on device",
];

pub const STACK_FRAMES: &[&str] = &[
    "at com.example.gateway.RequestRouter.dispatch(RequestRouter.java:142)",
    "at com.example.payments.StripeAdapter.charge(StripeAdapter.java:88)",
    "at org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:1067)",
    "at io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:379)",
    "at runtime.goexit (asm_amd64.s:1650)",
    "at tokio::runtime::task::raw::poll (tokio-1.39.2/src/runtime/task/raw.rs:201)",
    "at hyper::server::conn::http2::serve (hyper-1.0.1/src/server/conn/http2.rs:118)",
    "at sqlalchemy.orm.session.Session.flush (orm/session.py:3845)",
];

const TEMPLATE_BASE_TIMESTAMP_SECS: i64 = 1_735_689_600; // 2025-01-01T00:00:00Z

pub fn pick<'a, T>(rng: &mut ChaCha8Rng, xs: &'a [T]) -> &'a T {
    &xs[rng.gen_range(0..xs.len())]
}

pub fn rand_timestamp_iso(rng: &mut ChaCha8Rng) -> String {
    // Spread synthetic timestamps over a fixed 30-day window so date tokens vary
    // without making generated rows depend on the machine clock.
    let base: DateTime<Utc> = DateTime::from_timestamp(TEMPLATE_BASE_TIMESTAMP_SECS, 0)
        .expect("valid template base timestamp");
    let secs = rng.gen_range(0..30 * 24 * 3600);
    let ts: DateTime<Utc> = base - Duration::seconds(secs);
    ts.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

pub fn rand_glog_ts(rng: &mut ChaCha8Rng) -> String {
    let month = rng.gen_range(1..=12);
    let day = rng.gen_range(1..=28);
    let h = rng.gen_range(0..24);
    let m = rng.gen_range(0..60);
    let s = rng.gen_range(0..60);
    let us = rng.gen_range(0..1_000_000);
    format!(
        "{:02}{:02} {:02}:{:02}:{:02}.{:06}",
        month, day, h, m, s, us
    )
}

pub fn rand_nginx_ts(rng: &mut ChaCha8Rng) -> String {
    const MONTHS: &[&str] = &[
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let mi = rng.gen_range(0..12);
    let day = rng.gen_range(1..=28);
    let year = 2022 + rng.gen_range(0..4);
    let h = rng.gen_range(0..24);
    let m = rng.gen_range(0..60);
    let s = rng.gen_range(0..60);
    format!(
        "[{:02}/{}/{}:{:02}:{:02}:{:02} +0000]",
        day, MONTHS[mi], year, h, m, s
    )
}

pub fn rand_ip(rng: &mut ChaCha8Rng) -> String {
    format!(
        "10.{}.{}.{}",
        rng.gen_range(0..256),
        rng.gen_range(0..256),
        rng.gen_range(1..255),
    )
}

pub fn rand_uuid(rng: &mut ChaCha8Rng) -> String {
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

pub fn rand_hex(rng: &mut ChaCha8Rng, len: usize) -> String {
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let v: u8 = rng.gen_range(0..16);
        if v < 10 {
            s.push((b'0' + v) as char);
        } else {
            s.push((b'a' + v - 10) as char);
        }
    }
    s
}

pub fn rand_parquet_id(rng: &mut ChaCha8Rng) -> u64 {
    rng.gen_range(7_000_000_000_000_000_000_u64..7_999_999_999_999_999_999_u64)
}

pub fn fill_api_path(rng: &mut ChaCha8Rng, tpl: &str) -> String {
    let mut out = tpl.replace("{org}", pick(rng, ORG_NAMES).as_ref());
    if out.contains("{stream}") {
        out = out.replace("{stream}", pick(rng, STREAM_NAMES).as_ref());
    }
    if out.contains("{uuid}") {
        out = out.replace("{uuid}", &rand_uuid(rng));
    }
    out
}

// Nginx combined log line: matches the shape seen in tests/test-data/logs_data.json.
pub fn tpl_nginx_access(rng: &mut ChaCha8Rng) -> String {
    let ip = rand_ip(rng);
    let user = if rng.gen_bool(0.5) {
        "*****@domain.com"
    } else {
        "-"
    };
    let ts = rand_nginx_ts(rng);
    let method = pick(rng, HTTP_METHODS);
    let path_tpl = *pick(rng, API_PATH_TEMPLATES);
    let path = fill_api_path(rng, path_tpl);
    let http_ver = pick(rng, HTTP_VERSIONS);
    let status = *pick(rng, HTTP_STATUSES);
    let body_bytes = rng.gen_range(50..30_000);
    let referer = pick(rng, NGINX_REFERERS);
    let ua = pick(rng, NGINX_USER_AGENTS);
    let req_len = rng.gen_range(40..3000);
    let req_t = (rng.gen_range(1..2000) as f64) / 1000.0;
    let upstream_addr = format!("{}:{}", rand_ip(rng), rng.gen_range(3000..9000));
    let upstream_resp_t = (rng.gen_range(0..2000) as f64) / 1000.0;
    let hash = rand_hex(rng, 32);
    format!(
        "{ip} - {user} {ts} \"{method} {path} {http_ver}\" {status} {body_bytes} \"{referer}\" \"{ua}\" {req_len} {req_t:.3} [zinc-cp1-zinc-cp-4082] [] {upstream_addr} {body_bytes} {upstream_resp_t:.3} {status} {hash}"
    )
}

// Klog ts= caller= level= component= shape.
pub fn tpl_klog(rng: &mut ChaCha8Rng) -> String {
    let ts = rand_timestamp_iso(rng);
    let caller_line = rng.gen_range(50..400);
    let level = pick(rng, LEVELS_LOWER);
    let component = pick(rng, COMPONENTS);
    let func = pick(rng, KLOG_FUNCS);
    let module = pick(rng, GO_MODULE_PATHS);
    let res = pick(rng, K8S_RESOURCES);
    let plural = pick(rng, K8S_PLURALS);
    let ns = pick(rng, NAMESPACES);
    let sa = pick(rng, SERVICE_ACCOUNTS);
    format!(
        "ts={ts} caller=klog.go:{caller_line} level={level} component={component} func={func} msg=\"{module}: failed to list {res}: {plural} is forbidden: User \\\"system:serviceaccount:{ns}:{sa}\\\" cannot list resource \\\"{plural}\\\" in API group \\\"\\\" at the cluster scope\""
    )
}

// glog-style line: "E1227 14:10:20.433435 1 reflector.go:138] ..."
pub fn tpl_glog_reflector(rng: &mut ChaCha8Rng) -> String {
    let pfx = pick(rng, GLOG_PREFIX);
    let ts = rand_glog_ts(rng);
    let thread = rng.gen_range(1..50);
    let line = rng.gen_range(50..400);
    let module = pick(rng, GO_MODULE_PATHS);
    let res = pick(rng, K8S_RESOURCES);
    let plural = pick(rng, K8S_PLURALS);
    format!(
        "{pfx}{ts}       {thread} reflector.go:{line}] {module}: Failed to watch {res}: failed to list {res}: the server could not find the requested resource (get {plural})"
    )
}

// Rust env_logger-style: "[2022-12-27T14:10:03Z INFO  zinc_enl::handlers::search] ..."
pub fn tpl_rust_envlog(rng: &mut ChaCha8Rng) -> String {
    let ts = rand_timestamp_iso(rng);
    let lvl = pick(rng, LEVELS_UPPER);
    let module = pick(rng, RUST_MODULE_PATHS);
    let payload = match rng.gen_range(0..7) {
        0 => format!(
            "[TRACE] cluster->node: Node {{ id: {}, uuid: \"{}\", name: \"ziox-{}-{}\", addr: \"http://{}:{}\", role: [{}], cpu_num: {}, status: Online }}",
            rng.gen_range(1..10),
            rand_uuid(rng),
            pick(rng, QUERIER_KINDS),
            rand_hex(rng, 6),
            rand_ip(rng),
            rng.gen_range(5080..5090),
            pick(rng, ROLES),
            *pick(rng, &[2u32, 4, 8, 16, 32]),
        ),
        1 => format!(
            "[TRACE] cluster->file_list: num: {}, offset: {}",
            rng.gen_range(0..200),
            rng.gen_range(0..200)
        ),
        2 => format!(
            "[TRACE] cluster->partition: node: {}, is_querier: {}, file_range: {}-{}",
            rng.gen_range(1..10),
            rng.gen_bool(0.5),
            rng.gen_range(0..100),
            rng.gen_range(100..1000),
        ),
        3 => format!(
            "[TRACE] cluster->search: total: {}, took: {}, scan_size: {}",
            rng.gen_range(0..100_000),
            rng.gen_range(0..2000),
            rng.gen_range(0..10_000_000),
        ),
        4 => format!(
            "[JOB] File upload begin: local: data/wal/{}/logs/{}/0_2024_05_28_14_{}.json, size: {}",
            pick(rng, ORG_NAMES),
            pick(rng, STREAM_NAMES),
            rand_parquet_id(rng),
            rng.gen_range(100..1_000_000),
        ),
        5 => format!(
            "[JOB] File upload success: local: data/wal/{}/logs/{}/0_2024_05_28_14_{}.json, remote: {}/logs/{}/2024/05/28/14/{}.parquet",
            pick(rng, ORG_NAMES),
            pick(rng, STREAM_NAMES),
            rand_parquet_id(rng),
            pick(rng, ORG_NAMES),
            pick(rng, STREAM_NAMES),
            rand_parquet_id(rng),
        ),
        6 => format!(
            "[TRACE] sqlparser: index -> \"{}\", fields -> [], partition_key -> [], full_text -> [], time_range -> Some(({}, {})), order_by -> [], limit -> 0,{}",
            pick(rng, STREAM_NAMES),
            rng.gen_range(1_600_000_000_000_000_i64..1_700_000_000_000_000_i64),
            rng.gen_range(1_700_000_000_000_000_i64..1_800_000_000_000_000_i64),
            *pick(rng, &[50, 100, 500, 1000, 5000]),
        ),
        _ => unreachable!(),
    };
    format!("[{ts} {lvl} {module}] {payload}")
}

pub fn tpl_simple(rng: &mut ChaCha8Rng) -> String {
    pick(rng, SIMPLE_MESSAGES).to_string()
}

pub fn tpl_db_query(rng: &mut ChaCha8Rng) -> String {
    let table = pick(rng, DB_TABLES);
    let op = pick(rng, DB_OPS);
    let took = (rng.gen_range(1..5000) as f64) / 100.0;
    let rows = rng.gen_range(0..50_000);
    let id = rng.gen_range(1..1_000_000);
    let db = pick(rng, DB_NAMES);
    format!(
        "query={op} table={table} rows={rows} took_ms={took:.2} where id={id} db={db} pool=primary tenant={} trace_id={}",
        pick(rng, ORG_NAMES),
        rand_hex(rng, 16),
    )
}

pub fn tpl_grpc(rng: &mut ChaCha8Rng) -> String {
    let svc = pick(rng, GRPC_SERVICES);
    let m = pick(rng, GRPC_METHODS);
    let code = pick(rng, GRPC_CODES);
    let peer = format!("{}:{}", rand_ip(rng), rng.gen_range(20_000..60_000));
    let dur = rng.gen_range(1..2000);
    format!(
        "grpc service={svc} method={m} peer={peer} duration_ms={dur} code={code} request_id={} authority={}.svc.cluster.local",
        rand_uuid(rng),
        pick(
            rng,
            &["search", "ingester", "router", "alertmanager", "compactor"]
        ),
    )
}

pub fn tpl_exception(rng: &mut ChaCha8Rng) -> String {
    let exc = pick(rng, EXCEPTIONS);
    let f1 = pick(rng, STACK_FRAMES);
    let f2 = pick(rng, STACK_FRAMES);
    let f3 = pick(rng, STACK_FRAMES);
    format!("exception=\"{exc}\" stack=\"{f1} | {f2} | {f3}\"")
}

// Weighted selector over the templates above.
pub fn generate_log_line(rng: &mut ChaCha8Rng) -> String {
    match rng.gen_range(0..100) {
        0..=24 => tpl_nginx_access(rng),
        25..=44 => tpl_rust_envlog(rng),
        45..=59 => tpl_klog(rng),
        60..=69 => tpl_glog_reflector(rng),
        70..=79 => tpl_db_query(rng),
        80..=87 => tpl_grpc(rng),
        88..=92 => tpl_exception(rng),
        _ => tpl_simple(rng),
    }
}
