A couple<p align="center">
  <img src="images/logo.png" alt="Media Manager" width="96">
</p>

# Unified Logging — Design Proposal

A single log collection, storage, and query service that replaces the
scattered per-service log solutions currently in use across the Media
Manager ecosystem (MediaManager server, transcode buddy, Roku channel,
iOS app, Android TV app, any future Go/Rust services).

Target users: the homeowner operating the NAS, and the Claude Code
coding agent used for development and incident diagnosis.

Status: proposal. No code yet. See the security audit appendix before
implementing.

---

## Goals

- **One schema, every language.** A single protobuf log record definition
  that JVM, Go, Rust, Swift, Kotlin/Android, and BrightScript all emit.
- **Cross-service correlation.** One `correlation_id` threads a user
  action through server, buddy, and clients.
- **Query-friendly for both humans and agents.** Browser UI for
  eyeballing, JSON API designed around LLM context budgets.
- **Operationally simple.** One Docker service, one file on disk, one
  config file.
- **Survives app outages.** Logging stays online when MediaManager is
  down — that's when you need it most.

### Non-goals

- Distributed tracing (spans, waterfalls). A `correlation_id` is enough
  at this scale.
- Metrics. Prometheus already owns that surface via `/metrics`.
- Multi-tenancy. Single household.
- Public-internet exposure. See Security Model below.
- Replacing `docker logs`. Runs alongside; stdout still goes to Docker.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    LAN (172.16.0.0/16)                       │
│                                                              │
│   MediaManager ─┐                                            │
│   Transcode ────┤                                            │
│   buddy         │                                            │
│   iOS app ──────┼──► logcollector (Go, Docker)               │
│   Android TV ───┤    ┌──────────────────────────────┐        │
│   Roku channel ─┘    │  OTLP gRPC :4317             │        │
│                      │  OTLP HTTP :4318             │        │
│                      │  Query API + UI :8088        │        │
│                      │                              │        │
│                      │   ┌──────────────────────┐   │        │
│                      │   │ SQLite + FTS5        │   │        │
│                      │   │ /data/logs/*.db      │   │        │
│                      │   └──────────────────────┘   │        │
│                      └──────────────────────────────┘        │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

A single Go binary provides three facades over one SQLite store:

1. **Ingest** — gRPC (OTLP) for JVM/Go/Rust/iOS/Android; HTTP POST
   batches for Roku's BrightScript.
2. **Store** — SQLite with WAL mode and FTS5 full-text index. Daily
   partitioning for retention cleanup.
3. **Query + UI** — HTTP JSON API (designed for agent consumption) and
   a small server-rendered HTML UI (for humans).

One Docker service, one port for ingest (4317/4318), one port for query
and UI (8088).

---

## Data Model

The wire format is [OpenTelemetry's log data model]. Using OTel's
`.proto` files verbatim gives us free compatibility with every
off-the-shelf log tool, and avoids reinventing a schema.

The central record type (simplified):

```proto
message LogRecord {
  fixed64 time_unix_nano        = 1;   // client-side
  fixed64 observed_time_unix_nano = 11; // server-set on ingest
  SeverityNumber severity_number = 2;   // 1-24 per OTel convention
  string severity_text          = 3;
  AnyValue body                 = 5;   // usually a string
  repeated KeyValue attributes  = 6;   // structured fields
  bytes trace_id                = 9;   // 16 bytes — "correlation_id"
  bytes span_id                 = 10;  // 8 bytes (optional)
  // ... flags, dropped_attributes_count, etc.
}
```

Plus a `Resource` envelope describing the emitting service:

```proto
message Resource {
  repeated KeyValue attributes = 1;
  // Required attrs: service.name, service.instance.id, service.version
}
```

### Conventional attribute keys

Semantic conventions borrowed from OTel. These attribute keys are
reserved and consistent across services:

| Key                          | Meaning                                   |
|------------------------------|-------------------------------------------|
| `service.name`               | `mediamanager-server`, `transcode-buddy`, `roku-channel`, ... |
| `service.instance.id`        | Host name or device UUID                  |
| `service.version`            | App build tag or Docker image tag         |
| `code.namespace`             | Logger / module / file                    |
| `code.function`              | Function name                             |
| `user.id`                    | Authenticated username (redacted if absent) |
| `http.method`, `http.target`, `http.status_code`, `http.response_content_length`, `http.user_agent` | Access-log fields |
| `grpc.method`, `grpc.status_code` | gRPC-specific                         |
| `exception.type`, `exception.message`, `exception.stacktrace` | Error details |

### Access logs vs debug logs

There is no separate access-log record type. An access log is a
`LogRecord` with a conventional set of `http.*` attributes and usually
an INFO severity. This keeps the schema flat and makes
cross-correlation (application log for a request + access log for the
same request) trivial — same `trace_id`.

### Severity mapping

OTel severities 1-24 map to the six conventional levels used elsewhere:

| OTel range | Alias | Notes |
|------------|-------|-------|
| 1-4        | TRACE | disabled by default |
| 5-8        | DEBUG |                      |
| 9-12       | INFO  |                      |
| 13-16      | WARN  |                      |
| 17-20      | ERROR |                      |
| 21-24      | FATAL | application shutdown expected |

---

## Transport

### gRPC / OTLP

The primary path. Clients open a streaming `Export` call to the
collector on port 4317 and stream `ExportLogsServiceRequest` batches.
OTel SDKs in every language-on-our-list do this out of the box with
one config line. No custom client code needed on the JVM, Go, Rust,
iOS, or Android sides.

### HTTP POST / OTLP-HTTP

Same protocol, HTTP instead of gRPC. Same endpoints, same batched
request/response shape. Port 4318. BrightScript on Roku uses this path
because `roUrlTransfer` can POST JSON-encoded OTLP payloads but the
Roku platform has no good gRPC library.

### Client-side buffering

Every SDK keeps a bounded in-memory queue (default: 4 KB messages / 64
KB bytes, whichever hits first). When the collector is unreachable:

1. Drop lowest-severity messages first to make room for new ERROR+
   records.
2. When the queue refills with room, flush on reconnect.
3. Emit a synthetic "dropped N records (severity ≤ DEBUG)" log entry
   on recovery so we don't silently lose visibility.

No client persists logs across process restart. If MediaManager crashes
with unflushed buffered logs, those logs are gone; the process's stderr
copy via Docker is the fallback of last resort.

---

## Storage

SQLite with WAL mode and FTS5.

### Schema

One table per day, created lazily on first write:

```sql
CREATE TABLE IF NOT EXISTS logs_YYYY_MM_DD (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  time_ns       INTEGER NOT NULL,   -- client time, nanos since epoch
  ingest_ns     INTEGER NOT NULL,   -- server time, nanos since epoch
  severity      INTEGER NOT NULL,   -- 1-24 per OTel
  service       TEXT NOT NULL,      -- service.name
  instance      TEXT NOT NULL,      -- service.instance.id
  version       TEXT,               -- service.version
  logger        TEXT,               -- code.namespace
  trace_id      BLOB,               -- 16 bytes or NULL
  span_id       BLOB,               -- 8 bytes or NULL
  message       TEXT NOT NULL,      -- log body as string
  attrs_json    TEXT,               -- remaining attributes as JSON
  exception_json TEXT               -- exception.* bundle or NULL
);

CREATE INDEX ix_logs_YYYY_MM_DD_svc_sev_ts
  ON logs_YYYY_MM_DD (service, severity, time_ns DESC);

CREATE INDEX ix_logs_YYYY_MM_DD_trace
  ON logs_YYYY_MM_DD (trace_id) WHERE trace_id IS NOT NULL;

CREATE VIRTUAL TABLE fts_logs_YYYY_MM_DD USING fts5(
  message,
  service UNINDEXED,
  content='logs_YYYY_MM_DD',
  content_rowid='id'
);
```

FTS5 content is shadowed from the main table via triggers so writes go
to both transparently.

A thin metadata table tracks the active partitions:

```sql
CREATE TABLE partitions (
  day           TEXT PRIMARY KEY,   -- 'YYYY-MM-DD'
  row_count     INTEGER,
  byte_size     INTEGER,
  created_at    INTEGER,
  last_ingest_at INTEGER
);
```

### Retention

Controlled by the `--retention-days N` CLI flag on the collector.
Default: 7.

Every hour, the collector:
1. Lists partition tables older than the retention window.
2. `DROP TABLE logs_YYYY_MM_DD` for each.
3. Deletes the corresponding `fts_logs_YYYY_MM_DD` table.
4. Removes the `partitions` row.
5. `VACUUM` once a week off-peak so the file doesn't balloon.

Dropping a partition is essentially free — no row-by-row delete, no
index rebuild.

### Backup

`cp /data/logs/logs.db /data/logs/logs.db.backup` works because of WAL
mode. For a consistent snapshot, use SQLite's online backup API from a
separate script. Not included in the collector itself.

---

## Query API

All endpoints return JSON. Unauthenticated — see Security Model.

### Schema introspection

```
GET /api/logs/schema
```

Returns the set of services, loggers, and attribute keys currently in
the store. Agents read this first to learn the field namespace.

```json
{
  "services": ["mediamanager-server", "transcode-buddy", "roku-channel"],
  "severities": ["DEBUG", "INFO", "WARN", "ERROR"],
  "attribute_keys": ["http.method", "http.target", "http.status_code", ...],
  "retention_days": 7,
  "oldest_record_ts": "2026-04-06T00:00:00Z",
  "newest_record_ts": "2026-04-13T19:42:33Z"
}
```

### Summary / aggregate

```
GET /api/logs/summary?window=1h&group_by=service,severity
```

Returns bucketed counts — never raw rows. Lets an agent check overall
system health in one response.

```json
{
  "window": "1h",
  "buckets": [
    { "service": "mediamanager-server", "severity": "ERROR", "count": 2 },
    { "service": "mediamanager-server", "severity": "WARN",  "count": 17 },
    { "service": "mediamanager-server", "severity": "INFO",  "count": 8341 },
    { "service": "transcode-buddy",     "severity": "INFO",  "count": 742 }
  ]
}
```

### Recent errors

```
GET /api/logs/errors?window=1h&limit=50
```

Pre-canned "just give me what's broken" query. Default limit trimmed so
the response fits in an agent's context.

### Correlation walk

```
GET /api/logs/correlation/{trace_id}
```

All log records across all services that share the given `trace_id`,
sorted by `time_ns`. The killer feature for cross-service debugging.

### Structured query

```
GET /api/logs/query?service=X&severity=WARN&since=2026-04-13T18:00:00Z&limit=100&cursor=...
```

| Parameter     | Description                                           |
|---------------|-------------------------------------------------------|
| `service`     | exact match, repeatable                               |
| `severity`    | minimum severity level (WARN → returns WARN/ERROR/FATAL) |
| `since`, `until` | RFC3339 time bounds                                |
| `trace_id`    | exact match                                           |
| `text`        | FTS5 query on `message` (supports phrases, NEAR, etc.) |
| `attr.{key}`  | exact match on an attribute value                     |
| `limit`       | hard-capped at 1000; default 100                      |
| `cursor`      | opaque pagination cursor                              |

Response:

```json
{
  "records": [...],
  "count_returned": 100,
  "count_total_estimate": 4823,
  "next_cursor": "opaque...",
  "schema_hash": "..."
}
```

`count_total_estimate` is best-effort (SQLite `COUNT(*)` can be
expensive on large partitions; we stop counting at 10,000 and return
that as a lower bound).

### Live tail

```
GET /api/logs/tail?service=X&severity=INFO
```

Server-Sent Events stream. One JSON log record per SSE event, plus a
heartbeat every 15s so the agent knows the connection is alive. Cursor
resumable so reconnection replays any records received during the gap.

### Health + stats

```
GET /api/logs/health          # liveness
GET /api/logs/stats           # rows/sec, partition sizes, queue depth
```

---

## UI

Server-rendered HTML + a pinch of vanilla JS for the live tail. No
framework. Lives at `/` on port 8088.

Views:
- **Overview**: the summary endpoint rendered as a table. One row per
  service. Last-hour error count prominent.
- **Browse**: filter sidebar (service, severity, time range, free-text
  search) + paginated table. Click a row to expand detail.
- **Detail**: full record including `trace_id` with a "show all logs
  for this trace" link.
- **Tail**: live view of incoming logs matching the current filter.
- **Stats**: partition sizes, ingest rates, retention status.

Designed to be readable on a phone (the homeowner might glance at it
from bed). No auth since the UI is behind the LAN boundary.

---

## Client SDKs

The goal is ~1 hour of integration effort per language. OTel SDKs do
the heavy lifting for most:

| Language / runtime | SDK | Notes |
|--------------------|-----|-------|
| Kotlin (JVM, MediaManager server) | `opentelemetry-java` | Exporter configured for OTLP gRPC; bridge SLF4J → OTel. Replaces current `BufferingServiceProvider` + `AppLogBuffer`. |
| Kotlin (JVM, transcode-buddy)     | `opentelemetry-java` | Same as server. |
| Swift (iOS)                       | `opentelemetry-swift` | OTLP HTTP since iOS networking is more comfortable with HTTP than h2c. |
| Kotlin (Android TV)               | `opentelemetry-kotlin` | OTLP gRPC. |
| Go (any future services)          | `opentelemetry-go`    | OTLP gRPC. |
| Rust (any future services)        | `opentelemetry-rust`  | OTLP gRPC. |
| BrightScript (Roku)               | **Custom, ~200 LOC** | Minimal client: batches JSON log records, POSTs to `/v1/logs` endpoint on port 4318. No OTel SDK exists for BrightScript. |

The custom BrightScript client is the only net-new code. Everything
else is a dependency + config.

### Client configuration

All clients read two values:

- `OTEL_EXPORTER_OTLP_ENDPOINT` — `http://logcollector.lan:4317` for
  LAN services, `http://172.16.4.12:4318` for Roku (HTTP endpoint).
- `OTEL_EXPORTER_OTLP_HEADERS` — `x-logging-api-key=<key>` (see Security
  Model).

---

## Security Model

The collector is **LAN-only**. It is not port-forwarded through the
router, not exposed via HAProxy, not reachable from the internet.

### Read path

Unauthenticated. The query API and UI are readable by anyone on the
home LAN. Rationale: the homeowner is the only human user on the LAN;
every querying service (Angular app, agent curls, homeowner's browser)
already has LAN access. Auth would be friction with zero benefit given
the attacker model.

### Write path

Single shared API key (`LOGGING_API_KEY`), stored in `secrets/.env`
alongside the other service secrets. Every log emitter sends it as a
header: `x-logging-api-key: <value>`. Collector rejects ingest requests
without a valid key.

Rationale: prevents accidental pollution from LAN devices that
shouldn't be writing logs (printer, other people's laptops), not
defense against a determined attacker.

### Rotation

Rotating `LOGGING_API_KEY` requires updating every client's config and
restarting. Not a frequent operation; manual is fine.

### See also

The "Security Audit" appendix at the end of this document identifies
concrete risks with this model and the mitigations we accept.

---

## Operational Concerns

### CLI flags

```
logcollector \
  --data-dir=/data/logs          \
  --retention-days=7             \
  --otlp-grpc-port=4317          \
  --otlp-http-port=4318          \
  --query-port=8088              \
  --log-level=info               \
  --api-key-env=LOGGING_API_KEY
```

All tunable, all documented in `--help`.

### Metrics

The collector exposes its own `/metrics` endpoint for Prometheus:
ingest rate, queue depth, rejected-auth count, SQLite write latency,
partition sizes. Enough to tell if the logging subsystem itself is
healthy.

### Upgrades

Collector binary is stateless. Upgrade = swap image + restart.
In-flight gRPC streams reconnect. Clients buffer during the gap.

Schema changes (rare) ship with a migration script run on startup.
Because partitions are day-scoped, schema drift affects at most the
current day's table — yesterday's data is read-only.

### Resource ceilings

- Memory: 128 MB hard limit in Docker. Go's GC handles it; SQLite
  memory is in file cache.
- Disk: limited by retention × ingest rate. At 7 days and 1M
  records/day at ~250 bytes each, ~1.75 GB. Room to spare on the NAS.
- CPU: one core is enough for target volumes.

---

## Migration from Current State

Today MediaManager has:

- `AppLogBuffer` — in-memory ring of 200 app-log entries, exposed at
  `/admin/logs?format=json`.
- `RequestLogBuffer` — declared but never populated (see [the
  `RequestLogBuffer` note in claude.log for 2026-04-12]).

Migration path:

1. Build the collector and deploy it.
2. Add the OTel bridge to MediaManager (SLF4J → OTel exporter).
3. Keep `AppLogBuffer` as a fallback for a deprecation window.
4. Once the collector has caught one full retention window of data,
   delete `AppLogBuffer` and the unused `RequestLogBuffer`.
5. Repeat for transcode-buddy, iOS, Android TV, Roku.

The agent-facing `/admin/logs` URL on MediaManager stays working during
the deprecation window by proxying to the collector.

---

## Open Questions

To settle before starting implementation:

1. **BrightScript client implementation detail**: does Roku have
   reliable enough TCP for OTLP-HTTP, or do we need UDP fallback /
   local filesystem buffering? Historically Roku networking has been
   flaky.
2. **Exception shape**: OTel has no standard exception-with-stack
   schema (they use conventional attributes `exception.type`,
   `exception.message`, `exception.stacktrace`). Do we want to
   materialize those into a dedicated `exception_json` column, or just
   leave them in `attrs_json` and let the UI detect them?
3. **Correlation ID propagation**: MediaManager's current HTTP services
   and gRPC services don't propagate any trace ID. Bolting this on is
   its own small project — W3C traceparent header on HTTP, `trace-id`
   gRPC metadata key. Do we scope this in Phase 1 or defer?

---

## Implementation Plan

Every phase lands as a shippable improvement — the system is more
useful after each one, not only after the last.

Suggested execution order: **1 → 3 → 2 → 4 → 5 → 6**. Going straight
from basic ingest to integrating MediaManager validates the data
model with real traffic before committing to the full UI and server
feature set.

### Phase 1 — Minimum viable pipeline

One log record can flow end-to-end: `curl` posts an OTLP batch,
Binnacle stores it in SQLite, another `curl` queries it back.

- Vendor OpenTelemetry proto definitions; generate Go bindings.
- SQLite layer using **`modernc.org/sqlite`** (pure Go — keeps the
  distroless base image). Validate FTS5 support early; if the pure-Go
  driver lacks it, fall back to `LIKE`-based search until Phase 2.
- **Flyway-style schema migration framework**. Numbered SQL files
  under `internal/store/migrations/V{NNN}__{description}.sql`,
  embedded in the binary via `go:embed`, applied in order on startup,
  tracked in a `schema_migrations` table. Required so we can evolve
  the control-plane schema (metadata tables, future write-key tables,
  etc.) without manual DB surgery across deployments. Daily partition
  tables (`logs_YYYY_MM_DD`) are created dynamically by the writer
  and NOT managed by migrations — their shape lives in code.
- Write path: OTLP HTTP at `POST /v1/logs` with API key validation.
  One background goroutine owns all SQLite writes (fed by a bounded
  channel) to avoid writer contention.
- Read path: `GET /api/logs/query` with basic filters (service,
  severity, since, limit) and stable cursor pagination.
- Retention loop: hourly `DROP TABLE` on partitions older than
  `--retention-days`.

**Done when:** `curl -X POST .../v1/logs` writes, `curl .../api/logs/query`
reads it back, and retention reliably drops yesterday's partition.

### Phase 2 — Server feature-complete

Every endpoint from the design doc works. Browser UI lets you eyeball
incoming logs. Prometheus can scrape Binnacle's own health.

- OTLP gRPC ingest on 4317 feeding the same writer goroutine.
- Remaining query endpoints: `/schema`, `/summary`, `/errors`,
  `/correlation/{id}`, `/tail` (Server-Sent Events), `/stats`.
- HTML UI served from the same binary (`html/template` + vanilla JS).
- Prometheus metrics on `/metrics`: ingest rate, rejected-auth count,
  SQLite write latency, partition sizes, client queue depth.
- Safety rails from the security audit: 64 KB per-record cap, 4 MB
  per-batch cap, ANSI-escape stripping at ingest.

### Phase 3 — First real client (MediaManager)

MediaManager's production logs flow to Binnacle; `AppLogBuffer`
becomes redundant.

- Add `opentelemetry-java` SDK + OTLP exporter to MediaManager.
- Bridge SLF4J → OTel (logback appender or equivalent).
- Central redaction config for URLs with `?key=`, `Authorization`
  headers, anything matching the existing `UriCredentialRedactor`.
- Run in parallel with `AppLogBuffer` for one full retention period
  (7 days), then delete `AppLogBuffer`, `RequestLogBuffer`, and the
  `/admin/logs` + `/admin/requests` endpoints.

### Phase 4 — Remaining clients

Every app in the ecosystem logs to Binnacle, in order of difficulty:

1. **transcode-buddy** — same JVM + OTel SDK as MediaManager.
2. **iOS app** — `opentelemetry-swift` with OTLP HTTP exporter.
3. **Android TV** — `opentelemetry-kotlin-android` with OTLP gRPC.
4. **Roku BrightScript** — custom client (~200 LOC) batching JSON
   POSTs to `/v1/logs`. Local ring buffer for flaky networks.

### Phase 5 — Correlation IDs and security hardening

One log line from the Roku can be walked back through the full
request chain. Write-path keys move to per-service with attribution.

- W3C `traceparent` propagation on HTTP; gRPC metadata trace-id on
  Armeria. Server-generated `trace_id` when clients don't supply one.
- Per-service write keys in a `write_keys` table (added via a new
  migration). Collector reloads on SIGHUP.
- Attribution pin: collector overrides `service.name` on ingested
  records to match the authenticating key. A compromised iOS key
  cannot forge records claiming to be MediaManager.
- Per-key rate limiting (token bucket, 10 k records/minute).
- Wire the `--require-read-auth` toggle from security audit A1 (off
  by default).

### Phase 6 — Ops polish

- `govulncheck` in CI; block merges on HIGH+ Go CVEs.
- Daily cold-archive of old partitions to compressed files.
- Pre-computed hourly summary rollups so `/summary` is O(hours)
  instead of O(records).
- Simple alerting: webhook on ERROR rate threshold.
- Backup script: SQLite online backup → file on NAS share → daily
  cron.
- UI polish: attribute-key autocomplete, saved filter presets.

---

## Security Audit (appendix)

A critical review of the security model above. Each finding lists
severity and the decision we're making. "Accept" means we understand
the risk and have chosen to accept it for now; "Mitigate" means the
design has been or will be updated.

### A1. Unauthenticated read path — **HIGH by conventional standards, Accept for this context**

*Finding*: anyone on the LAN can read all logs without credentials,
including logs that may contain:
- User names and activity (who played what, when)
- HTTP request paths (which expose titles, queries, internal IDs)
- IP addresses of authenticated clients
- Exception messages with system internals, file paths, DB error
  strings (potentially revealing schema)
- Query parameters that, despite SDK-side redaction, might slip through

*Impact*: a LAN attacker (compromised IoT device, guest Wi-Fi user
with wrong VLAN config, visiting houseguest on the main network) can
reconstruct household viewing habits and probe for vulnerabilities in
MediaManager by reading its error messages.

*Decision*: **Accept, with caveats.** The homeowner has stated the
LAN is the trust boundary. Mitigations we'll apply anyway because they
are cheap:

1. **SDK-side redaction config** is mandatory (already in the design).
   Passwords, tokens, `?key=` URL params, auth cookies must never be
   emitted. Redaction runs in every SDK *before* the log leaves the
   emitter.
2. **Document the attacker model explicitly.** The `docs/ADMIN_GUIDE.md`
   must state: "anyone with LAN access can read logs — do not connect
   untrusted devices to the logging LAN segment."
3. **Future hook**: add a config flag `--require-read-auth` that, when
   set, gates the read path behind the same shared API key. Trivial to
   wire in advance; flip it on the day a guest network falls through a
   VLAN boundary.

### A2. Single shared write-path API key — **MEDIUM, Accept with monitoring**

*Finding*: all emitters share one key. Consequences:

- **No attribution.** If a malfunctioning or compromised client starts
  spamming the collector, we can't identify or revoke it without
  rotating the key everywhere.
- **Key compromise blast radius is total.** One client compromised →
  attacker can emit arbitrary log records impersonating any service,
  poisoning the log of record during an incident.
- **Rotation friction.** Changing the key means rebuilding / redeploying
  every client (iOS, Android TV, Roku, server, buddy).

*Impact*: limited real-world — the clients are all in-house and the
LAN is trusted — but the log store becomes a plausible forgery vector
during a post-incident investigation. An attacker who owns one service
can emit fake log records saying another service did something bad.

*Decision*: **Accept for Phase 1, plan to fix in Phase 2.** Mitigations:

1. **Per-service keys** should land in Phase 2 before we onboard the
   mobile apps. Each service (`mediamanager-server`,
   `transcode-buddy`, `roku-channel`, `ios-app`, `android-tv-app`) gets
   its own key. Keys are long random strings in `secrets/.env`.
2. **Server-side attribution check**: the collector pins the
   `service.name` attribute in each ingested record to the identity
   implied by the authenticating key. A client presenting the
   `ios-app` key cannot claim `service.name=mediamanager-server`. This
   prevents forgery cross-service.
3. **Rate limiting per key**: hard cap at 10,000 records/minute per
   key. A compromised or buggy client can't flood the store faster
   than we can notice.
4. **Revocation list**: keys stored in a SQLite table (not only in
   env), collector reloads on SIGHUP, revocation takes seconds not a
   rebuild.

### A3. Log injection / stored XSS — **MEDIUM, Mitigate**

*Finding*: log messages are user-controlled (an attacker crafts an
HTTP path that becomes a log line). If the browser UI renders message
text without escaping, `<script>` in a log message executes in the
viewer's browser.

*Impact*: the UI is LAN-only and viewed only by the homeowner, but
Claude Code (an agent) may fetch the same URL and render it via a
browser tool in the future. XSS in logs is a well-known class bug.

*Decision*: **Mitigate.** The design is updated:

1. **Go's `html/template`** (not `text/template`) for all rendering.
   Auto-escapes by default.
2. **JSON API** responses are just JSON; clients control rendering.
3. **Log-line ANSI escape codes** stripped at ingest so a raw terminal
   dump of the DB doesn't echo attacker-controlled escape sequences.
   (This is already best practice — cf. the existing
   `sanitizeFfmpegOutput` in `VideoProbe.kt`.)

### A4. Message body DoS — **LOW, Mitigate**

*Finding*: a client can emit very large log messages (a multi-megabyte
stack trace, a full HTTP body). Unbounded, these fill disk fast.

*Decision*: **Mitigate.**

1. **Per-record size cap** of 64 KB at ingest. Oversize records are
   truncated with a `truncated=true` attribute and a count incremented.
2. **Per-batch size cap** of 4 MB. OTLP spec supports this.
3. **Partition-level disk guard**: if today's partition exceeds 2 GB,
   the collector starts dropping DEBUG-and-below records. Warn loudly.

### A5. SQLite file integrity — **LOW, Accept**

*Finding*: anyone with file-level NAS access (SSH, SMB with admin
creds) can modify the log DB directly, erasing or forging records.

*Decision*: **Accept.** The NAS filesystem is the root of trust. If
someone has shell access to the NAS, log tampering is the least of our
problems. Not a threat we attempt to defend against.

### A6. Correlation ID leaks to logs — **LOW, Accept**

*Finding*: `trace_id` is intended to correlate across services, but
it's visible in URLs and UIs. Knowing a `trace_id` does not grant any
capability in our model (there's no "replay this request" endpoint
that takes one). Accept.

### A7. No TLS on ingest / query ports — **INFORMATIONAL**

*Finding*: the Docker container exposes ports 4317, 4318, 8088 in
plaintext. All data — logs and queries — flows in the clear on the
LAN.

*Decision*: **Accept for LAN-only deployment.** If at any future point
we expose a port to the internet, this is a blocking issue and must be
fronted by TLS (HAProxy, same pattern as the main MediaManager
endpoint).

### A8. Timestamp forgery — **LOW, Mitigate partially**

*Finding*: client-side `time_unix_nano` is attacker-controlled. A
rogue client can emit records with timestamps days in the past or the
future, polluting historical analysis.

*Decision*: **Mitigate partially.** The collector always sets
`observed_time_unix_nano` on ingest. Queries default to sorting by
client time (so bad clocks look like late-arriving data), but the UI
shows both timestamps when they diverge by more than 5 minutes. An
attacker can confuse casual chronological analysis but cannot
retroactively insert records into dropped partitions.

### Audit Summary

| ID | Title | Severity | Decision |
|----|-------|----------|----------|
| A1 | Unauthenticated read path | HIGH (conventional) | Accept for LAN-only; document; keep `--require-read-auth` hook |
| A2 | Single shared write key | MEDIUM | Accept Phase 1; per-service keys in Phase 2 |
| A3 | Log injection / XSS in UI | MEDIUM | Mitigate (auto-escape, strip ANSI) |
| A4 | Oversized record DoS | LOW | Mitigate (size caps) |
| A5 | SQLite file tampering | LOW | Accept (NAS is root of trust) |
| A6 | trace_id leak | LOW | Accept |
| A7 | No TLS | INFO | Accept for LAN-only |
| A8 | Client timestamp forgery | LOW | Mitigate (observed_ts) |

The design is safe to implement as specified for a LAN-only deployment
in a single-occupant household. Two items (A2, A3) imply concrete
additions to the Phase 1 scope; three (A1, A4, A8) imply
straightforward safety rails that add little complexity. One (A7) is a
hard gate on any future internet exposure and must be revisited before
that transition.

[OpenTelemetry's log data model]: https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/logs/data-model.md
