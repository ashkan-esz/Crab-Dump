# ADR-0001: Multi-Database Support

| Field       | Value                                       |
|-------------|---------------------------------------------|
| **Status**  | Proposed                                    |
| **Date**    | 2026-08-08                                  |
| **Authors** | crab-dump maintainers                       |
| **Supersedes** | N/A                                      |

---

## Context

crab-dump currently backs up exactly one PostgreSQL database per invocation.
The single `DATABASE_URL` environment variable drives the entire pipeline —
spawning `pg_dump`, streaming through compression and optional encryption,
chunking to Telegram-safe sizes, and uploading.

As use cases grow, operators need to back up databases on different servers
(e.g., production vs analytics), each with its own connection string, network
topology, and dump parameters. Backing up these databases currently requires
running separate processes sequentially, which is slow and error-prone.

## Decision

Support backing up multiple independent PostgreSQL servers in a single run by
introducing an indexed configuration scheme:

### Configuration model

Two parallel input paths resolve to a vector of `DatabaseConfig` entries:

1. **TOML array** (preferred for TOML-based deployments):

   ```toml
   [[databases]]
   url = "postgresql://user:pass@host1:5432/app1"
   name = "app1-prod"

   [[databases]]
   url = "postgresql://user:pass@host2:5432/analytics"
   pg_dump_extra_args = "--exclude-table=logs"
   ```

2. **Indexed environment variables** (preferred for containerized / CI deployments):

   ```
   DATABASE_URL_0=postgresql://user:pass@host1:5432/app1
   DATABASE_URL_1=postgresql://user:pass@host2:5432/analytics
   PG_DUMP_EXTRA_ARGS_1=--exclude-table=logs
   ```

3. **Fallback**: If no indexed entries and no TOML array are found, the existing
   single `DATABASE_URL` flow proceeds unchanged — full backward compatibility.

Each `DatabaseConfig` carries:

| Field                | Required | Default                          |
|----------------------|----------|----------------------------------|
| `url`                | Yes      | N/A                              |
| `name`               | No       | Database extracted from URL      |
| `pg_dump_extra_args` | No       | Shared `PG_DUMP_EXTRA_ARGS` value or none |

Shared settings (Telegram credentials, chunk size, proxy, AGE_RECIPIENT) remain
global — they apply uniformly across all servers.

### Execution model

- **Parallelism**: One `tokio::task::spawn` per server, each running its own
  `pg_dump → zstd → age? → ChunkWriter → Telegram upload` pipeline.
- **Chunk namespacing**: File prefix becomes `"db-{name}-{timestamp}"` to
  prevent collisions in the shared work directory.
- **Failure policy**: Non-blocking per-server failures. The orchestrator uses
  `JoinSet` to collect results; failed databases log errors but do not cancel
  remaining ones. The process exits success if at least one succeeded, error only
  if all fail.
- **Manifest output**: Per-database summary lines followed by aggregate totals:

  ```
  # crab-dump manifest
  servers: 2
  server 0: myapp-prod (bytes=123M, chunks=3, encrypted=true, sha256=abc..., duration=12.3s)
  server 1: analytics-db (bytes=98M, chunks=2, encrypted=false, sha256=def..., duration=8.1s)
  ```

### Status dashboard extension

The existing `/api/status/process` endpoint returns a global aggregated status.
A new per-database endpoint exposes individual states:

- `GET /api/status/database/{name}` → `{ state, message, timestamp }`
- Global aggregation: UP iff all UP; DEGRADED if any DEGRADED; DOWN if any DOWN.

## Consequences

### Positive

- **Speed**: Parallel dumps reduce total wall time from sequential sum to max.
- **Backward compat**: Existing single-DATABASE_URL setups keep working unmodified.
- **Flexibility**: Per-DB extra args allow fine-grained control (e.g., exclude
  noisy tables from analytics dumps).
- **Debuggability**: Chunk filenames are namespaced by database; manifests show
  per-server outcomes independently.
- **Operational simplicity**: One binary invocation replaces cron-loops of
  multiple process launches.

### Negative / Trade-offs

- **Configuration complexity**: Two config formats (TOML array vs indexed env
  vars) adds cognitive overhead; operators must pick one pattern.
- **Resource consumption**: Concurrent dumps increase disk I/O, CPU, and memory
  usage proportionally to the number of active servers.
- **Global-only encryption**: All databases share the same `AGE_RECIPIENT`.
  Operators who need per-DB encryption toggles must fork or request v2.
- **Shared chat constraint**: All uploads go to the same Telegram chat.  Future
  versions may index `TG_CHAT_ID_N` per server to decouple this.
- **Concurrency cap**: Hard limit of 10 concurrent servers (`MAX_SERVERS`) prevents
  runaway resource usage; operators with many small databases will need batching.

### Open questions

| Question                               | Tentative answer                    |
|----------------------------------------|-------------------------------------|
| Should per-DB encryption be added in v1? | No — scope creep risk; deferred to v2. |
| What default `MAX_SERVERS` is safe?     | 10 (configurable via env).           |
| Should we support per-server TG chat IDs? | Out of scope; deferred to v2.       |
| Is there a preferred concurrency library? | `tokio::task::JoinSet` — already available. |
