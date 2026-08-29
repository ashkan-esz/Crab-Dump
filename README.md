# crab-dump

Self-hosted PostgreSQL backups delivered to Telegram with streaming compression,
optional age encryption, shell-reassemblable chunks, and an operator dashboard.

[![Rust](https://img.shields.io/badge/rust-2021-orange?logo=rust)](https://www.rust-lang.org/)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

`crab-dump` is a single Rust binary that turns `pg_dump` output into encrypted
or unencrypted backup archives and sends them to one or more Telegram
destinations. It is designed for small self-hosted deployments that want
simple storage, predictable recovery commands, and no hosted backup service.

> Telegram's cloud Bot API has a 50 MiB `sendDocument` limit. `crab-dump`
> defaults to 49 MiB parts so every upload remains below that limit.

## Contents

- [Features](#features)
- [How the backup pipeline works](#how-the-backup-pipeline-works)
- [Quick start with Docker Compose](#quick-start-with-docker-compose)
- [Local installation](#local-installation)
- [Releases and versioning](#releases-and-versioning)
- [Telegram setup](#telegram-setup)
- [Configuration](#configuration)
- [Encryption](#encryption)
- [Scheduling](#scheduling)
- [Dashboard](#dashboard)
- [Restore backups](#restore-backups)
- [File naming and recovery contract](#file-naming-and-recovery-contract)
- [Routing and network access](#routing-and-network-access)
- [Service monitoring](#service-monitoring)
- [Reliability and security](#reliability-and-security)
- [Operational boundaries](#operational-boundaries)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [Project layout](#project-layout)
- [Development and verification](#development-and-verification)

## Features

- Constant-memory streaming: dump data is copied through fixed-size buffers and
  is never collected into a single in-memory archive.
- PostgreSQL custom-format dumps suitable for selective `pg_restore`.
- Optional streaming compression with `zstd`, `gzip`, or `brotli`.
- Optional streaming encryption with age X25519 recipients or passphrases.
- Multiple PostgreSQL databases and Telegram destinations.
- Bounded parallel backups with fail-soft execution: one failed database does
  not stop its siblings.
- Deterministic chunk names with zero-padded part numbers, allowing recovery
  with ordinary shell tools.
- One-shot mode for cron and systemd timers.
- Long-running interval or crontab scheduling for Docker Compose deployments.
- Authenticated dashboard for status, database controls, history, restores,
  routing, and service monitoring.
- SOCKS5 proxy support and dashboard-managed VMess, VLESS, Shadowsocks, and
  Trojan routing profiles.
- SHA-256 manifests for uploaded backup streams.
- Automatic cleanup of temporary files and stale restore workspaces.
- No direct-connect fallback when routed Telegram traffic fails.

## How the backup pipeline works

```text
PostgreSQL
    │
    ▼
pg_dump -Fc
    │
    ▼
compression (optional)
    │
    ▼
age encryption (optional)
    │
    ▼
bounded chunk writer
    │
    ▼
Telegram Bot API
```

Compression always happens before encryption. The resulting stream is either
uploaded as one file or split into ordered `.partNNNN` files. A part is deleted
from `WORK_DIR` after Telegram accepts it, so disk usage is bounded by the
number of active database pipelines and their configured chunk size.

## Quick start with Docker Compose

Docker is the easiest deployment path because the image includes a matching
PostgreSQL client (`pg_dump`) and exposes the embedded dashboard.

```bash
cp .env.example .env
${EDITOR:-vi} .env

docker compose build
docker compose up -d
docker compose logs -f crab-dump
```

The dashboard is available at `http://127.0.0.1:1111` with the credentials
configured in `.env`. Compose persists application data and backup history in
named volumes. Temporary backup and restore workspaces are cleaned according
to `WORK_DIR` and `KEEP_FAILED_DUMPS`.

For a one-shot backup instead of a long-running service:

```bash
docker compose run --rm crab-dump
```

The default Compose service uses the `runtime-all` image, which contains both
supported routing cores. Select a smaller image when dashboard-managed routing
is not needed:

```bash
ROUTING_TARGET=runtime-none docker compose build
ROUTING_TARGET=runtime-none docker compose up -d
```

Available Docker targets are `runtime-none`, `runtime-sing-box`,
`runtime-shoes`, and `runtime-all`.

## Local installation

### Requirements

- Rust toolchain compatible with the 2021 edition.
- PostgreSQL client tools, including `pg_dump` and `pg_restore`.
- A Telegram bot and a destination chat, group, or channel.
- Docker is optional.
- `rage` is optional and is only needed for convenient age key generation or
  command-line restoration.

Build and validate the binary:

```bash
cargo build --release
cargo run --release -- --dry-run
```

`--dry-run` loads and validates configuration and checks `pg_dump` availability.
It does not dump a database or upload anything.

Check the installed version without loading configuration:

```bash
./target/release/crab-dump --version
# crab-dump 1.0.0
```

Run a backup:

```bash
set -a
source .env
set +a

./target/release/crab-dump
```

Each cycle prints a consolidated manifest to stdout. Successful databases and
failed databases are both included, making partial completion visible to cron,
systemd, or container logs:

```text
# crab-dump manifest
servers: 2 (1 ok, 1 failed)
server 0: production (bytes=145572521, chunks=3, compression=zstd, encryption_type=none, encrypted=false, sha256=..., duration=31.4s)
FAILED [1] analytics: pg_dump — pg_dump exited with status 1. Action: Check DATABASE_URL, PostgreSQL access, and pg_dump availability.
```

One-shot mode exits non-zero when any database fails, after all databases have
finished. Scheduled mode keeps running and retries failed databases during the
next cycle.

Useful Make targets:

```bash
make release       # optimized binary
make dry-run       # configuration and pg_dump validation
make verify        # format check, Clippy, and tests
make compose-up    # build and start Docker Compose
make compose-logs  # follow service logs
```

## Releases and versioning

The version in `Cargo.toml` is the single source of truth and follows
Semantic Versioning. The CLI embeds this value, and Docker images expose it as
the `org.opencontainers.image.version` label.

To publish a release:

1. Update the version in `Cargo.toml` and add the matching entry to
   `CHANGELOG.md`.
2. Commit the changes and create a matching tag, for example:

   ```bash
   git tag v1.0.0
   git push origin master v1.0.0
   ```

3. GitHub Actions validates the tag, runs the quality gates, builds Linux
   `amd64` and `arm64` archives with checksums, creates the GitHub Release, and
   publishes the multi-architecture image to GHCR.

Release images are tagged with the full version and `major.minor` (for example
`1.0.0` and `1.0`). Stable releases also update `latest`; prereleases such as
`v1.0.0-rc.1` do not.

## Telegram setup

1. Open [@BotFather](https://t.me/BotFather), run `/newbot`, and copy the bot
   token into `TG_BOT_TOKEN`.
2. Create or choose a destination chat, group, or channel.
3. Add the bot to that destination. In a channel, grant **Post Messages**.
4. Configure one or more contiguous `TG_CHAT_ID_N` variables.

For a channel, the numeric chat ID generally starts with `-100`. A public
channel username such as `@backup-channel` can also be used. Never commit bot
tokens or database URLs containing passwords.

## Configuration

`crab-dump` supports environment variables and an optional `config.toml`.
Environment variables override values loaded from `config.toml`. Telegram chat
IDs and encryption settings remain environment-only; this keeps chat
destinations and decryption-related secrets out of the TOML file.

Start from the provided example:

```bash
cp .env.example .env
```

### Minimal `.env`

```dotenv
DATABASE_URL_0=postgresql://user:password@postgres.example.com:5432/app
TG_BOT_TOKEN=replace-with-your-bot-token
TG_CHAT_ID_0=-1001234567890

DASHBOARD_HOST=127.0.0.1
DASHBOARD_USERNAME=admin
DASHBOARD_PASSWORD=replace-with-a-long-secret
```

At least one database, one Telegram destination, and the dashboard
administrator credentials are required.

### Configuration reference

| Variable | Required | Default | Description |
|---|:---:|---|---|
| `DATABASE_URL_N` | Yes* | — | PostgreSQL connection URL; indices must be contiguous from `0`. |
| `DB_NAME_N` | No | URL database name | Dashboard and manifest display name. |
| `PG_DUMP_EXTRA_ARGS` | No | — | Shared read-oriented `pg_dump` filters. |
| `PG_DUMP_EXTRA_ARGS_N` | No | Shared value | Per-database override. |
| `TG_BOT_TOKEN` | Yes | — | Telegram bot token. |
| `TG_CHAT_ID_N` | Yes* | — | Contiguous Telegram destination IDs or usernames. |
| `DASHBOARD_HOST` | No | `127.0.0.1` | Dashboard bind address. |
| `API_PORT` | No | `8080` | Dashboard HTTP port; Compose sets `1111`. |
| `DASHBOARD_USERNAME` | Yes | — | Administrator username. |
| `DASHBOARD_PASSWORD` | Yes | — | Administrator password; minimum 12 characters. |
| `DASHBOARD_OPERATOR_USERNAME` | No | — | Optional backup/database-control account. |
| `DASHBOARD_OPERATOR_PASSWORD` | No | — | Operator password. |
| `DASHBOARD_VIEWER_USERNAME` | No | — | Optional read-only account. |
| `DASHBOARD_VIEWER_PASSWORD` | No | — | Viewer password. |
| `COMPRESSION_CODEC` | No | None | `zstd`, `gzip`, or `brotli`; omit for raw `.dump`. |
| `COMPRESSION_LEVEL` | No | Codec-native | zstd `1..22`, gzip `0..9`, brotli `0..11`. |
| `COMPRESSION_CHECKSUM` | No | zstd enabled | zstd checksum setting; rejected for other codecs. |
| `ENCRYPTION_TYPE` | No | `none` | `none`, `age-recipient`, or `age-passphrase`. |
| `AGE_RECIPIENT` | Conditional | — | Age public recipient beginning with `age1`. |
| `AGE_PASSPHRASE` | Conditional | — | Backup or restore passphrase, depending on mode. |
| `AGE_IDENTITY_FILE` | Restore only | — | Identity file for decrypting recipient-encrypted backups. |
| `SOCKS_PROXY` | No | — | SOCKS5 proxy, preferably `socks5h://...`. |
| `SING_BOX_PATH` | No | `/usr/local/bin/sing-box` | Local sing-box executable path. |
| `SHOES_PATH` | No | `/usr/local/bin/shoes` | Local shoes executable path. |
| `CHUNK_SIZE_MB` | No | `49` | Upload part size; must be between 1 and 49 MiB. |
| `WORK_DIR` | No | OS temporary directory | Temporary chunk and restore workspace. |
| `MAX_PARALLEL_DATABASES` | No | `4` | Maximum concurrent database pipelines. |
| `CRAB_MAX_DATABASES` | No | `10` | Maximum configured databases accepted at startup. |
| `PG_DUMP_TIMEOUT_SECS` | No | `3600` | Per-database dump timeout. |
| `MAX_DUMP_SIZE_MB` | No | `10240` | Maximum packaged output per database. |
| `KEEP_FAILED_DUMPS` | No | Off | Preserve failed backup files for debugging. |
| `HISTORY_DIR` | No | `./history` | Monthly JSONL backup history directory. |
| `HISTORY_RETENTION_MONTHS` | No | `12` | Number of retained monthly history files. |
| `HISTORY_UPLOAD_SCHEDULE` | No | `59 23 * * sun` | Weekly history upload time in scheduled mode; `0` disables it. |
| `BACKUP_INTERVAL` | No | `12h` | Interval such as `12h`, or a five-field crontab expression; `0` disables the built-in scheduler. |
| `RUST_LOG` | No | `info` | Logging filter; use `debug` for detailed chunk logs. |

\* Database and Telegram destination indices must be contiguous from zero.

Only read-oriented `pg_dump` arguments are accepted. Connection, output-file,
role, restore-behavior, and other unsafe arguments are rejected.
`ENCRYPTION_TYPE`, `AGE_RECIPIENT`, and backup `AGE_PASSPHRASE` must be set
through the environment. Restore credentials are also supplied through the
environment when a dashboard restore is approved.

### TOML configuration

Copy the provided example when you prefer configuration files:

```bash
cp config.toml.example config.toml
```

Example:

```toml
[[databases]]
url = "postgresql://user:password@db.internal:5432/production"
name = "production"

[[databases]]
url = "postgresql://user:password@analytics.internal:5432/analytics"
name = "analytics"
pg_dump_extra_args = "--exclude-table=sessions"

tg_bot_token = "replace-with-your-bot-token"
compression_codec = "zstd"
compression_level = 3
chunk_size_mb = 49
max_parallel_databases = 2
```

Set `TG_CHAT_ID_0`, `TG_CHAT_ID_1`, and so on in the environment. Sensitive
values should generally remain in environment variables or container secrets.

## Encryption

Encryption is optional and configured globally for all databases in a run.
The backup process uses the age Rust library; the `age` command-line tool is
not required to create backups.

Generate a recipient keypair with `rage`:

```bash
cargo install rage
rage-keygen -o identity.txt
```

Configure recipient encryption with the public key:

```dotenv
ENCRYPTION_TYPE=age-recipient
AGE_RECIPIENT=age1...
```

Or use a passphrase:

```dotenv
ENCRYPTION_TYPE=age-passphrase
AGE_PASSPHRASE=replace-with-a-long-unique-passphrase
```

Keep `identity.txt` or the passphrase outside the backup host. Recipient
encryption stores only the public recipient in the backup configuration.

## Scheduling

Without `BACKUP_INTERVAL`, the process backs up every 12 hours. To run one
cycle and exit for cron or a systemd timer, set `BACKUP_INTERVAL=0`:

```cron
0 3 * * * cd /opt/crab-dump && ./target/release/crab-dump >> /var/log/crab-dump.log 2>&1
```

An interval keeps the process alive, performs a backup immediately, and repeats
from the start of each cycle:

```dotenv
BACKUP_INTERVAL=12h
```

Intervals may use seconds or `s`, `m`, `h`, and `d` suffixes, with a minimum of
60 seconds. A five-field crontab expression aligns runs to local wall-clock
time and does not run immediately at startup:

```dotenv
BACKUP_INTERVAL=0 */4 * * *
```

The built-in scheduler never runs overlapping backup cycles. If one cycle
overruns its interval, the next cycle starts as soon as the current one
finishes.

## Dashboard

The embedded dashboard is served by the same binary. It requires a login
session; mutating requests also require a same-site CSRF token. Sessions expire
after eight hours.

| Role | Capabilities |
|---|---|
| Administrator | Full dashboard access, routing profiles, Telegram users, services, database management, restore approval, and settings. |
| Operator | Trigger backups, enable or disable configured databases, inspect status/history, create safe restores, and manage permitted operational actions. |
| Viewer | Read-only status, history, service, database, and routing information. |

Dashboard pages include:

- Overview and process status.
- Per-database status and enable/disable controls.
- Backup history and manifests.
- Restore requests and approval workflow.
- Telegram user directory.
- Service health checks and incidents.
- Routing profile management.
- Compression settings and runtime information.

Place the dashboard behind HTTPS when exposing it outside localhost. The
Compose deployment binds the dashboard to `0.0.0.0` inside the container and
publishes it on host port `1111` by default.

## Restore backups

Backups use PostgreSQL custom format (`pg_dump -Fc`). Restore options depend on
whether the stream is encrypted and whether you are restoring through the
dashboard or shell.

### Shell restore

For a zstd-compressed single-part backup:

```bash
cat production_*.dump.zst | zstd -d | pg_restore --dbname="$TARGET_DATABASE_URL"
```

For a zstd-compressed multi-part backup:

```bash
cat production_*.dump.zst.part* | zstd -d | pg_restore --dbname="$TARGET_DATABASE_URL"
```

For gzip or Brotli backups, replace `zstd -d` with the matching decompressor
(`gzip -d` or `brotli -d`). The filename extension identifies the configured
codec.

For an encrypted backup using an age identity:

```bash
cat production_*.dump.zst.age.part* \
  | age -d -i identity.txt \
  | zstd -d \
  | pg_restore --dbname="$TARGET_DATABASE_URL"
```

For passphrase-encrypted backups, replace the `age -d -i identity.txt`
operation with `age -d` and provide the passphrase interactively.

Raw uncompressed backups omit the `zstd -d` stage:

```bash
cat production_*.dump | pg_restore --dbname="$TARGET_DATABASE_URL"
```

Use `pg_restore --list` to inspect archive contents before restoring. Use
`pg_restore --clean --if-exists` only when replacing objects in an existing
database is intentional.

### Dashboard restore

Dashboard restores are limited to configured database targets; users cannot
submit arbitrary database URLs.

- `safe` restores are available to operators.
- `clean` restores use `pg_restore --clean --if-exists` and require administrator
  approval.
- Encrypted restores require exactly one of `AGE_IDENTITY_FILE` or
  `AGE_PASSPHRASE`.
- Parts are downloaded through the active routed client, reassembled, verified
  with SHA-256, decrypted, decompressed, and passed to `pg_restore`.
- Temporary restore files are removed after success or failure unless
  `KEEP_FAILED_DUMPS=1` is enabled.

Telegram does not expose a restore command or create restore requests.

## File naming and recovery contract

A backup that fits within one chunk is uploaded as a bare file:

```text
{database}_{utc_timestamp}.dump
{database}_{utc_timestamp}.dump.zst
{database}_{utc_timestamp}.dump.zst.age
```

Larger streams use the same prefix followed by zero-padded parts:

```text
{database}_{utc_timestamp}.dump.zst.age.part0000
{database}_{utc_timestamp}.dump.zst.age.part0001
```

Lexicographic order is concatenation order. Multi-part backups can therefore
be reassembled with `cat ...part*` without custom tooling. Each manifest
records the database, byte count, chunk count, encryption state, SHA-256, and
duration.

## Routing and network access

### SOCKS5 proxy

Set `SOCKS_PROXY` when Telegram access requires a proxy:

```dotenv
SOCKS_PROXY=socks5h://127.0.0.1:2080
```

The `socks5h` scheme resolves DNS through the proxy. Telegram uploads,
retries, preflight checks, and dashboard restore downloads all use the active
routed client. A failed routed request does not fall back to a direct
connection.

In Docker, `127.0.0.1` refers to the container. Use
`host.docker.internal` or the host's LAN address when the proxy runs on the
Docker host.

### Dashboard-managed routing profiles

The dashboard can manage VMess, VLESS, Shadowsocks SIP002, and Trojan share
URLs through the bundled `sing-box` and `shoes` cores. Profile URLs and
credentials are never exposed in API responses or to operators/viewers.

The administrator can create, test, select, and apply profiles. Applying a
profile starts and verifies its local SOCKS5 listener before replacing the
previous route; a failed replacement retains the previous working route.

Do not configure `SOCKS_PROXY` while a dashboard routing profile is active.
Startup rejects that combination. Local builds need the routing executable
installed separately; Docker images bundle the executable selected by the
runtime target.

## Service monitoring

The `/services` dashboard page supports authenticated HTTP health checks with:

- Service name and URL.
- Expected status code.
- Poll interval.
- Additional retries.
- Consecutive-failure threshold.
- Optional version header.
- Selected Telegram recipients.
- Optional use of the active routing profile.

Health alerts are transition-only: one notification is sent when a service
enters outage, and one when it recovers. Definitions and bounded incident
history are stored in the persistent `data` directory.

By default, health checks use a direct client. An administrator can enable the
active routing profile for an individual service when that service must be
checked through the same route as application traffic.

### Container and process health

The Docker image exposes a lightweight public health endpoint:

```bash
curl http://127.0.0.1:1111/healthz
```

Docker uses this endpoint for its health check. It is intentionally separate
from authenticated dashboard APIs and does not expose backup metadata or
secrets.

## Reliability and security

- Database failures are isolated; sibling databases continue to run.
- A one-shot process exits non-zero if any database failed, after all databases
  have completed.
- Scheduled mode logs failures and retries them in the next cycle.
- Temporary files are managed and cleaned on success and failure.
- `KEEP_FAILED_DUMPS=1` is an explicit debugging override; retained files must
  be removed manually.
- Bot tokens, database passwords, chat IDs, and routing credentials are
  redacted from logs, diagnostics, and dashboard responses.
- Keep `.env`, `config.toml`, age identities, and persistent `data` volumes
  owner-readable only.
- Protect the dashboard with HTTPS and a network access policy when it is
  reachable beyond localhost.
- Store age identity files and passphrases separately from the backup host.
- Verify Telegram destination permissions before relying on scheduled backups.

### Upload retries and rate limits

Telegram uploads are serialized within the process so concurrent database
pipelines do not race the same Bot API rate limits. Each chunk allows up to
five total attempts. Transient transport errors and HTTP 5xx responses use
bounded exponential backoff. HTTP 429 responses honor Telegram's
`retry_after` value, capped at five minutes.

If all attempts for a destination fail, that destination is skipped while
other configured destinations continue. If no destination receives the
complete stream, the database backup is reported as failed.

## Operational boundaries

`crab-dump` is a backup packaging and delivery tool, not a hosted backup
platform. Telegram is the configured backup destination; the application does
not provide object storage, cross-region replication, or a separate catalog
service.

- Backups are full `pg_dump` archives. Incremental and point-in-time recovery
  are outside the current design.
- PostgreSQL connectivity is supplied by your database network and credentials.
- Telegram retention, channel permissions, and available storage are managed
  by Telegram and the operator.
- Restores target databases already configured in `crab-dump`; arbitrary
  destination URLs are intentionally rejected.
- The dashboard is an operational interface, not a replacement for database
  administration, disaster-recovery testing, or an off-site key-management
  process.

### Persistent application state

In Docker Compose, keep the following state protected and persistent:

| Location | Contents |
|---|---|
| `/app/data` | Database enablement, manifests, restore requests, Telegram users, routing profiles, and health-monitor state. |
| `/app/history` | Monthly JSONL backup-attempt history. |
| `WORK_DIR` | In-progress backup and restore files; normally temporary and cleaned automatically. |

The `data` and `history` volumes are part of the application’s operational
state. Include them in your host backup strategy, restrict their permissions,
and do not expose routing profile files or restore credentials.

## Troubleshooting

### `pg_dump` is missing

Use a Docker image, or install the PostgreSQL client package for your
distribution. Confirm availability with:

```bash
./target/release/crab-dump --dry-run
```

### Telegram returns permission or chat errors

Confirm that the bot is a member of the destination and has permission to send
documents. Confirm that every `TG_CHAT_ID_N` index is contiguous from zero.

### Docker cannot reach PostgreSQL or the proxy

The container has its own network namespace. Do not use `127.0.0.1` for a
service running on the host. Use `host.docker.internal` (provided by the
Compose file) or a reachable LAN/Docker service name.

### The dashboard is unreachable

Check `DASHBOARD_HOST`, `API_PORT`, container port publishing, and logs:

```bash
docker compose ps
docker compose logs --tail=100 crab-dump
```

### A backup runs out of disk space

Lower `MAX_PARALLEL_DATABASES`, ensure `WORK_DIR` has enough free space, and
review whether `KEEP_FAILED_DUMPS=1` has left old files behind.

### An encrypted restore fails

Use the identity or passphrase that matches the encryption mode. For recipient
encryption, the restore identity must contain the private key corresponding to
the configured `AGE_RECIPIENT`.

## FAQ

### Does `crab-dump` store the complete dump in memory?

No. The dump, compression/encryption stages, and chunk writer use streaming
fixed-size copies. Memory usage does not grow with database size.

### What happens when one database fails?

Other databases continue running. The manifest records the failure, and a
one-shot invocation exits non-zero after the remaining databases finish.

### Can I change compression from the dashboard?

Yes. The dashboard can persist compression settings. A complete backup cycle
uses one compression snapshot, so all databases in that cycle are packaged
consistently.

### Can I restore to an arbitrary PostgreSQL URL?

No. Dashboard restores are restricted to configured databases. This prevents a
restore request from becoming an arbitrary outbound database connection.

### Is Telegram used for restore commands?

No. Telegram is the backup transport and notification destination. Restore
requests are created and approved through the authenticated dashboard.

### Do I need both routing cores?

No. Use `runtime-none` without dashboard-managed routing, or select
`runtime-sing-box` / `runtime-shoes` when only one compatible core is required.
Use `runtime-all` when profiles may need either core.

## Project layout

```text
src/
├── main.rs                 runtime orchestration
├── config.rs               environment and TOML configuration
├── dump.rs                 pg_dump process and streaming output
├── compress.rs             compression stages
├── encrypt.rs              age encryption stage
├── chunk.rs                bounded chunk writer
├── telegram.rs             Bot API uploads and retries
├── restore.rs              restore downloads and verification
├── routing.rs              managed routing profiles
├── web.rs                  embedded dashboard and API
├── health_monitor.rs       service checks and alerts
├── history.rs              backup-attempt history
└── database_registry.rs    dashboard-managed database registry
```

Additional architecture notes are available in:

- [`docs/ADR-0001-multi-database-support.md`](docs/ADR-0001-multi-database-support.md)
- [`docs/ADR-0002-dashboard-database-enable-disable.md`](docs/ADR-0002-dashboard-database-enable-disable.md)
- [`docs/ADR-0003-dashboard-triggered-database-backups.md`](docs/ADR-0003-dashboard-triggered-database-backups.md)

## Development and verification

```bash
cargo check
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

Docker checks:

```bash
make docker-build
make docker-smoke
```

`cargo test` covers core streaming and chunk behavior. Telegram, Docker, and
full restore paths require integration-style verification against the relevant
services and credentials.

## License

No license file is currently included in this repository. Add a license before
redistributing the project or publishing license-specific usage claims.
