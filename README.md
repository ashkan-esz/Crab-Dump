# crab-dump

Stream a **compressed**, optionally **encrypted** PostgreSQL dump to **Telegram**, in chunks.

```
encrypted:  pg_dump  →  zstd  →  age (X25519)  →  ≤49 MiB parts  →  Telegram
plain:      pg_dump  →  zstd                          →  ≤49 MiB parts  →  Telegram

Single self-contained binary. No external services to run. Designed to be
scheduled from cron or a systemd timer.

## Why these choices

| Stage   | Choice         | Reason                                                            |
|---------|----------------|-------------------------------------------------------------------|
| Dump    | `pg_dump -Fc`  | Custom format → fast, supports selective `pg_restore`.            |
| Compress| `zstd`         | ~2× better ratio than gzip at higher speed; built-in checksum.    |
| Encrypt | `age` (X25519) | Optional — modern, audited, hybrid encryption to a public key. No password to leak. Set `AGE_RECIPIENT` to enable; otherwise the dump is compressed but not encrypted. |
| Upload  | `reqwest`      | Direct Bot API calls; no bot-framework overhead for a one-shot job.|

**Chunking** is used instead of a self-hosted Telegram Bot API server: the
cloud Bot API caps `sendDocument` at 50 MiB, so this tool splits the archive
into ≤49 MiB parts (`name.part0000`, `name.part0001`, …) and uploads each. The
receiving side reassembles with plain `cat` (the zero-padded names make the
shell glob `cat name.part*` order lexically).

## Setup

### 1. Build

**Option A — Cargo (local binary):**
```bash
cargo build --release
# binary: target/release/crab-dump
```

**Option B — Docker (image with `pg_dump` baked in):**
```bash
docker build -t crab-dump .
# or: docker compose build
```
The runtime image is based on `postgres:17-bookworm`, so it ships a matching
`pg_dump` — no need to install Postgres client tools on the host.

### 2. (Optional) Generate an age keypair

The backup works without encryption — only set this up if you want encrypted dumps.

You only need the `age` tooling for key generation and for restoring. The
backup itself uses the `age` Rust crate internally and needs no external CLI.

```bash
cargo install rage   # provides rage-keygen, rage
rage-keygen -o identity.txt
# → writes identity.txt (KEEP SECRET) and prints:
#   Public key: age1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Put the **public key** (`age1…`) in `AGE_RECIPIENT` on the backup host.
Keep `identity.txt` somewhere safe and offline — you'll need it to restore.

### 3. (Optional) SOCKS5 proxy

If Telegram is blocked in your country, set `SOCKS_PROXY`:

```bash
export SOCKS_PROXY=socks5h://127.0.0.1:2080
```

Use the `socks5h://` scheme (the `h` resolves DNS through the proxy) to
avoid DNS-based blocking. All Telegram API traffic goes through the proxy.

### 4. Create a Telegram bot

1. Message [@BotFather](https://t.me/BotFather) → `/newbot` → copy the token into `TG_BOT_TOKEN`.
2. Create a chat/channel, add the bot, and promote it (channels: *Post Messages*; groups: *not* a member-only restriction).
3. Get the chat id. For a channel: `-100` + channel id. Easiest way: post a message, then `curl https://api.telegram.org/bot<TOKEN>/getUpdates` and read `result[].message.chat.id`.

### 5. Configure

```bash
cp .env.example .env  # then edit
```

### 6. Run

```bash
set -a; source .env; set +a
./target/release/crab-dump            # run a backup
./target/release/crab-dump --dry-run  # validate config, no upload
./target/release/crab-dump --no-encryption  # disable encryption for this run
```

On success the binary prints a **manifest**, one line per database:

```
# crab-dump manifest
servers: 3 (2 ok, 1 failed)
server 0: app (bytes=145572521, chunks=3, encrypted=false, sha256=4d52dfe801d349c71de6d485f2af62f5d01ab6dd08fb33fc037cdea4e964117, duration=31.4s)
server 1: analytics (bytes=8815104, chunks=1, encrypted=false, sha256=9f2b1c0e5d47a8836be1f0a2c9d43e7715b6c8a09f3d2e1b4c5a6978d0e1f2a3, duration=4.2s)
FAILED [2] archive: dumping database 'archive': pg_dump exited with status 1

# restore [app]: cat db0-app-20260704-205532.part* | zstd -d | pg_restore --dbname=...
# restore [analytics]: cat db1-analytics-20260704-205532.part* | zstd -d | pg_restore --dbname=...
```

Chunk files are named `db{index}-{name}-{YYYYmmdd-HHMMSS}.partNNNN`. A database
that fails does not stop the others; it gets a `FAILED` line in the manifest
with its error, and the run exits non-zero.

Temp chunk files are deleted on success and **kept on failure** for debugging —
including partial chunks from a database that failed mid-dump. They are never
removed automatically, so sweep `WORK_DIR` after investigating a failed run.

## Configuration reference

| Variable             | Required | Default        | Notes                                              |
|----------------------|:--------:|----------------|----------------------------------------------------|
| `DATABASE_URL`       | yes\*     |                | `postgresql://user:pass@host:5432/db`              |
| `DATABASE_URL_N`     | yes\*     |                | one per database, indexed from `0` — see below     |
| `DB_NAME_N`          | no       | from URL path  | display name for database `N`; must be unique      |
| `PG_DUMP_EXTRA_ARGS_N` | no     | shared value   | per-database override of `PG_DUMP_EXTRA_ARGS`      |
| `CRAB_MAX_DATABASES` | no       | `10`           | refuses to start above this count                  |
| `TG_BOT_TOKEN`       | yes      |                | from @BotFather                                    |
| `TG_CHAT_ID`         | yes      |                | numeric id or `@channelusername`                   |
| `AGE_RECIPIENT`      | no       | *(none)*       | `age1…` X25519 public key (omit for unencrypted) |
| `--no-encryption`     | no       | off            | disable encryption for one run, even when `AGE_RECIPIENT` is set |
| `SOCKS_PROXY`        | no       | *(none)*       | SOCKS5 proxy, e.g. `socks5h://127.0.0.1:2080`    |
| `PG_DUMP_EXTRA_ARGS` | no       | *(none)*       | extra `pg_dump` args                               |
| `CHUNK_SIZE_MB`      | no       | `49`           | must be 1–49                                       |
| `WORK_DIR`           | no       | OS temp dir    | temp chunk storage                                 |
| `RUST_LOG`           | no       | `info`         | `debug` for per-chunk detail                       |

\* Either `DATABASE_URL` (single database) or `DATABASE_URL_0`, `DATABASE_URL_1`,
… (multiple). Every value in this table can also be set in `config.toml`; the
environment wins where both define the same key.

### Multiple databases

Declare databases with indexed environment variables, contiguous from `0` — a
gap stops the scan, and every index after it is ignored (you get a warning):

```bash
DATABASE_URL_0=postgresql://user:pass@host-a:5432/app
DB_NAME_0=app
DATABASE_URL_1=postgresql://user:pass@host-b:5432/analytics
DB_NAME_1=analytics
PG_DUMP_EXTRA_ARGS_1=--exclude-table=events
```

or as `[[databases]]` entries in `config.toml` (which take precedence over the
indexed variables):

```toml
[[databases]]
url  = "postgresql://user:pass@host-a:5432/app"
name = "app"

[[databases]]
url  = "postgresql://user:pass@host-b:5432/analytics"
name = "analytics"
pg_dump_extra_args = "--exclude-table=events"
```

Display names must be unique — they key both the chunk filenames and the
dashboard cards, so crab-dump refuses to start on a collision. Set `DB_NAME_N`
(or `name`) when two servers host a database of the same name.

Each database dumps and packages on its own thread; one failure does not stop
the others, but any failure makes the run exit non-zero. Uploads are
serialized process-wide because all databases share one `TG_CHAT_ID` and
Telegram rate-limits per chat.

> **Disk:** each pipeline writes its entire compressed dump to `WORK_DIR`
> before uploading its first chunk, so peak disk usage is the sum across all
> databases running concurrently — up to `CRAB_MAX_DATABASES` × the
> single-database figure. Nothing pre-checks free space; exhaustion surfaces as
> an I/O error mid-dump, after the dump time has been spent. Size `WORK_DIR`
> accordingly, or lower `CRAB_MAX_DATABASES`.

## Running in Docker

The image runs as the `postgres` user and ships `pg_dump` 17, so the only
thing you need to provide is configuration via environment variables.

```bash
# One-shot backup (without encryption):
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@dbhost:5432/mydb" \
  -e TG_BOT_TOKEN="..." \
  -e TG_CHAT_ID="..." \
  crab-dump

# One-shot backup (with encryption):
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@dbhost:5432/mydb" \
  -e TG_BOT_TOKEN="..." \
  -e TG_CHAT_ID="..." \
  -e AGE_RECIPIENT="age1..." \
  -e SOCKS_PROXY="socks5h://127.0.0.1:2080" \
  crab-dump

# Or with docker-compose (uses a .env file)
docker compose run --rm crab-dump
```

### Networking notes (important)

The container has its own network namespace:

- **Postgres / SOCKS proxy on the Docker host?** Use `host.docker.internal`
  (Docker Desktop) or the host's LAN IP — *not* `127.0.0.1`, which refers to
  the container itself.
- On Linux, add `--add-host=host.docker.internal:host-gateway` if you need
  that name, or run with `--network host`.
- **`WORK_DIR` on a mounted volume:** the container runs as UID 999
  (`postgres`). Make the mount writable, e.g. `chmod 777 ./work` or use a
  named volume.

```bash
docker run --rm \
  -v pgbackup-work:/work \
  -e WORK_DIR=/work \
  ... crab-dump
```

## Scheduling

Example systemd timer (replace paths/users as needed):

```ini
# /etc/systemd/system/crab-dump.service
[Unit]
Description=PostgreSQL → Telegram backup
Wants=network-online.target
After=network-online.target postgresql.service

[Service]
Type=oneshot
User=postgres
EnvironmentFile=/etc/crab-dump.env
ExecStart=/usr/local/bin/crab-dump

# /etc/systemd/system/crab-dump.timer
[Unit]
Description=Daily PostgreSQL backup to Telegram

[Timer]
OnCalendar=*-*-* 03:30:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable with `systemctl enable --now crab-dump.timer`.

## Restoring

On a machine with the `identity.txt` (private key) and the downloaded parts.
`BASE` is the prefix printed in the manifest's restore line
(`db{index}-{name}-{YYYYmmdd-HHMMSS}`):

```bash
BASE=db0-app-20260704-205532

# Encrypted dump:
cat "$BASE".part* \
  | rage -d -i identity.txt \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists

# Plain (unencrypted) dump:
cat "$BASE".part* \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists
```

Restore one database at a time — glob on the full `BASE`, not on `db*`, or you
will concatenate two different dumps.

For a **plain-text** dump (if you set `PG_DUMP_EXTRA_ARGS=--format=plain`),
swap `pg_restore` for `psql`:

```bash
# Encrypted plain dump:
cat "$BASE".part* | rage -d -i identity.txt | zstd -d | psql "$DATABASE_URL"

# Plain plain dump:
cat "$BASE".part* | zstd -d | psql "$DATABASE_URL"
```

> The `sha256` in the manifest covers the stream that was written to
> the parts (encrypted if AGE_RECIPIENT was set, otherwise the
> compressed stream before uploading). Verify before decrypting:
> ```bash
> cat "$BASE".part* | sha256sum
> ```

> Backups taken before the part suffix widened to four digits use two-digit
> names (`.part00`). The globs above still match them, and they still order
> correctly as long as the backup has fewer than 100 parts. A two-digit backup
> with 100 or more parts must be reassembled from the manifest's `parts:` list
> in the order given, not by glob.

## Behavior notes

- **Single pass, constant memory.** `pg_dump` stdout is streamed through zstd
  → age → the rolling chunk writer; the uncompressed dump is never written to
  disk.
- **Retries.** Upload failures are retried up to 5× with exponential backoff
  for transient errors (network, 429, 5xx). Permanent errors (e.g. 401 bad
  token, 400 bad chat) abort immediately.
- **Atomic-ish.** If the pipeline or upload fails partway, already-uploaded
  chunks remain in Telegram; the failure is surfaced via non-zero exit. Temp
  files are preserved on failure and removed on success.
- **Limits.** This targets dumps up to ~2 GiB (chunked). For larger archives,
  either raise `CHUNK_SIZE_MB` after standing up a [local Bot API server]
  (2 GiB limit) or split the archive yourself.

## Testing

```bash
cargo test                 # unit tests for chunk rolling/reassembly
```

A full end-to-end round-trip (dump → chunk → restore)
was validated against a containerized Postgres 17: 128 MiB of data → 2 chunks
→ restored row count matched exactly.

[local Bot API server]: https://github.com/tdlib/telegram-bot-api
