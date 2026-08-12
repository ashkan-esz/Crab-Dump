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
cloud Bot API caps `sendDocument` at 50 MiB, so this tool packages the archive
into ≤49 MiB uploads. A stream that fits in one chunk is uploaded as the bare
`name`; larger streams use `name.part0000`, `name.part0001`, … . The receiving
side reassembles multi-part backups with plain `cat` (the zero-padded names
make the shell glob `cat name.part*` order lexically).

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
# restore [analytics]: cat db1-analytics-20260704-205532 | zstd -d | pg_restore --dbname=...
```

Chunk files use the prefix `db{index}-{name}-{YYYYmmdd-HHMMSS}`. A one-chunk
backup uses that prefix as its bare filename; multi-part backups append
`.partNNNN`. A database that fails does not stop the others — every remaining database is still dumped
and uploaded — it gets a `FAILED` line in the manifest with its error. A
one-shot run then exits non-zero; a scheduled run (`BACKUP_INTERVAL`) logs it
and retries that database on the next cycle.

Temp chunk files are deleted as soon as Telegram accepts them, so `WORK_DIR`
holds only the chunks still waiting to upload — not the whole dump. A failed
database's leftovers are swept too, unless you set `KEEP_FAILED_DUMPS=1`, which
leaves them in `WORK_DIR` for debugging (nothing removes them later — sweep the
directory yourself).

## Configuration reference

| Variable             | Required | Default        | Notes                                              |
|----------------------|:--------:|----------------|----------------------------------------------------|
| `DATABASE_URL`       | yes\*     |                | `postgresql://user:pass@host:5432/db`              |
| `DATABASE_URL_N`     | yes\*     |                | one per database, indexed from `0` — see below     |
| `DB_NAME_N`          | no       | from URL path  | display name for database `N`; must be unique      |
| `PG_DUMP_EXTRA_ARGS_N` | no     | shared value   | per-database override of `PG_DUMP_EXTRA_ARGS`      |
| `CRAB_MAX_DATABASES` | no       | `10`           | refuses to start above this count                  |
| `MAX_PARALLEL_DATABASES` | no   | `4`            | how many databases back up at the same time (≥ 1)  |
| `TG_BOT_TOKEN`       | yes      |                | from @BotFather                                    |
| `TG_CHAT_ID`         | yes      |                | numeric id or `@channelusername`                   |
| `AGE_RECIPIENT`      | no       | *(none)*       | `age1…` X25519 public key (omit for unencrypted) |
| `--no-encryption`     | no       | off            | disable encryption for one run, even when `AGE_RECIPIENT` is set |
| `SOCKS_PROXY`        | no       | *(none)*       | SOCKS5 proxy, e.g. `socks5h://127.0.0.1:2080`    |
| `PG_DUMP_EXTRA_ARGS` | no       | *(none)*       | extra `pg_dump` args                               |
| `CHUNK_SIZE_MB`      | no       | `49`           | must be 1–49                                       |
| `WORK_DIR`           | no       | OS temp dir    | temp chunk storage                                 |
| `HISTORY_DIR`        | no       | `./history`    | monthly JSONL backup-attempt history               |
| `HISTORY_RETENTION_MONTHS` | no | `12`       | current month plus this many total monthly files   |
| `HISTORY_UPLOAD_SCHEDULE` | no | `59 23 * * *` | upload active monthly history in scheduled mode; `0`/blank disables |
| `KEEP_FAILED_DUMPS`  | no       | `0`            | keep a failed backup's chunks in `WORK_DIR` for debugging |
| `BACKUP_INTERVAL`    | no       | *(one-shot)*   | repeat instead of exiting: an interval like `6h` (min `60s`), or a crontab expression like `0 */4 * * *` |
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
the others — not even when every database fails — but in one-shot mode any
failure makes the run exit non-zero. At most
`MAX_PARALLEL_DATABASES` (default `4`) run at a time — a database waits for a
free slot, and a worker takes the next queued database as soon as it finishes,
so a slow dump never idles the others. Set it to `1` for strictly sequential
backups. Uploads are serialized process-wide because all databases share one
`TG_CHAT_ID` and Telegram rate-limits per chat.

> **Disk:** each pipeline writes its entire compressed dump to `WORK_DIR`
> before uploading its first chunk, so peak disk usage is the sum across the
> databases running concurrently — up to `MAX_PARALLEL_DATABASES` × the
> single-database figure. Chunks are deleted as they upload, so usage falls
> during the upload stage, but the peak stands. Nothing pre-checks free space;
> exhaustion surfaces as an I/O error mid-dump, after the dump time has been
> spent. Size `WORK_DIR` accordingly, or lower `MAX_PARALLEL_DATABASES`.

The dashboard shows the active limit in its info bar ("Parallel limit").

Every database attempt is also appended to `HISTORY_DIR/YYYY-MM.jsonl`,
including failures that happen before packaging completes. Records include
timestamps, byte counts, chunk count, SHA-256, encryption, duration, and
aggregate Telegram upload attempts/retries. History is best-effort: an
I/O error is logged as a warning and does not change the backup outcome.
Retention defaults to the current month plus the previous 11 months. In
containers, mount `HISTORY_DIR` as persistent storage; the example
`docker-compose.yml` provides a named volume.

The dashboard keeps the live pipeline view separate from historical data.
Expand a database row to load its retained history on demand; the expanded
view shows the newest 30 attempts and aggregate statistics across all retained
monthly files. It includes success/failure counts and rate, last run and last
successful run, average duration, dump and packaged sizes, upload retries, and
sanitized failure messages.

The read-only endpoint is:

```text
GET /api/history/{database_name}
```

It returns `{ "database", "stats", "records" }`. Missing or empty history
returns a successful response with zero-valued statistics and an empty
`records` array. The dashboard caches an expanded result until the user
presses Refresh.

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

Two options: let crab-dump schedule itself, or drive it from cron / a systemd
timer.

### Built-in scheduler (`BACKUP_INTERVAL`)

Set `BACKUP_INTERVAL` and the process stays alive, repeating forever. Nothing
external is needed, and the dashboard keeps serving between runs — its info bar
shows the schedule, whether a cycle is **Running** or **Waiting**, and a live
countdown to the next one. It takes either a plain interval or a crontab
expression:

```bash
BACKUP_INTERVAL=6h            # every 6 hours, first backup immediately
BACKUP_INTERVAL="0 */4 * * *" # 00:00, 04:00, 08:00, … first backup at the next match
```

```bash
# docker-compose.yml — long-running instead of one-shot
services:
  crab-dump:
    build: .
    env_file: .env      # with BACKUP_INTERVAL set
    restart: unless-stopped
    ports: ["8080:8080"]
```

**Interval form** — seconds, or a number with an `s`/`m`/`h`/`d` suffix, minimum
`60s`. The first backup runs at startup. The interval is measured from the
**start** of each cycle, so `6h` means six hours apart rather than six hours of
idle time.

**Crontab form** — any value containing whitespace is parsed as a 5-field
crontab line, `minute hour day-of-month month day-of-week`:

| Expression | Fires |
|------------|-------|
| `0 */4 * * *`     | every 4 hours, on the hour: 00:00, 04:00, 08:00, … |
| `30 3 * * *`      | every day at 03:30 |
| `0 2 * * sun`     | Sundays at 02:00 |
| `0 9-17/2 * * 1-5`| weekdays at 09:00, 11:00, 13:00, 15:00, 17:00 |
| `0 0 1 * *`       | the 1st of every month |

`*`, `*/n`, ranges, stepped ranges, lists, and 3-letter month/weekday names all
work; `7` is Sunday. Day-of-month and day-of-week are ORed when both are
restricted, as in vixie cron. `@daily`-style nicknames, `L`/`W`/`#`, and a
seconds field are not supported. Unlike the interval form, **nothing runs at
startup** — the first backup happens at the next matching time. Times follow the
machine's local clock, so set `TZ` if the schedule should be timezone-independent
(a DST shift can otherwise move a cycle by an hour). A bad expression, or one
that can never fire, is rejected at startup rather than silently never running.

Cycles never overlap in either form: the next firing time is computed after a
cycle finishes, so a slot missed by a long-running cycle is skipped rather than
run back-to-back — two at once would double the `pg_dump` load and the
`WORK_DIR` peak. With the interval form, an overrun logs a warning and the next
cycle starts immediately.

Failures never stop the loop: a database that fails is reported in that cycle's
manifest and retried on the next one. Unset `BACKUP_INTERVAL` (or set it to
`0`) for the one-shot behaviour below.

### Daily history upload (`HISTORY_UPLOAD_SCHEDULE`)

In scheduled mode, crab-dump independently uploads the current
`HISTORY_DIR/YYYY-MM.jsonl` file once per matching local calendar date. The
default is `59 23 * * *` (23:59 in the local container timezone). It uses the
same five-field cron syntax as `BACKUP_INTERVAL`; set it to `0` or blank to
disable. Large snapshots are sent as ordered ≤49 MiB parts, and one-shot mode
never uploads history.

### External timer (one-shot)

With `BACKUP_INTERVAL` unset, crab-dump runs one cycle and exits — non-zero if
any database failed, which is what a timer's failure handling wants.

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

# Encrypted, single-chunk dump (manifest says chunks=1):
cat "$BASE" \
  | rage -d -i identity.txt \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists

# Encrypted, multi-part dump (manifest says chunks>1):
cat "$BASE".part* \
  | rage -d -i identity.txt \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists

# Plain (unencrypted), single-chunk dump:
cat "$BASE" \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists

# Plain (unencrypted), multi-part dump:
cat "$BASE".part* \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists
```

Restore one database at a time — glob on the full `BASE`, not on `db*`, or you
will concatenate two different dumps.

For a **plain-text** dump (if you set `PG_DUMP_EXTRA_ARGS=--format=plain`),
swap `pg_restore` for `psql`:

```bash
# Encrypted plain dump, single chunk:
cat "$BASE" | rage -d -i identity.txt | zstd -d | psql "$DATABASE_URL"

# Encrypted plain dump, multiple chunks:
cat "$BASE".part* | rage -d -i identity.txt | zstd -d | psql "$DATABASE_URL"

# Plain plain dump, single chunk:
cat "$BASE" | zstd -d | psql "$DATABASE_URL"

# Plain plain dump, multiple chunks:
cat "$BASE".part* | zstd -d | psql "$DATABASE_URL"
```

> The `sha256` in the manifest covers the stream that was written to
> the parts (encrypted if AGE_RECIPIENT was set, otherwise the
> compressed stream before uploading). Verify before decrypting:
> ```bash
> Use `cat "$BASE"` for `chunks=1`, or `cat "$BASE".part*` for `chunks>1`.
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
