# pg-backup-tg

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
into ≤49 MiB parts (`name.part00`, `name.part01`, …) and uploads each. The
receiving side reassembles with plain `cat`.

## Setup

### 1. Build

**Option A — Cargo (local binary):**
```bash
cargo build --release
# binary: target/release/pg-backup-tg
```

**Option B — Docker (image with `pg_dump` baked in):**
```bash
docker build -t pg-backup-tg .
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
./target/release/pg-backup-tg            # run a backup
./target/release/pg-backup-tg --dry-run  # validate config, no upload
./target/release/pg-backup-tg --no-encryption  # disable encryption for this run
```

On success the binary prints a **manifest**:

```
# pg-backup-tg manifest
base:   pgdump-20260704-205532
chunks: 3
bytes:  145572521
encrypted: false
sha256: 4d52dfe801d349c71de6d485f2af62f5d01ab6dd08fb33fc037cdea4e964117
parts:
  pgdump-20260704-205532.part00
  pgdump-20260704-205532.part01
  pgdump-20260704-205532.part02

# restore (encrypted): cat pgdump-20260704-205532.part* | age -d | zstd -d | pg_restore --dbname=...
# restore (plain):     cat pgdump-20260704-205532.part* | zstd -d | pg_restore --dbname=...
```

Temp chunk files are deleted on success and **kept on failure** for debugging.

## Configuration reference

| Variable             | Required | Default        | Notes                                              |
|----------------------|:--------:|----------------|----------------------------------------------------|
| `DATABASE_URL`       | yes      |                | `postgresql://user:pass@host:5432/db`              |
| `TG_BOT_TOKEN`       | yes      |                | from @BotFather                                    |
| `TG_CHAT_ID`         | yes      |                | numeric id or `@channelusername`                   |
| `AGE_RECIPIENT`      | no       | *(none)*       | `age1…` X25519 public key (omit for unencrypted) |
| `--no-encryption`     | no       | off            | disable encryption for one run, even when `AGE_RECIPIENT` is set |
| `SOCKS_PROXY`        | no       | *(none)*       | SOCKS5 proxy, e.g. `socks5h://127.0.0.1:2080`    |
| `PG_DUMP_EXTRA_ARGS` | no       | *(none)*       | extra `pg_dump` args                               |
| `CHUNK_SIZE_MB`      | no       | `49`           | must be 1–49                                       |
| `WORK_DIR`           | no       | OS temp dir    | temp chunk storage                                 |
| `RUST_LOG`           | no       | `info`         | `debug` for per-chunk detail                       |

## Running in Docker

The image runs as the `postgres` user and ships `pg_dump` 17, so the only
thing you need to provide is configuration via environment variables.

```bash
# One-shot backup (without encryption):
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@dbhost:5432/mydb" \
  -e TG_BOT_TOKEN="..." \
  -e TG_CHAT_ID="..." \
  pg-backup-tg

# One-shot backup (with encryption):
docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@dbhost:5432/mydb" \
  -e TG_BOT_TOKEN="..." \
  -e TG_CHAT_ID="..." \
  -e AGE_RECIPIENT="age1..." \
  -e SOCKS_PROXY="socks5h://127.0.0.1:2080" \
  pg-backup-tg

# Or with docker-compose (uses a .env file)
docker compose run --rm pg-backup-tg
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
  ... pg-backup-tg
```

## Scheduling

Example systemd timer (replace paths/users as needed):

```ini
# /etc/systemd/system/pg-backup-tg.service
[Unit]
Description=PostgreSQL → Telegram backup
Wants=network-online.target
After=network-online.target postgresql.service

[Service]
Type=oneshot
User=postgres
EnvironmentFile=/etc/pg-backup-tg.env
ExecStart=/usr/local/bin/pg-backup-tg

# /etc/systemd/system/pg-backup-tg.timer
[Unit]
Description=Daily PostgreSQL backup to Telegram

[Timer]
OnCalendar=*-*-* 03:30:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable with `systemctl enable --now pg-backup-tg.timer`.

## Restoring

On a machine with the `identity.txt` (private key) and the downloaded parts:

```bash
# Encrypted dump:
cat pgdump-YYYYmmdd-HHMMSS.part* \
  | rage -d -i identity.txt \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists

# Plain (unencrypted) dump:
cat pgdump-YYYYmmdd-HHMMSS.part* \
  | zstd -d \
  | pg_restore --dbname=postgresql://user:pass@host:5432/db --no-owner --clean --if-exists
```

For a **plain-text** dump (if you set `PG_DUMP_EXTRA_ARGS=--format=plain`),
swap `pg_restore` for `psql`:

```bash
# Encrypted plain dump:
cat pgdump-*.part* | rage -d -i identity.txt | zstd -d | psql "$DATABASE_URL"

# Plain plain dump:
cat pgdump-*.part* | zstd -d | psql "$DATABASE_URL"
```

> The `sha256` in the manifest covers the stream that was written to
> the parts (encrypted if AGE_RECIPIENT was set, otherwise the
> compressed stream before uploading). Verify before decrypting:
> ```bash
> cat pgdump-*.part* | sha256sum
> ```

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
