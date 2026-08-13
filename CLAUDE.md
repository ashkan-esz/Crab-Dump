# Repository Guidelines

## Structure
Single-binary Rust project (`crab-dump`), flat `src/`:
- `main.rs` — CLI entry, pipeline orchestration
- `config.rs` — env-var load/validate (`clap`, `anyhow`)
- `dump.rs` — spawns `pg_dump`, reads stdout
- `compress.rs` — wraps writer in `zstd` encoder
- `encrypt.rs` — wraps writer in `age` (X25519) encryptor
- `chunk.rs` — rolling `ChunkWriter`, splits into ≤49 MiB `.partNNNN`
- `telegram.rs` — Bot API document upload with retries

Roots: `Cargo.toml`, `Dockerfile` (multi-stage), `docker-compose.yml`, `.env.example`.

Pipeline: `pg_dump` → zstd → age (optional) → chunk → Telegram.

## Commands
- `cargo build --release` → `target/release/crab-dump`
- `cargo test` — unit tests (chunk roll/reassembly)
- `cargo run --release -- --dry-run` — validate config + `pg_dump` availability
- Pre-submit: `cargo fmt` && `cargo clippy -- -D warnings`

## Style
Rust 2021. `snake_case` modules/fns/vars, `PascalCase` types; keep items module-scoped by default.
Errors: `anyhow::Result` at API boundaries, `thiserror` for typed errors; `.context("…")` on every fallible call.
Logging: `tracing` + `EnvFilter` (`RUST_LOG`).

## Tests
Inline `#[cfg(test)]` modules at file bottom. Streaming + Telegram paths are integration-grade, verified manually.

## Commits & PRs
Conventional Commits (`feat:`, `fix:`, `chore:`, `refactor:`, `docs:`, `test:`).
PRs: brief description, linked issues, note config/env changes.

## Config
Env-only (`set -a; source .env`). See `src/config.rs` for defaults.
Required: indexed `DATABASE_URL_0` and `TG_CHAT_ID_0`, plus `TG_BOT_TOKEN`.
Optional: `AGE_RECIPIENT`, `SOCKS_PROXY`, `CHUNK_SIZE_MB`, `MAX_PARALLEL_DATABASES` (4), `WORK_DIR`, `KEEP_FAILED_DUMPS` (off), `BACKUP_INTERVAL` (e.g. `6h`; default one-shot), `RUST_LOG`.

## Invariants

Violating these is a bug, not a style preference. Do not "simplify" past them.
If a task appears to require breaking one, stop and say so instead.

1. **Constant memory.** The dump never lands fully in RAM. No `read_to_end`,
   no `Vec<u8>` accumulation, no `collect()` on the dump or chunk data path —
   fixed-size buffer copies only. Memory use must not scale with dump size.
2. **Secrets never surface.** `TG_BOT_TOKEN`, `DATABASE_URL` passwords, and
   chat IDs are redacted in logs, `Debug`/`Display` impls, `anyhow` context
   strings, and `--dry-run` output.
3. **Chunk naming is a wire contract.** `{db}_{utc_ts}.sql.zst[.age].part0001`
   — 4-digit zero-padded. Lexicographic order == concatenation order; parts
   must reassemble with `cat parts*`. Changing this is a breaking change.
4. **Pipeline order is fixed.** dump → compress → encrypt → chunk → upload.
   Encryption is always after compression, never before.
5. **Fail-soft across databases.** One database's failure must not abort its
   siblings. The process exits non-zero if any database failed, after all
   others finish.
6. **WORK_DIR is left clean.** Intermediate files are `tempfile`-managed and
   removed on every exit path, including error and signal, unless
   `KEEP_FAILED_DUMPS=1`.
7. **One runtime, one HTTP client.** `tokio` + `reqwest` (SOCKS feature).
   Do not introduce a second async runtime or HTTP client.
   `
8. **Chunk size stays under the Bot API upload cap.** `CHUNK_SIZE` is a single
   named constant in bytes. No code path emits a part larger than it, including
   the final short part's predecessors. Changing it is a wire-contract change
   (see Invariant 3).
9. **All egress honors `SOCKS_PROXY`.** Every outbound connection routes through
   the proxy when set — Telegram API, retries, and any preflight or health
   check. A direct-connect fallback on proxy failure is a bug, not a
   resilience feature: fail the upload instead.
10. **Rate limits are obeyed, not raced.** On HTTP 429, sleep for the
    server-provided `retry_after` before the next attempt. Never retry 429
    faster than the server asked, and never treat it as a generic 5xx.
    `

## Style

Defaults, not laws — deviate when the situation warrants it.

- Small focused functions; module boundaries match the pipeline stages.
- `anyhow` at boundaries, `thiserror` for typed module errors.
- `tracing` for structured logs; no `println!` outside `--dry-run` output.
- No new dependencies without justification in the PR description.
- No `unwrap`/`expect`/`panic!` outside tests and startup in `main`.
  `
- Retry policy on 5xx: exponential backoff with jitter, bounded attempts.
  Curve, base delay, and cap are tunable; the *existence* of bounded retry
  is not (see Invariants 9, 10).
  `

## Restore Contract

A backup is valid only if it restores with shell primitives and no custom
tooling. This is the observable consequence of Invariants 3 and 4, and should
be covered by an integration test rather than trusted by inspection.

Encrypted:
cat mydb_20260812T031500Z.sql.zst.age.part* | age -d -i key.txt | zstd -d | psql
Unencrypted:
cat mydb_20260812T031500Z.sql.zst.part* | zstd -d | psql

## Done
`cargo fmt --check && cargo clippy -- -D warnings && cargo test` clean; no new deps without justification.

## Bash Tooling
Prefer these; fall back silently if missing.
- Content search: `rg` (not `grep`); files: `fd` (not `find`)
- Avoid `find -exec` / `xargs` chains — prefer `fd -x` or `rg -l | xargs`
- Structural search/refactor: `ast-grep` (`sg`)
- JSON: `jq` · YAML/TOML: `yq`
- GitHub (PRs, issues, reviews, CI, releases): `gh` — never scrape github.com or call REST directly
- Benchmarking: `hyperfine`
- Unused deps: `cargo machete` · outdated: `cargo outdated`
- Audit: `cargo audit` (age/zstd/reqwest are the CVE-prone surface)

@RTK.md
