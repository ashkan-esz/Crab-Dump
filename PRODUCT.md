# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Existing Rust 2021 single-binary application with an Actix Web dashboard, Tokio runtime, PostgreSQL client tooling, Telegram Bot API integration, and Docker deployment support.

## Users

Self-hosting operators who need to schedule, monitor, deliver, and restore backups for one or more PostgreSQL databases. Operators use the dashboard to manage databases, routing, service monitoring, backup history, and restore approvals.

## Product Purpose

Crab-dump creates PostgreSQL custom-format backups, optionally compresses and encrypts them, splits them into Telegram-safe chunks, and uploads them through the configured network route. It also provides a web dashboard and Telegram workflows for operational control, monitoring, backup history, and approved restores.

Success means operators can produce dependable backups and restore them with standard shell tools, while keeping the service self-hosted, secure, and resilient when individual databases or network attempts fail.

## Positioning

The product combines constant-memory PostgreSQL dump streaming with optional compression and encryption, Telegram delivery, lexically ordered shell-reassemblable chunks, and an operator dashboard for controlled restores and network routing. Its mechanism is a single self-hosted binary rather than a separate backup platform or hosted storage service.

## Operating Context

The service runs locally or in Docker and is typically invoked once or on a cron/systemd-style schedule. PostgreSQL is the source system; Telegram is the backup destination and user notification channel. Operators authenticate to the dashboard with role-based permissions. Restore operations are approved and started from the dashboard.

## Capabilities and Constraints

- Supports multiple indexed PostgreSQL databases with bounded parallelism and fail-soft execution.
- Pipeline order is fixed: `pg_dump` → compression → optional age encryption → chunking → Telegram upload.
- Supports optional zstd, gzip, or brotli compression and age recipient or passphrase encryption.
- Keeps dump and chunk processing constant-memory; dump data must never be accumulated in RAM.
- Produces one bare backup file or zero-padded `.partNNNN` chunks no larger than the Telegram upload safety limit; parts reassemble with `cat`.
- Supports dashboard-managed VMess, VLESS, Shadowsocks, and Trojan routing profiles, plus legacy `SOCKS_PROXY` when no active dashboard profile exists.
- All application egress honors the active routing profile or configured proxy; direct fallback is not allowed for routed application traffic.
- Supports authenticated service health checks with transition-only outage and recovery alerts.
- Restore requests are limited to configured databases, require dashboard approval, verify downloaded parts, and clean temporary files unless failure retention is enabled.
- Secrets and sensitive connection details must never appear in logs, diagnostics, or user-facing output.
- The dashboard has administrator, operator, and viewer access levels.

## Brand Commitments

The product name is `crab-dump`. Existing terminology and functional names in the repository, including backup, restore, database, routing profile, Telegram, and dashboard, should remain recognizable.

## Evidence on Hand

- Product behavior and setup documentation: `README.md`
- Dashboard pages and styles: `dashboard/*.html`, `dashboard/dashboard.css`, `dashboard/dashboard-auth.js`
- Runtime implementation: `src/`
- Deployment configuration: `Dockerfile`, `docker-compose.yml`, `.env.example`, `config.toml.example`
- Existing architectural records: `docs/ADR-0001-multi-database-support.md`, `docs/ADR-0002-dashboard-database-enable-disable.md`, `docs/ADR-0003-dashboard-triggered-database-backups.md`
- No user testimonials, customer studies, or approved marketing claims are currently established; future work must not fabricate them.

## Product Principles

- Preserve backup integrity and restoreability above convenience.
- Keep sensitive data private by default.
- Fail softly across independent databases and network operations.
- Make operational state understandable and controllable.
- Prefer standard tools and reversible, self-hosted workflows.
