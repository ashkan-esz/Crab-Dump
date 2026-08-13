# ADR-0002: Dashboard Database Enable/Disable

| Field       | Value                                  |
|-------------|----------------------------------------|
| **Status**  | Accepted — implemented                 |
| **Date**    | 2026-08-13                             |
| **Authors** | crab-dump maintainers                  |
| **Supersedes** | N/A                                  |

---

## Context

crab-dump can back up several configured databases in one process, but
temporarily excluding a database requires changing deployment configuration or
stopping the process. That is inconvenient during maintenance windows,
database migrations, and investigations.

The dashboard already exposes per-database status and history, so it is the
natural control surface for temporarily pausing a configured database. The
control must survive process restarts and must not interrupt a backup that is
already in progress.

## Decision

Add persistent per-database enablement controlled by the local dashboard.

### Persistent state

Store state in:

```text
HISTORY_DIR/database-state.json
```

The state file is separate from monthly JSONL history but uses the same
history volume. Database display names are the keys because they already
identify dashboard cards, history queries, and configured databases.

State behavior:

- Missing state defaults every configured database to enabled.
- Malformed or unreadable state defaults databases to enabled and emits a
  warning.
- Names no longer present in configuration are ignored.
- Newly configured names default to enabled.
- Updates are written through a temporary file and atomic rename.

Only configured database names are accepted by the dashboard API.

### Dashboard API

Extend `GET /api/status/databases` with an `enabled` boolean for each entry.

Add these endpoints:

```text
POST /api/status/database/{name}/enable
POST /api/status/database/{name}/disable
```

Each successful request returns the database name and its new `enabled` state.
Unknown databases return HTTP 404.

Every successful toggle appends a synthetic history record with status
`enabled` or `disabled`, the database identity, a timestamp, and zero backup
metrics.

### Backup-cycle behavior

The persisted state is loaded once at startup and shared by the dashboard and
backup executor.

- Disabled databases remain registered in the dashboard.
- A disabled database is shown as `DISABLED` and has no active pipeline.
- Disabled databases are filtered out before a cycle starts.
- Disabled databases do not count as failures.
- One-shot mode exits successfully when all configured databases are
  disabled.
- If a database is disabled while its backup is running, that backup finishes.
  The new state is observed by the next cycle.

### History behavior

Synthetic `enabled` and `disabled` records remain visible in the returned
database timeline. They are excluded from backup statistics:

- attempts, successes, and failures;
- success rate;
- duration, dump-size, packaged-size, and retry averages;
- last backup run and last successful backup.

The dashboard labels action records distinctly from successful and failed
backup attempts.

### Dashboard controls

Each database card includes an explicit enable/disable button separate from
the history-expansion button. Toggling clears the cached history for that
database and refreshes its card state.

## Consequences

### Positive

- Operators can pause or resume one database without changing deployment
  configuration.
- Enablement survives restarts.
- A toggle cannot corrupt the state file if the process fails during
  persistence.
- In-flight backups are not interrupted, preserving pipeline cleanup and
  restore guarantees.
- The audit trail makes operational state changes visible alongside backup
  history.
- Disabled databases do not create false failures or non-zero one-shot exits.

### Negative / Trade-offs

- Database display names become a persisted operational identity and should
  not be casually renamed.
- The state file is local to the configured `HISTORY_DIR`; replacing or
  deleting that volume resets all databases to enabled.
- The dashboard is a trusted local control surface and does not add
  authentication.
- State changes take effect at cycle boundaries, so disabling an active
  backup does not stop work immediately.
- Synthetic action records add non-backup entries to the JSONL history and
  require explicit filtering in consumers that calculate backup metrics.

## Alternatives considered

### Environment-only enablement

Rejected because it requires a deployment/configuration change for every
toggle and does not provide an operational audit record.

### Stop or cancel an active backup on disable

Rejected because cancellation complicates subprocess termination, temporary
file cleanup, and partial upload handling. Boundary-based application is
safer and matches the existing cycle model.

### Separate database-state database or schema

Rejected as unnecessary operational infrastructure. A small atomic JSON file
fits the existing history volume and avoids adding a dependency or migration
path.

## Open questions

| Question | Tentative answer |
|----------|------------------|
| Should state be keyed by immutable database IDs? | Not currently needed; display names are already validated as unique and are the dashboard/history identity. |
| Should the API require authentication? | Not in the current local/trusted deployment model; revisit if the dashboard bind scope expands. |
| Should disabled databases be removed from the dashboard? | No; preserving the card makes the state visible and allows re-enabling it. |
