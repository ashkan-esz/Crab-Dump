# ADR-0003: Dashboard-Triggered Database Backups

| Field       | Value                                  |
|-------------|----------------------------------------|
| **Status**  | Accepted — implemented                 |
| **Date**    | 2026-08-13                             |
| **Authors** | crab-dump maintainers                  |
| **Supersedes** | N/A                                  |

---

## Context

The dashboard exposes live status and history for each configured database,
but operators can only start backups through the normal scheduled or one-shot
execution paths. Starting an immediate backup currently requires changing the
deployment schedule or invoking the process manually.

Operators need a per-database control for ad-hoc backups without weakening the
existing pipeline guarantees:

- scheduled and manual backups must not overlap for the same database;
- the configured parallelism limit must continue to apply;
- one database's failure must not abort its siblings;
- manual backups must use the same cleanup, upload, encryption, and history
  behavior as scheduled backups;
- the dashboard must distinguish manual attempts from scheduled attempts.

## Decision

Add a per-database `Backup now` action to the dashboard. Manual requests are
accepted by the running process, queued in memory, and executed by the
existing blocking scheduler and backup pipeline.

### Dashboard API

Add:

```text
POST /api/status/database/{name}/backup
```

The endpoint accepts only configured database display names and returns:

- `202 Accepted` when the request is queued;
- `404 Not Found` for an unknown database;
- `409 Conflict` when the database is already running, already has a pending
  manual request, or is disabled.

The response does not expose credentials or other configuration secrets.

### Request controller and wake-up

Use one process-wide manual-backup controller shared by the web server and
scheduler. The controller tracks:

- pending manual database names;
- active database names.

Requests for a name already in either set are rejected. A successful request
signals the scheduler so it does not wait for the next scheduled interval or
cron occurrence.

The controller is in-memory and exists only for the lifetime of the running
process. Pending requests are therefore not persisted across restarts.

### Scheduler and pipeline integration

Manual requests are drained by the long-running scheduled mode and executed
through the existing `run_database` pipeline. They share:

- `MAX_PARALLEL_DATABASES`;
- status publishing;
- streaming dump, compression, encryption, chunking, and Telegram upload;
- temporary-file cleanup;
- fail-soft behavior across databases;
- history recording.

Scheduled execution and manual execution use the same per-database active-run
guard. A scheduled run cannot start a database whose manual request is pending
or active, and a manual request cannot start while its database is scheduled
or already running.

Manual requests are available while the long-running dashboard process is
alive. One-shot CLI execution retains its existing behavior and exits after
its normal cycle.

### History source

Add a `source` field to `HistoryRecord` with these values:

```text
manual
scheduled
one-shot
```

The `status` field remains the outcome of the attempt (`success` or
`failure`). Existing JSONL records without `source` deserialize as
`scheduled`, preserving backward compatibility.

Manual attempts are included in the normal history timeline and participate
in success/failure statistics like any other backup attempt. Enable/disable
action records remain excluded from backup statistics as defined by
ADR-0002.

### Dashboard behavior

Each enabled database card includes a `Backup now` button. The button:

- shows queued/running state while the request is outstanding;
- is disabled while the database is queued or running;
- displays a concise conflict or error message when the request is rejected;
- refreshes live status and expanded history after acceptance or completion.

The history table adds a `Source` column. Manual records render with the
distinct `Manual` label; existing records without a source are displayed as
scheduled.

## Consequences

### Positive

- Operators can immediately back up one database without changing schedules.
- Manual work reuses the tested production pipeline and its safety invariants.
- Duplicate and overlapping requests are rejected deterministically.
- Scheduled work remains bounded by the configured concurrency limit.
- Manual attempts are visible and distinguishable in database history.
- The scheduler wakes promptly when a manual request arrives.

### Negative / Trade-offs

- Pending manual requests are lost if the process exits or restarts.
- The dashboard cannot trigger a backup in one-shot mode after its normal
  cycle has completed.
- The in-memory controller adds process-global coordination between the
  dashboard thread and the scheduler.
- A manual request may wait behind other active work when all parallel slots
  are occupied.
- The dashboard remains a trusted local control surface and has no
  authentication, consistent with ADR-0002.

## Alternatives considered

### Start a separate backup subprocess per dashboard request

Rejected because it could bypass the existing parallelism limit, duplicate
pipeline setup, and create overlapping work for the same database.

### Run manual requests in the web-server thread

Rejected because the backup pipeline is blocking and long-running. Running it
there would interfere with HTTP responsiveness and make scheduler
coordination harder.

### Persist manual requests to disk

Rejected because requests are operational triggers, not durable backup
configuration. Persistence would require restart recovery semantics and could
unexpectedly start old requests after deployment changes.

### Add a global “backup all” action

Rejected because the operational need is targeted recovery or verification of
one database, and a global action would create unnecessary load.

## Open questions

| Question | Tentative answer |
|----------|------------------|
| Should manual requests survive restarts? | No; treat them as transient operator commands. |
| Should disabled databases accept manual backup requests? | No; return `409 Conflict` and require re-enabling first. |
| Should manual backups have a separate retention policy? | No; they are ordinary backup attempts and use existing history retention. |
| Should the API require authentication? | Not in the current local/trusted deployment model; revisit if dashboard binding expands. |

