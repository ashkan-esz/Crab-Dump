// Self-check for the dashboard's state → timeline mapping.
// Run with: node test_dashboard.mjs
//
// It loads the <script> block out of index.html (the dashboard has no build
// step) with minimal document/window stubs, then asserts that the backend's
// (state, stage) pairs produce the expected badge and timeline classes.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('./index.html', import.meta.url), 'utf8');
const body = html.match(/<script>([\s\S]*?)<\/script>/)[1];

const noop = () => {};
const stubs = {
    document: { addEventListener: noop, getElementById: () => null },
    window: { addEventListener: noop },
};
const load = new Function(
    ...Object.keys(stubs),
    `${body}\nreturn { dbView, timelineClasses, sizeLine, fmtBytes, fmtSpeed, telegramDestinationText, historyValue, historyMarkup, expandedDatabases, STAGES };`,
);
const { dbView, timelineClasses, sizeLine, fmtBytes, fmtSpeed, telegramDestinationText, historyValue, historyMarkup, expandedDatabases, STAGES } = load(...Object.values(stubs));

assert.deepEqual(STAGES, ['dump', 'package', 'upload']);

/** Assert badge text plus the node/bar classes for one backend payload. */
function check(db, badge, nodes, bars) {
    const v = dbView(db);
    const tl = timelineClasses(v);
    assert.equal(v.badge, badge, `badge for stage=${db.stage} state=${db.state}`);
    assert.deepEqual(tl.nodes, nodes, `nodes for stage=${db.stage} state=${db.state}`);
    assert.deepEqual(tl.bars, bars, `bars for stage=${db.stage} state=${db.state}`);
}

// Queued: nothing lit up yet.
check({ state: 'UP', stage: 'queued' }, 'QUEUED', ['', '', ''], ['', '']);

// Running: current stage active, earlier stages and bars green.
check({ state: 'DEGRADED', stage: 'dump' },    'RUNNING', ['active', '', ''],                 ['', '']);
check({ state: 'DEGRADED', stage: 'package' }, 'RUNNING', ['completed', 'active', ''],        ['filled', '']);
check({ state: 'DEGRADED', stage: 'upload' },  'RUNNING', ['completed', 'completed', 'active'], ['filled', 'filled']);

// Done: every stage and bar green.
check({ state: 'UP', stage: 'done' }, 'DONE',
    ['completed', 'completed', 'completed'], ['filled', 'filled']);

// Failed: the stage that broke is marked, earlier ones stay green.
check({ state: 'DOWN', stage: 'package' }, 'FAILED', ['completed', 'failed', ''], ['filled', '']);
check({ state: 'DOWN', stage: 'upload' },  'FAILED', ['completed', 'completed', 'failed'], ['filled', 'filled']);

// Failing before the first stage must not leave the timeline blank.
check({ state: 'DOWN', stage: 'queued' }, 'FAILED', ['failed', '', ''], ['', '']);

// Upload size / speed formatting.
assert.equal(fmtBytes(0), '0 B');
assert.equal(fmtBytes(512), '512 B');
assert.equal(fmtBytes(1024), '1.0 KiB');
assert.equal(fmtBytes(1536), '1.5 KiB');
assert.equal(fmtBytes(10 * 1024), '10 KiB');
assert.equal(fmtBytes(2.5 * 1024 ** 3), '2.5 GiB');
assert.equal(fmtSpeed(0), '—');                     // idle, not "0 B/s"
assert.equal(fmtSpeed(4 * 1024 ** 2), '4.0 MiB/s');

assert.equal(telegramDestinationText(0), '0 destinations');
assert.equal(telegramDestinationText(1), '1 destination');
assert.equal(telegramDestinationText(3), '3 destinations');

// Dumped size line: hidden with no bytes, no ratio until the payload size is
// known, ratio against the uploaded size once packaging reports it.
assert.equal(sizeLine({ dump_bytes: 0, bytes_total: 0 }), '');
assert.equal(sizeLine({ dump_bytes: 4 * 1024 ** 3, bytes_total: 0 }),
    'dump <strong>4.0 GiB</strong>');
assert.equal(sizeLine({ dump_bytes: 4 * 1024 ** 3, bytes_total: 1024 ** 3 }),
    'dump <strong>4.0 GiB</strong> &nbsp;·&nbsp; compressed 4.0x');

assert.equal(historyValue(18.456), '18.5');
assert.match(historyMarkup({
    stats: { attempts: 2, successes: 1, success_rate: 50, last_run: null, last_success: null,
        average_duration_secs: 2, average_dump_bytes: 1024, average_packaged_bytes: 512,
        average_upload_retries: .5 },
    records: [],
}), /No backup history yet\./);
expandedDatabases.add('app');
assert.equal(expandedDatabases.has('app'), true);
expandedDatabases.delete('app');
assert.equal(expandedDatabases.has('app'), false);

console.log('dashboard timeline, history formatting, and expansion state OK');
