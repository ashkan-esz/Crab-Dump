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
const element = () => ({ classList: { add: noop, remove: noop }, textContent: '' });
const stubs = {
    document: { addEventListener: noop, getElementById: element },
    window: { addEventListener: noop },
};
const load = new Function(
    ...Object.keys(stubs),
    `${body}\nreturn { dbView, timelineClasses, sizeLine, fmtBytes, fmtSpeed, telegramDestinationText, historyValue, historyMarkup, expandedDatabases, databaseOrder, STAGES, renderPhase, renderSchedule, scheduleSecs, formatSchedule };`,
);
const { dbView, timelineClasses, sizeLine, fmtBytes, fmtSpeed, telegramDestinationText, historyValue, historyMarkup, expandedDatabases, databaseOrder, STAGES, renderPhase, renderSchedule, scheduleSecs, formatSchedule } = load(...Object.values(stubs));

assert.doesNotMatch(body, /<button class="db-summary"/);
assert.match(body, /class="db-summary-main"/);
assert.match(body, /role="switch"/);
assert.match(body, /class="db-backup"/);
assert.match(body, /Backup now/);
assert.match(html, /id="manualBackupModal"/);
assert.match(html, /role="dialog"/);
assert.match(html, /class="manual-backup-recipient"/);
assert.match(html, /id="manualBackupNoEncryption" type="checkbox" checked/);
assert.match(html, /Plaintext upload/);
assert.match(body, /manual_backup_available/);
assert.match(body, /api\/telegram-users/);
assert.match(body, /user\.enabled/);
assert.match(body, /no_encryption: noEncryption/);
assert.match(body, /openManualBackupModal/);
assert.match(body, /setupManualBackupModal/);
assert.match(html, /history-table \.action \{ color: var\(--text-muted\); \}/);
assert.match(html, /history-table \.action-disable \{ color: var\(--warn\); \}/);
assert.match(body, /'disable', 'disabled'/);
assert.match(body, /'enable', 'enabled'/);
assert.match(html, /\.db-group-list \{ display: flex; flex-direction: column; gap: 1rem; \}/);
assert.match(body, /page_size=\$\{state\.pageSize\}/);
assert.match(body, /historyCacheKey\(name, state\.page, state\.pageSize\)/);

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
check({ enabled: false, state: 'UP', stage: 'disabled' }, 'DISABLED', ['', '', ''], ['', '']);

assert.deepEqual(
    databaseOrder([
        { name: 'zeta', enabled: false },
        { name: 'beta', enabled: true },
        { name: 'alpha', enabled: true },
        { name: 'aardvark', enabled: false },
    ]).map(db => db.name),
    ['alpha', 'beta', 'aardvark', 'zeta'],
);

// Upload size / speed formatting.
assert.equal(fmtBytes(0), '0 B');
assert.equal(fmtBytes(512), '512 B');
assert.equal(fmtBytes(1024), '1.0 KiB');
assert.equal(fmtBytes(1536), '1.5 KiB');
assert.equal(fmtBytes(10 * 1024), '10 KiB');
assert.equal(fmtBytes(2.5 * 1024 ** 3), '2.5 GiB');
assert.equal(fmtSpeed(0), '-');                     // idle, not "0 B/s"
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
const actionHistory = historyMarkup({
    stats: {},
    records: [
        { started_at: '2026-08-13T00:00:00Z', status: 'enable' },
        { started_at: '2026-08-13T00:00:00Z', status: 'disable' },
    ],
});
const manualHistory = historyMarkup({
    stats: {},
    records: [{ started_at: '2026-08-13T00:00:00Z', source: 'manual', recipient: 'Alice', status: 'success' }],
});
assert.match(manualHistory, /<td class="manual-source">Manual<\/td>/);
assert.match(manualHistory, /<td>Alice<\/td>/);
assert.match(manualHistory, /<th>Receiver<\/th>/);
assert.match(actionHistory, /<td class="action">enable<\/td>/);
assert.match(actionHistory, /<td class="action-disable">disable<\/td>/);
const pagedHistory = historyMarkup({
    page: 2, page_size: 20, total_records: 35, total_pages: 2,
    stats: {}, records: Array.from({ length: 15 }, (_, i) => ({
        started_at: `2026-08-${15 - i}T00:00:00Z`, status: 'success',
    })),
});
assert.match(pagedHistory, /value="10"/);
assert.match(pagedHistory, /value="20" selected/);
assert.match(pagedHistory, /value="50"/);
assert.match(pagedHistory, /21-35 of 35 · Page 2 of 2/);
assert.match(pagedHistory, /class="history-page-button history-prev"/);
assert.match(pagedHistory, /class="history-page-button history-next" disabled/);
expandedDatabases.add('app');
assert.equal(expandedDatabases.has('app'), true);
expandedDatabases.delete('app');
assert.equal(expandedDatabases.has('app'), false);

// Both countdowns derive from independent deadlines, so a delayed timer
// callback cannot overstate either schedule's time remaining.
const originalNow = Date.now;
let now = 1_000_000;
Date.now = () => now;
renderSchedule('backup', 'every 6h', 10, 'one-shot');
renderSchedule('history', 'cron 59 23 * * *', 20, 'disabled');
now += 2_500;
assert.equal(scheduleSecs({ atMs: 1_000_000 + 10_000 }), 8);
assert.equal(scheduleSecs({ atMs: 1_000_000 + 20_000 }), 18);
now += 7_500;
assert.equal(scheduleSecs({ atMs: 1_000_000 + 10_000 }), 0);
assert.equal(scheduleSecs({ atMs: 1_000_000 + 20_000 }), 10);
assert.equal(
    formatSchedule({ label: 'every 6h', atMs: now }),
    'every 6h · due now',
);
Date.now = originalNow;

console.log('dashboard timeline, history formatting, and expansion state OK');

// Telegram users workspace contract checks. The page is intentionally
// dependency-free, so these assertions protect the rendered states and
// mutation wiring at the HTML/JavaScript boundary.
const usersHtml = readFileSync(new URL('./users.html', import.meta.url), 'utf8');
const usersBody = usersHtml.match(/<script>([\s\S]*?)<\/script>/)[1];

assert.match(usersHtml, /<table>/);
assert.match(usersHtml, /class="table-scroll"/);
assert.match(usersHtml, /class="inspector".*role="dialog"/);
assert.match(usersHtml, /role="alertdialog"/);
assert.match(usersHtml, /id="cancel-delete"/);
assert.match(usersHtml, /id="refreshing"/);
assert.match(usersHtml, /id="refresh-notice"/);
assert.match(usersHtml, /No matching users/);
assert.match(usersHtml, /No users yet/);
assert.match(usersHtml, /Add your first user/);
assert.match(usersHtml, /Users could not be loaded/);
assert.match(usersBody, /state\.query\.trim\(\)/);
assert.match(usersBody, /state\.status === 'enabled'/);
assert.match(usersBody, /state\.status === 'all' \|\| \(state\.status === 'enabled' \? user\.enabled : !user\.enabled\)/);
assert.match(usersBody, /localeCompare/);
assert.match(usersBody, /escapeHtml/);
assert.match(usersBody, /method: editingId \? 'PUT' : 'POST'/);
assert.match(usersBody, /enabled: \$\('enabled'\)\.checked/);
assert.match(usersBody, /method: 'PUT', body: JSON\.stringify\(body\)/);
assert.match(usersBody, /Delete \$\{user\.name\} \(\$\{user\.chat_id\}\)\?/);
assert.match(usersBody, /This removes the directory record and cannot be undone/);
assert.match(usersBody, /catch \(error\) \{ setText\('form-summary', error\.message\)/);
assert.doesNotMatch(usersBody, /catch \(error\)[\s\S]{0,180}user-form'\)\.reset/);
assert.match(usersBody, /window\.escapeHtml = escapeHtml/);

const escapeSource = usersBody.match(/function escapeHtml\(value\) \{[\s\S]*?\n  \}/)[0];
const escapeHtml = new Function(`${escapeSource}; return escapeHtml;`)();
assert.equal(escapeHtml(`<script>alert("x")</script> & 'id'`),
    '&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt; &amp; &#39;id&#39;');
assert.doesNotMatch(usersHtml, /—/);

console.log('Telegram users workspace contract OK');
