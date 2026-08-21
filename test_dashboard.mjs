// Self-check for the dashboard's state → timeline mapping.
// Run with: node test_dashboard.mjs
//
// It loads the <script> block out of index.html (the dashboard has no build
// step) with minimal document/window stubs, then asserts that the backend's
// (state, stage) pairs produce the expected badge and timeline classes.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const dashboardDir = './dashboard/';
const html = readFileSync(new URL(`${dashboardDir}index.html`, import.meta.url), 'utf8');
const dashboardPages = ['index.html', 'databases.html', 'users.html', 'routing.html', 'services.html'];
const pageHtml = Object.fromEntries(dashboardPages.map(file => [
    file,
    readFileSync(new URL(`${dashboardDir}${file}`, import.meta.url), 'utf8'),
]));
const authJs = readFileSync(new URL(`${dashboardDir}dashboard-auth.js`, import.meta.url), 'utf8');
const body = html.match(/<script>([\s\S]*?)<\/script>/)[1];

const noop = () => {};
const element = () => ({ classList: { add: noop, remove: noop }, textContent: '' });
const stubs = {
    document: { addEventListener: noop, getElementById: element },
    window: { addEventListener: noop },
};
const load = new Function(
    ...Object.keys(stubs),
    `${body}\nreturn { dbView, timelineClasses, sizeLine, fmtBytes, fmtSpeed, telegramDestinationText, historyValue, historyMarkup, expandedDatabases, databaseOrder, STAGES, renderPhase, renderSchedule, scheduleSecs, formatSchedule, uploadPercent, uploadDetailsVisible, progressLabel };`,
);
const { dbView, timelineClasses, sizeLine, fmtBytes, fmtSpeed, telegramDestinationText, historyValue, historyMarkup, expandedDatabases, databaseOrder, STAGES, renderPhase, renderSchedule, scheduleSecs, formatSchedule, uploadPercent, uploadDetailsVisible, progressLabel } = load(...Object.values(stubs));

function operationsLinks(source) {
    const nav = source.match(/<span class="app-nav-label">Operations<\/span>\s*<div class="app-nav-links">([\s\S]*?)<\/div>/);
    assert.ok(nav, 'Operations navigation group must exist');
    return [...nav[1].matchAll(/<a href="([^"]+)"[^>]*>[\s\S]*?<span>([^<]+)<\/span><\/a>/g)]
        .map(([, href, label]) => ({ href, label }));
}

const expectedOperations = [
    { href: '/', label: 'Overview' },
    { href: '/databases', label: 'Databases' },
    { href: '/services', label: 'Services' },
    { href: '/users', label: 'Telegram users' },
    { href: '/routing', label: 'Routing' },
];
for (const [file, source] of Object.entries(pageHtml)) {
    const operations = source.match(/<span class="app-nav-label">Operations<\/span>\s*<div class="app-nav-links">([\s\S]*?)<\/div>/)?.[1] ?? '';
    assert.doesNotMatch(operations, />Backup history</, `${file} must omit the Backup history sidebar link`);
    assert.deepEqual(operationsLinks(source), expectedOperations, `${file} Operations navigation`);
}
const routingStyles = pageHtml['routing.html'].split('<link rel="stylesheet" href="/dashboard.css">', 1)[0];
assert.doesNotMatch(routingStyles, /\.app-nav\s*\{/, 'routing.html must use shared sidebar geometry');
assert.doesNotMatch(routingStyles, /(?:^|[}])\s*main(?:\s*[,{]|\s*\{)/, 'routing.html must not override main layout');
assert.doesNotMatch(routingStyles, /\.routing-main\s*\{[^}]*\b(?:width|margin-left|position)\s*:/, 'routing.html must not override shared content geometry');
assert.match(pageHtml['routing.html'], /<main id="routing-main" class="routing-main">/);
assert.match(pageHtml['routing.html'], /id="profileForm"/);
assert.match(pageHtml['routing.html'], /id="profiles" aria-label="Saved routing profiles"/);
assert.match(pageHtml['routing.html'], /id="profileForm" class="routing-form" autocomplete="off"/);
assert.match(pageHtml['routing.html'], /id="profileUrl" name="routing-share-url" readonly type="password"[^>]*autocomplete="new-password"/);

assert.doesNotMatch(body, /<button class="db-summary"/);
assert.match(body, /class="db-summary-main"/);
assert.match(body, /className = 'db-row'/);
assert.match(body, /class="db-status"/);
assert.match(body, /class="db-pipeline"/);
assert.match(body, /class="db-progress"/);
assert.match(body, /class="db-upload-row" hidden/);
assert.match(body, /class="db-progress-percent"/);
assert.match(body, /class="db-progress-bytes"/);
assert.match(body, /class="db-progress-meter" role="progressbar"/);
assert.match(body, /aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"/);
assert.match(body, /aria-label="Upload progress unavailable"/);
assert.match(body, /class="db-progress-speed"/);
assert.match(body, /class="db-column-label">Status/);
assert.match(body, /class="db-column-label">Database/);
assert.match(body, /class="db-column-label">Pipeline/);
assert.match(body, /class="db-column-label">Progress/);
assert.doesNotMatch(body, /class="db-column-label">Actions/);
assert.match(html, /\.db-actions \{ flex-direction: column; align-items: flex-end; \}/);
assert.match(body, /<section class="db-history" hidden/);
assert.match(body, /setAttribute\('aria-controls', historyId\)/);
assert.match(body, /role="switch"/);
assert.match(body, /class="db-backup"/);
assert.match(html, /\.db-backup\.cancel-action \{[\s\S]*background: var\(--err\)/);
assert.match(html, /\.db-backup\.cancel-action \{[\s\S]*white-space: normal/);
assert.match(html, /\.db-backup\.cancel-action \{[\s\S]*text-align: center/);
assert.match(html, /\.db-toggle, \.db-backup \{[\s\S]*width: 6\.3rem/);
assert.match(html, /\.db-backup \{[\s\S]*font-size: \.56rem/);
assert.match(body, /Backup Now/);
assert.match(body, /class="db-chunk-info" hidden/);
assert.match(body, /class="db-chunk-count"/);
assert.match(body, /class="db-chunk-current"/);
assert.doesNotMatch(body, /class="tr-chunk"/);
assert.match(body, /current_chunk_done/);
assert.match(body, /current_chunk_total/);
assert.match(html, /id="manualBackupModal"/);
assert.match(html, /role="dialog"/);
assert.match(html, /class="manual-backup-recipient"/);
assert.match(html, /Encryption follows the server's/);
assert.match(html, /id="manualBackupNoEncryption"/);
assert.match(html, /id="manualBackupNoEncryptionInput" type="checkbox"/);
assert.match(html, /id="manualBackupNoEncryption" class="manual-backup-no-encryption" hidden/);
assert.match(body, /dashboard_role/);
assert.match(body, /config\?\.dashboard_role !== 'admin'/);
assert.match(body, /no_encryption: noEncryption === true/);
assert.match(body, /Only administrators may upload backups without encryption/);
assert.match(body, /manual_backup_available/);
assert.match(body, /api\/telegram-users/);
assert.match(body, /user\.enabled/);
assert.match(body, /body: JSON\.stringify\(\{ chat_id: chatId, no_encryption: noEncryption === true \}\)/);
assert.match(body, /openManualBackupModal/);
assert.match(body, /setupManualBackupModal/);
assert.match(html, /id="tgTestBtn"/);
assert.match(html, /<details class="card service-card up" id="tgCard">/);
assert.match(html, /<details class="card service-card up" id="dumpCard">/);
assert.match(html, /<summary class="service-summary">/);
assert.match(html, /service-details-label">View details<\/span>/);
for (const id of ['tgCard', 'dumpCard', 'tgMsg', 'dumpMsg', 'tgTime', 'dumpTime', 'tgTestMsg']) {
    assert.match(html, new RegExp(`id="${id}"`), `${id} must remain available to polling`);
}
assert.match(html, /id="tgTestMsg" aria-live="polite"/);
assert.match(body, /api\/status\/service\/test/);
assert.match(body, /testTelegramApi/);
assert.match(body, /Testing\.\.\./);
assert.match(body, /test_disabled_reason/);
assert.match(body, /renderTelegramTestState/);
assert.match(body, /Test unavailable:/);
assert.match(body, /disabled_reason/);
assert.match(html, /history-table \.action \{ color: var\(--text-muted\); \}/);
assert.match(html, /history-table \.action-disable \{ color: var\(--warn\); \}/);
assert.match(body, /'disable', 'disabled'/);
assert.match(body, /'enable', 'enabled'/);
assert.match(html, /\.db-row \{/);
assert.match(html, /grid-template-columns: minmax\(12rem, 1\.45fr\) minmax\(5rem, \.45fr\) minmax\(22rem, 1\.8fr\) auto/);
assert.match(html, /\.db-timeline \{[\s\S]*width: 100%;[\s\S]*max-width: none/);
assert.match(html, /\.db-timeline \{ max-width: 320px; \}/);
assert.match(html, /\.db-upload-row \{/);
assert.match(html, /\.db-upload-row\[hidden\] \{ display: none; \}/);
assert.match(html, /\.db-progress-meter \{/);
assert.match(html, /\.db-row\.running \.db-progress-meter span \{ background: var\(--warn\); \}/);
assert.match(html, /\.db-row\.done \.db-progress-meter span \{ background: var\(--up\); \}/);
assert.match(html, /\.db-row\.disabled \.db-progress-meter span,[\s\S]*background: var\(--idle\)/);
assert.match(html, /\.db-row\.failed \.db-progress-meter span \{ background: var\(--err\); \}/);
assert.match(html, /font-variant-numeric: tabular-nums/);
assert.match(html, /\.db-row:focus-within/);
assert.match(html, /@media \(max-width: 640px\)[\s\S]*\.db-summary-main, \.db-status, \.db-pipeline, \.db-actions/);
assert.doesNotMatch(html, /\.db-card:hover/);
assert.doesNotMatch(body, /closest\?\.\('\.card, \.db-card'\)/);
assert.match(body, /page_size=\$\{state\.pageSize\}/);
assert.match(body, /historyCacheKey\(name, state\.page, state\.pageSize\)/);
assert.match(html, /id="resourceCard"/);
assert.match(html, /id="resourceCpuFill"/);
assert.match(html, /id="resourceMemoryFill"/);
assert.match(html, /id="resourceDiskFill"/);
assert.match(body, /api\/status\/resources/);
assert.match(body, /renderResourcesError/);
assert.match(body, /Updated \$\{relTime\(data\.timestamp\)\}/);
assert.match(html, /id="resourceUpdated"/);
assert.match(html, /id="resourceCpuMetric"/);
assert.match(html, /id="resourceMemoryMetric"/);
assert.match(html, /id="resourceDiskMetric"/);
assert.match(html, /aria-live="polite"/);
assert.match(html, /id="compressionCard"/);
assert.match(html, /id="compressionToggle"/);
assert.match(html, /aria-expanded="false"/);
assert.match(html, /aria-controls="compressionForm"/);
assert.match(html, /id="compressionSummary"/);
assert.match(html, /id="compressionForm" class="compression-form" hidden/);
assert.match(html, /id="compressionCodec"/);
assert.match(html, /id="compressionLevel"/);
assert.match(html, /id="compressionChecksum"/);
assert.match(body, /api\/compression-config/);
assert.match(body, /next backup cycle/);
assert.match(body, /Only an administrator can edit compression settings/);
assert.match(body, /api\/status\/database\/\$\{encodeURIComponent\(name\)\}\/cancel/);
assert.match(body, /CANCELLED/);
assert.match(body, /cancelDatabase\(db\.name\)/);
assert.match(body, /container scope/);
assert.match(html, /WORK_DIR disk/);

assert.deepEqual(STAGES, ['dump', 'compression', 'encryption', 'upload']);

/** Assert badge text plus the node/bar classes for one backend payload. */
function check(db, badge, nodes, bars) {
    const v = dbView(db);
    const tl = timelineClasses(v, db);
    assert.equal(v.badge, badge, `badge for stage=${db.stage} state=${db.state}`);
    assert.deepEqual(tl.nodes, nodes, `nodes for stage=${db.stage} state=${db.state}`);
    assert.deepEqual(tl.bars, bars, `bars for stage=${db.stage} state=${db.state}`);
}

// Queued: nothing lit up yet.
check({ state: 'UP', stage: 'queued' }, 'QUEUED', ['', '', '', ''], ['', '', '']);

// Running: current stage active, earlier stages and bars green.
check({ state: 'DEGRADED', stage: 'dump' },        'RUNNING', ['active', '', '', ''],                         ['', '', '']);
check({ state: 'DEGRADED', stage: 'compression' }, 'RUNNING', ['completed', 'active', '', ''],              ['filled', '', '']);
check({ state: 'DEGRADED', stage: 'encryption' },  'RUNNING', ['completed', 'completed', 'active', ''],     ['filled', 'filled', '']);
check({ state: 'DEGRADED', stage: 'upload' },      'RUNNING', ['completed', 'completed', 'completed', 'active'], ['filled', 'filled', 'filled']);

// Done: every stage and bar green.
check({ state: 'UP', stage: 'done' }, 'DONE',
    ['completed', 'completed', 'completed', 'completed'], ['filled', 'filled', 'filled']);

// Failed: the stage that broke is marked, earlier ones stay green.
check({ state: 'DOWN', stage: 'compression' }, 'FAILED', ['completed', 'failed', '', ''], ['filled', '', '']);
check({ state: 'DOWN', stage: 'encryption' },  'FAILED', ['completed', 'completed', 'failed', ''], ['filled', 'filled', '']);
check({ state: 'DOWN', stage: 'upload' },      'FAILED', ['completed', 'completed', 'completed', 'failed'], ['filled', 'filled', 'filled']);

// Failing before the first stage must not leave the timeline blank.
check({ state: 'DOWN', stage: 'queued' }, 'FAILED', ['failed', '', '', ''], ['', '', '']);
check({ enabled: false, state: 'UP', stage: 'disabled' }, 'DISABLED', ['', '', '', ''], ['', '', '']);
check({ state: 'DEGRADED', stage: 'encryption', compression_enabled: false, encryption_enabled: false },
    'RUNNING', ['completed', 'skipped', 'skipped', ''], ['filled', 'filled', '']);

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
assert.equal(fmtBytes(1024), '1.0 KB');
assert.equal(fmtBytes(1536), '1.5 KB');
assert.equal(fmtBytes(10 * 1024), '10 KB');
assert.equal(fmtBytes(2.5 * 1024 ** 3), '2.5 GiB');
assert.equal(fmtSpeed(0), '-');                     // idle, not "0 B/s"
assert.equal(fmtSpeed(Number.NaN), '-');
assert.equal(fmtSpeed(Number.POSITIVE_INFINITY), '-');
assert.equal(fmtSpeed(-1), '-');
assert.equal(fmtSpeed(4 * 1024 ** 2), '4.0 MB/s');
assert.equal(uploadPercent({ bytes_done: 250, bytes_total: 1000 }), 25);
assert.equal(uploadPercent({ bytes_done: 2000, bytes_total: 1000 }), 100);
assert.equal(uploadPercent({ bytes_done: 2000, bytes_total: 0 }), 0);
assert.equal(uploadDetailsVisible({ stage: 'queued' }), false);
assert.equal(uploadDetailsVisible({ stage: 'dump' }), false);
assert.equal(uploadDetailsVisible({ stage: 'compression' }), false);
assert.equal(uploadDetailsVisible({ stage: 'encryption' }), false);
assert.equal(uploadDetailsVisible({ stage: 'upload' }), true);
assert.equal(uploadDetailsVisible({ stage: 'done' }), true);
assert.equal(uploadDetailsVisible({ stage: 'failed' }), false);
assert.equal(uploadDetailsVisible({ stage: 'cancelled' }), false);
assert.equal(progressLabel({ stage: 'queued' }, dbView({ state: 'UP', stage: 'queued' }), 0),
    'Backup queued; upload progress unavailable');
assert.equal(progressLabel({ stage: 'upload' }, dbView({ state: 'DEGRADED', stage: 'upload' }), 42),
    'Uploading backup: 42% complete');
assert.equal(progressLabel({ stage: 'done' }, dbView({ state: 'UP', stage: 'done' }), 100),
    'Backup completed: 100% uploaded');
assert.equal(progressLabel({ stage: 'compression' }, dbView({ state: 'DOWN', stage: 'compression' }), 42),
    'Backup failed during compression; 42% uploaded');
assert.equal(progressLabel({ stage: 'disabled' }, dbView({ enabled: false, state: 'UP', stage: 'disabled' }), 0),
    'Backups disabled; upload progress unavailable');
assert.equal(progressLabel({ stage: 'cancelled' }, dbView({ state: 'UP', stage: 'cancelled' }), 0),
    'Backup cancelled; upload progress unavailable');

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
    records: [{
        started_at: '2026-08-13T00:00:00Z',
        source: 'manual',
        recipient: 'Alice',
        status: 'success',
        compression_type: 'zstd',
        compression_level: 7,
    }],
});
assert.match(manualHistory, /<td class="manual-source">Manual<\/td>/);
assert.match(manualHistory, /<td>Alice<\/td>/);
assert.match(manualHistory, /<th scope="col">Receiver<\/th>/);
assert.match(manualHistory, /<th scope="col">Compression<\/th>/);
assert.match(manualHistory, /<th scope="col">Encryption<\/th>/);
assert.match(manualHistory, /<td>zstd 7<\/td>/);
assert.equal(dbView({ enabled: true, state: 'UP', stage: 'cancelled' }).badge, 'CANCELLED');
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
const usersHtml = readFileSync(new URL(`${dashboardDir}users.html`, import.meta.url), 'utf8');
const usersBody = usersHtml.match(/<script>([\s\S]*?)<\/script>/)[1];
const databasesHtml = readFileSync(new URL(`${dashboardDir}databases.html`, import.meta.url), 'utf8');

assert.match(usersHtml, /<table>/);
assert.match(usersHtml, /class="table-scroll"/);
assert.match(usersHtml, /\.summary \{[\s\S]*border: 1px solid var\(--border\)/);
assert.match(usersHtml, /\.summary-item \{[\s\S]*gap: 7px; \}/);
assert.doesNotMatch(usersHtml, /margin-left:\s*12px/);
assert.match(usersHtml, /class="inspector".*role="dialog"/);
assert.match(usersHtml, /role="alertdialog"/);
assert.match(usersHtml, /id="cancel-delete"/);
assert.match(usersHtml, /id="delete-error"/);
assert.match(usersBody, /Deleting…/);
assert.match(usersBody, /delete-error'\)\.hidden = false/);
assert.match(usersHtml, /id="refreshing"/);
assert.match(usersHtml, /id="refresh-notice"/);
assert.match(usersHtml, /No matching users/);
assert.match(usersHtml, /No users yet/);
assert.match(usersHtml, /Add your first user/);
assert.match(usersHtml, /Viewer access is read-only/);
assert.match(usersBody, /admin = config\.dashboard_role === 'admin'/);
assert.match(usersBody, /Read-only/);
assert.match(usersHtml, /Users could not be loaded/);
assert.match(usersBody, /state\.query\.trim\(\)/);
assert.match(usersBody, /state\.status === 'enabled'/);
assert.match(usersBody, /state\.status === 'all' \|\| \(state\.status === 'enabled' \? user\.enabled : !user\.enabled\)/);
assert.match(usersBody, /localeCompare/);
assert.match(usersBody, /escapeHtml/);
assert.match(usersBody, /method: editingId \? 'PUT' : 'POST'/);
assert.match(usersBody, /enabled: \$\('enabled'\)\.checked/);
assert.match(usersBody, /button\.textContent = 'Saving…'/);
assert.match(usersBody, /user-form'\)\.setAttribute\('aria-busy', 'true'\)/);
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

const routingHtml = readFileSync(new URL(`${dashboardDir}routing.html`, import.meta.url), 'utf8');
assert.match(routingHtml, /disableRouting/);
assert.match(routingHtml, /api\/routing\/disable/);
assert.match(routingHtml, /ss:\/\/|trojan:\/\//);
assert.match(routingHtml, /Saved profiles will remain available/);
assert.match(routingHtml, /Check all/);
assert.match(routingHtml, /api\/routing\/profiles\/check-all/);
assert.match(routingHtml, /checkSummary/);
assert.match(routingHtml, /CHECK FAILED/);
assert.match(routingHtml, /role="status"/);
assert.match(routingHtml, /setProfilesBusy/);
assert.match(routingHtml, /setMessage\(e\.message,'error'\)/);
assert.match(routingHtml, /finally\{setProfilesBusy\(false\)\}/);
assert.match(routingHtml, /api\/routing\/status/);
assert.match(routingHtml, /api\/routing\/core/);
assert.match(routingHtml, /compatible_cores/);
assert.match(routingHtml, /coreSelect/);
assert.match(routingHtml, /dashboardAuth\.request/);
assert.match(routingHtml, /previous working route/);
assert.match(routingHtml, /profile-status/);
assert.match(routingHtml, /routingRetry/);
assert.match(routingHtml, /Saved profiles are unavailable until the profiles request succeeds/);
assert.match(routingHtml, /Promise\.allSettled\(\[api\('\/api\/routing\/profiles'\),api\('\/api\/routing\/status'\)\]\)/);
assert.match(routingHtml, /Routing status unavailable:/);
assert.match(routingHtml, /Routing profiles could not be loaded: \$\{e\.message\}/);
assert.match(routingHtml, /const routingRetry=document\.getElementById\('routingRetry'\)/);
assert.match(routingHtml, /const routingStatus=document\.getElementById\('routingStatus'\)/);
assert.match(routingHtml, /profileSubmit/);
assert.match(routingHtml, /setMessage\('Saving profile…','busy'\)/);
assert.match(routingHtml, /dashboardRole==='admin'/);
assert.match(routingHtml, /onclick="editP\('\$\{p\.id\}'\)">Edit/);
assert.match(routingHtml, /let checkResults=\{\};let routingState=null;let dashboardRole='viewer';let editingProfile=null/);
assert.match(routingHtml, /function setProfileFormMode\(profile=null\)/);
assert.match(routingHtml, /profileFormTitle\.textContent=editing\?'Edit profile':'Add a profile'/);
assert.match(routingHtml, /profileSubmitLabel\.textContent=editing\?'Save changes':'Create profile'/);
assert.match(routingHtml, /id="profileCancel" class="routing-form-cancel" type="button" hidden/);
assert.match(routingHtml, /function cancelEdit\(\)\{setProfileFormMode\(\)\}/);
assert.match(routingHtml, /Saved share URLs are never displayed\. Paste the profile URL again/);
assert.match(routingHtml, /const path=editing\?`\/api\/routing\/profiles\/\$\{editing\.id\}`:'\/api\/routing\/profiles'/);
assert.match(routingHtml, /const method=editing\?'PUT':'POST'/);
assert.match(routingHtml, /Profile updated\. Re-apply it to use the new configuration\./);
assert.match(routingHtml, /routingPermission/);
assert.match(routingHtml, /canOperate=canAdmin\|\|dashboardRole==='operator'/);
assert.match(routingHtml, /Operator access can apply or disable routes/);
assert.match(routingHtml, /id="profiles" aria-label="Saved routing profiles" aria-busy="true"/);
assert.match(routingHtml, /profiles\.setAttribute\('aria-busy','false'\)/);
assert.match(routingHtml, /\(async\(\)=>\{await loadRole\(\);await loadRouting\(\)\}\)\(\)/);
assert.doesNotMatch(routingHtml, /Promise\.all\(\[loadRouting\(\),loadRole\(\)\]\)\.then\(\(\)=>refresh\(\)\)/);
assert.match(routingHtml, /routingStatus\.append\(routingRetry\).*routingRetry\.hidden=false/);
assert.match(routingHtml, /routingStatus\.className='routing-status status error'/);
assert.match(routingHtml, /routingStatus\.append\(routingRetry\);routingStatus\.className='routing-status status error'/);
console.log('Routing disable workspace contract OK');

// Shared operations shell contract. Every dashboard surface must expose the
// same destinations so operators do not have to hunt for a return link.
const pageFiles = ['index.html', 'databases.html', 'routing.html', 'users.html', 'services.html'];
for (const file of pageFiles) {
    const source = readFileSync(new URL(`${dashboardDir}${file}`, import.meta.url), 'utf8');
    assert.match(source, /<nav class="app-nav" aria-label="Primary navigation">/);
    assert.match(source, /class="skip-link"/);
    assert.match(source, /Skip to main content/);
    assert.match(source, /<link rel="stylesheet" href="\/dashboard\.css">/);
    assert.match(source, /class="app-signout"/);
    assert.match(source, /<script src="\/dashboard-auth\.js"><\/script>/);
    if (file === 'databases.html') {
        assert.match(source, /id="sign-out" class="app-signout" type="button">Sign out/);
        assert.match(source, /querySelector\('#sign-out'\)\.addEventListener\('click', dashboardAuth\.signOut\)/);
        assert.doesNotMatch(source, /onclick="signOut\(\)"/);
    }
    assert.match(source, /id="sidebarRole"/);
    assert.match(source, /Current role/);
    for (const href of ['/', '/databases', '/routing', '/users']) {
        assert.match(source, new RegExp(`href="${href.replace('/', '\\/')}"`));
    }
    const visible = source
        .replace(/<script[\s\S]*?<\/script>/gi, '')
        .replace(/<style[\s\S]*?<\/style>/gi, '');
    assert.doesNotMatch(visible, /—|–/, `${file} contains a visible dash variant`);
    assert.doesNotMatch(source, /createElement\(['"]dialog['"]\)/, `${file} retains inline auth dialog`);
}
assert.match(authJs, /createElement\('dialog'\)/);
assert.match(authJs, /name="username"/);
assert.match(authJs, /name="password"/);
assert.match(authJs, /autocomplete="username"/);
assert.match(authJs, /autocomplete="current-password"/);
assert.match(authJs, /aria-labelledby/);
assert.match(authJs, /aria-describedby/);
assert.match(authJs, /data-dashboard-auth-cancel/);
assert.match(authJs, /showModal\(\)/);
assert.match(authJs, /addEventListener\('cancel'/);
assert.match(authJs, /api\/auth\/login/);
assert.match(authJs, /api\/auth\/logout/);
assert.match(authJs, /status === 401/);
assert.match(authJs, /X-CSRF-Token/);
assert.match(authJs, /window\.dashboardAuth/);
assert.match(authJs, /window\.signOut/);
assert.match(authJs, /dashboard-auth-dialog/);
assert.match(authJs, /dashboard-auth-error/);
assert.match(authJs, /disabled = true/);
assert.match(authJs, /Sign in/);
assert.match(authJs, /try again/);
const dashboardCss = readFileSync(new URL(`${dashboardDir}dashboard.css`, import.meta.url), 'utf8');
assert.match(dashboardCss, /\.routing-status \{[\s\S]*min-width: 0;[\s\S]*overflow-wrap: anywhere;[\s\S]*word-break: break-word/);
assert.match(dashboardCss, /\.routing-status button \{[\s\S]*max-width: 100%;[\s\S]*overflow-wrap: anywhere/);
assert.match(dashboardCss, /body \{[\s\S]*overflow-x: hidden/);
assert.match(dashboardCss, /button:focus-visible,[\s\S]*a:focus-visible/);
assert.match(dashboardCss, /@media \(prefers-reduced-motion: reduce\)/);
assert.match(dashboardCss, /dashboard-auth-dialog/);
assert.match(dashboardCss, /var\(--dash-surface\)/);
assert.match(dashboardCss, /var\(--dash-border\)/);
assert.match(dashboardCss, /var\(--dash-radius-lg\)/);
assert.match(dashboardCss, /var\(--dash-z-dialog\)/);
assert.match(dashboardCss, /var\(--dash-shadow-dialog\)/);
// Pages with no local CSS depend on the shared sheet for these two; without
// them routing.html and services.html show a permanently visible skip link and
// default cursors on every button.
assert.match(dashboardCss, /\.skip-link \{[\s\S]*position: fixed;[\s\S]*transform: translateY\(-180%\)/);
assert.match(dashboardCss, /\.skip-link:focus,[\s\S]*transform: translateY\(0\)/);
assert.match(dashboardCss, /button \{\s*cursor: pointer;\s*\}/);
// Service workspace: `.svc-*` namespace, split layout, and the released table
// floor for the panel-sized incident table.
assert.match(dashboardCss, /Service health workspace/);
assert.match(dashboardCss, /\.svc-layout \{[\s\S]*grid-template-columns: minmax\(230px, \.8fr\) minmax\(0, 1\.7fr\)/);
assert.match(dashboardCss, /\.svc-row\[aria-current="true"\] \{[\s\S]*box-shadow: inset 2px 0 0 var\(--dash-accent\)/);
assert.match(dashboardCss, /\.svc-incident-table table \{\s*min-width: 0;\s*\}/);
assert.doesNotMatch(dashboardCss.replace(/\/\*[\s\S]*?\*\//g, ''),
    /\.service-card|\.service-summary|\.service-meta|\.service-grid/,
    'dashboard.css must not claim the .service-* names index.html owns locally');
assert.match(routingHtml, /<header class="page-header">[\s\S]*class="eyebrow"[\s\S]*<h1>Routing profiles<\/h1>[\s\S]*class="lede"/);
assert.doesNotMatch(routingHtml.slice(0, routingHtml.indexOf('<link rel="stylesheet" href="/dashboard.css">')), /<style/);
assert.match(dashboardCss, /\.routing-main > form \{[\s\S]*grid-template-columns: minmax\(0, 1fr\) minmax\(0, 1\.4fr\) auto/);
assert.match(dashboardCss, /margin-left: calc\(var\(--dash-nav-width\) \+ var\(--dash-space-6\)\)/);
assert.match(dashboardCss, /\.routing-main > \.page-header,[\s\S]*max-width: 980px/);
assert.match(dashboardCss, /\.routing-main > form label \{[\s\S]*background: transparent[\s\S]*border: 0/);
assert.match(dashboardCss, /\.profile-details \{[\s\S]*min-width: 0/);
assert.match(dashboardCss, /\.profile-actions,[\s\S]*flex-wrap: wrap/);
assert.match(dashboardCss, /\.routing-main > form \{[\s\S]*grid-template-columns: 1fr;[\s\S]*\.routing-main > form button \{[\s\S]*width: 100%/);
assert.match(dashboardCss, /@media \(max-width: 480px\) \{[\s\S]*\.page,[\s\S]*#dashboard-main,[\s\S]*#databases-main/);
assert.match(dashboardCss, /#dashboard-main \.db-backup\.cancel-action,[\s\S]*color: var\(--dash-danger\)/);
assert.match(dashboardCss, /#dashboard-main \.db-backup\.cancel-action,[\s\S]*background: transparent/);
assert.match(dashboardCss, /#dashboard-main \.db-backup\.cancel-action:hover[\s\S]*background: var\(--dash-danger-soft\)/);
assert.match(dashboardCss, /\.dashboard-auth-dialog form \{[\s\S]*margin: 0/);
assert.match(dashboardCss, /\.dashboard-auth-form label \{[\s\S]*margin: 0/);
assert.match(dashboardCss, /\.dashboard-auth-form input \{[\s\S]*margin: 0/);
assert.match(dashboardCss, /\.dashboard-auth-actions button \{[\s\S]*margin: 0/);
assert.match(html, /id="resourceCard"/);
assert.match(html, /id="compressionCard"/);
assert.match(html, /id="dbSection"/);
assert.match(html, /class="db-grid"/);
assert.match(html, /role="table" aria-label="Live database backup status"/);
assert.match(body, /el\.setAttribute\('role', 'row'\)/);
assert.match(html, /<main id="dashboard-main" class="app-content">/);
assert.match(html, /id="refreshInterval"/);
assert.match(html, /id="refreshState" class="refresh-state" role="status"/);
assert.match(body, /crab-dump\.refresh-interval-ms/);
assert.match(body, /function scheduleRefresh\(\)/);
assert.match(body, /function setRefreshInterval\(value\)/);
assert.match(body, /Refresh failed\. Showing last known data\./);
assert.match(html, /Operations \/ Overview/);
assert.match(body, /manualBackupLastFocus/);
assert.match(body, /event\.key !== 'Tab'/);
assert.match(usersHtml, /role="alertdialog"/);
assert.match(usersHtml, /@media\(min-width:901px\)\{\.page\{width:calc\(100% - 216px\)/);
assert.match(body, /config\.dashboard_role/);
assert.match(databasesHtml, /config\.dashboard_role/);
assert.match(routingHtml, /loadRole/);
assert.match(usersBody, /loadRole/);
assert.match(databasesHtml, /id="database-search"/);
assert.match(databasesHtml, /No matching databases/);
assert.match(databasesHtml, /No databases configured/);
assert.match(databasesHtml, /clear-database-search/);
assert.match(databasesHtml, /skeleton-line/);
assert.match(databasesHtml, /Databases could not be loaded/);
assert.match(databasesHtml, /retry-database-load/);
assert.match(databasesHtml, /<th scope="col">Connection<\/th>/);
assert.match(databasesHtml, /id="check-all"/);
assert.match(databasesHtml, /id="check-connections"/);
assert.match(databasesHtml, /id="select-all"/);
assert.match(databasesHtml, /data-select/);
assert.match(databasesHtml, /api\/database-connections\/check/);
assert.match(databasesHtml, /id="mutation-history"/);
assert.match(databasesHtml, /history-table/);
assert.doesNotMatch(databasesHtml, /<header class="head">[\s\S]*<a href="\/">Overview<\/a>/);
assert.match(databasesHtml, /id="database-table-scroll" aria-busy="false"/);
assert.match(databasesHtml, /database-table-scroll'\)\.setAttribute\('aria-busy','true'\)/);
assert.match(databasesHtml, /PostgreSQL credentials are never displayed/);
assert.match(databasesHtml, /Environment managed/);
assert.doesNotMatch(databasesHtml, /<td class="muted">\$\{esc\(db\.url\)\}<\/td>/);
assert.match(databasesHtml, /permission-notice/);
assert.match(databasesHtml, /id="state" class="muted" role="status" aria-live="polite"/);
assert.match(databasesHtml, /dashboardAuth\.request/);
assert.match(databasesHtml, /role="alert"/);
assert.match(databasesHtml, /save-database/);
assert.match(databasesHtml, /Saving…/);
assert.match(databasesHtml, /form\.setAttribute\('aria-busy','true'\)/);
console.log('shared operations shell and state-region contract OK');

// Service health workspace contract. The page is a split master/detail surface
// with zero page-local CSS, so these assertions protect the shared-shell
// migration, the poll loop, role gating, and the paginated incident table.
const servicesHtml = readFileSync(new URL(`${dashboardDir}services.html`, import.meta.url), 'utf8');
const servicesBody = servicesHtml.match(/<script>([\s\S]*?)<\/script>/)[1];

// Constructing (never calling) the page script is a cheap syntax gate: the
// dashboard has no build step, so a typo here would only surface in a browser.
assert.doesNotThrow(() => new Function('document', 'window', 'localStorage', 'confirm', servicesBody),
    'services.html inline script must parse');

// Fully design-system driven, like routing.html: no <style> block at all.
assert.doesNotMatch(servicesHtml, /<style/, 'services.html must not ship page-local CSS');
assert.match(servicesHtml, /<main id="services-main" class="page">/);
assert.match(servicesHtml, /<header class="page-header">[\s\S]*class="eyebrow"[\s\S]*<h1>Services<\/h1>[\s\S]*class="lede"/);
assert.match(servicesHtml, /class="svc-layout"/);
assert.match(servicesHtml, /id="svc-list" aria-label="Monitored services" aria-busy="true"/);
assert.match(servicesHtml, /id="svc-detail"/);
assert.doesNotMatch(servicesHtml, /id="service-grid"/, 'the flat card grid is retired');

// The `.service-*` namespace belongs to index.html's Telegram/dump status
// cards; this page must not collide with it in shared CSS.
assert.doesNotMatch(servicesHtml, /class="service-card"|class="service-grid"|class="service-meta"/);

// Endpoints and payload fields.
assert.match(servicesBody, /api\/services/);
assert.match(servicesBody, /api\/service-incidents/);
assert.match(servicesBody, /dashboardAuth\.request/);
assert.match(servicesHtml, /failure_threshold/);
assert.match(servicesHtml, /version_header/);
assert.match(servicesBody, /data-action/);
assert.match(servicesBody, /method: editing \? 'PUT' : 'POST'/);
assert.match(servicesBody, /method: 'DELETE'/);

// Runtime fields the old page fetched but never rendered.
assert.match(servicesBody, /last_reason/);
assert.match(servicesBody, /last_status_code/);
assert.match(servicesBody, /last_success/);
assert.match(servicesBody, /last_failure/);
assert.match(servicesBody, /last_observed_version/);
assert.match(servicesBody, /d\.recipients/);
assert.match(servicesBody, /definition\.enabled/);
assert.match(servicesBody, /NOT POLLED/);

// Recipients are picked from the Telegram user directory, never typed as raw
// chat IDs, and an already-saved recipient must survive an edit even once the
// directory no longer lists it as enabled.
assert.doesNotMatch(servicesHtml, /<input id="service-recipients"/,
    'recipients must be a directory picker, not a free-text chat ID field');
assert.match(servicesHtml, /<div class="svc-recipients" id="service-recipients"/);
assert.match(servicesHtml, /<legend>Alert recipients<\/legend>/);
assert.match(servicesBody, /call\('\/api\/telegram-users'\)/);
assert.match(servicesBody, /function loadDirectory\(\)/);
assert.match(servicesBody, /function recipientOptions\(\)/);
assert.match(servicesBody, /function renderRecipientPicker\(\)/);
assert.match(servicesBody, /recipientPicks = new Set\(\(d\?\.recipients \|\| \[\]\)\.map\(String\)\)/);
assert.match(servicesBody, /recipients: \[\.\.\.recipientPicks\]/);
assert.match(servicesBody, /filter\(user => user\.enabled\)/);
assert.match(servicesBody, /NOT IN DIRECTORY/);
assert.match(servicesBody, /input\[data-recipient\]/);
assert.match(servicesBody, /Directory unavailable:/);
assert.match(servicesBody, /id="retry-directory"/);

// Live refresh: interval control, persistence, pause when hidden, and a poll
// failure that keeps the last good render instead of blanking the panels.
assert.match(servicesHtml, /id="refreshInterval"/);
assert.match(servicesHtml, /id="refresh-now"/);
assert.match(servicesBody, /crab-dump\.services-refresh-ms/);
assert.match(servicesBody, /function scheduleRefresh\(\)/);
assert.match(servicesBody, /function setRefreshInterval\(value\)/);
assert.match(servicesBody, /Refresh failed\. Showing last known data\./);
assert.match(servicesBody, /document\.hidden/);
assert.match(servicesBody, /visibilitychange/);

// Latency trend is a client-side ring buffer, deduplicated on last_check so a
// poll faster than interval_secs cannot inflate the series.
assert.match(servicesBody, /function recordLatency\(entry\)/);
assert.match(servicesBody, /samples\[samples\.length - 1\]\.at === at/);
assert.match(servicesBody, /SPARK_SAMPLES/);
assert.match(servicesBody, /<polyline points=/);

// Incident pagination: the backend has always supported ?page=, the old page
// silently capped at the first 20 records.
assert.match(servicesBody, /page=\$\{page\}&page_size=\$\{INCIDENT_PAGE_SIZE\}/);
assert.match(servicesBody, /history-page-button history-prev/);
assert.match(servicesBody, /history-page-button history-next/);
assert.match(servicesBody, /total_records/);
assert.match(servicesBody, /id="incident-prev"/);
assert.match(servicesBody, /id="incident-next"/);

// Role gating mirrors the backend: Admin manages services, Operator may
// acknowledge incidents, Viewer is read-only.
assert.match(servicesBody, /loadRole/);
assert.match(servicesBody, /dashboard_role/);
assert.match(servicesBody, /canAdmin = \(\) => dashboardRole === 'admin'/);
assert.match(servicesBody, /canOperate = \(\) => canAdmin\(\) \|\| dashboardRole === 'operator'/);
assert.match(servicesBody, /canOperate\(\)\s*\?/);
assert.match(servicesHtml, /Viewer access is read-only/);
assert.match(servicesBody, /add-service'\)\.hidden = !canAdmin\(\)/);

// Region states and the shared editor skin.
assert.match(servicesHtml, /<dialog class="dialog" id="service-dialog"/);
assert.match(servicesHtml, /id="form-summary" role="alert" hidden/);
assert.match(servicesHtml, /class="skeleton-line"/);
assert.match(servicesBody, /No services monitored/);
assert.match(servicesBody, /Services could not be loaded/);
assert.match(servicesBody, /No incidents recorded/);
assert.match(servicesBody, /Incidents could not be loaded/);
assert.match(servicesBody, /No enabled users in the directory/);
assert.match(dashboardCss, /\.svc-recipient-list \{/);
assert.match(dashboardCss, /\.svc-recipients-field \{/);
console.log('services workspace contract OK');
