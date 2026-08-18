// Self-check for the dashboard's state → timeline mapping.
// Run with: node test_dashboard.mjs
//
// It loads the <script> block out of index.html (the dashboard has no build
// step) with minimal document/window stubs, then asserts that the backend's
// (state, stage) pairs produce the expected badge and timeline classes.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('./index.html', import.meta.url), 'utf8');
const authJs = readFileSync(new URL('./dashboard-auth.js', import.meta.url), 'utf8');
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
const usersHtml = readFileSync(new URL('./users.html', import.meta.url), 'utf8');
const usersBody = usersHtml.match(/<script>([\s\S]*?)<\/script>/)[1];
const databasesHtml = readFileSync(new URL('./databases.html', import.meta.url), 'utf8');

assert.match(usersHtml, /<table>/);
assert.match(usersHtml, /class="table-scroll"/);
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

const routingHtml = readFileSync(new URL('./routing.html', import.meta.url), 'utf8');
assert.match(routingHtml, /disableRouting/);
assert.match(routingHtml, /api\/routing\/disable/);
assert.match(routingHtml, /ss:\/\/|trojan:\/\//);
assert.match(routingHtml, /Saved profiles will remain available/);
assert.match(routingHtml, /Check all configurations/);
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
assert.match(routingHtml, /Saved profiles are unavailable until routing state loads/);
assert.match(routingHtml, /profileSubmit/);
assert.match(routingHtml, /setMessage\('Saving profile…','busy'\)/);
assert.match(routingHtml, /dashboardRole==='admin'/);
assert.match(routingHtml, /routingPermission/);
assert.match(routingHtml, /canOperate=canAdmin\|\|dashboardRole==='operator'/);
assert.match(routingHtml, /Operator access can apply or disable routes/);
assert.match(routingHtml, /id="profiles" aria-label="Saved routing profiles" aria-busy="true"/);
assert.match(routingHtml, /profiles\.setAttribute\('aria-busy','false'\)/);
assert.match(routingHtml, /\(async\(\)=>\{await loadRole\(\);await loadRouting\(\)\}\)\(\)/);
assert.doesNotMatch(routingHtml, /Promise\.all\(\[loadRouting\(\),loadRole\(\)\]\)\.then\(\(\)=>refresh\(\)\)/);
assert.match(routingHtml, /routingStatus\.append\(routingRetry\).*routingRetry\.hidden=false/);
console.log('Routing disable workspace contract OK');

// Shared operations shell contract. Every dashboard surface must expose the
// same destinations so operators do not have to hunt for a return link.
const pageFiles = ['index.html', 'databases.html', 'routing.html', 'users.html'];
for (const file of pageFiles) {
    const source = readFileSync(new URL(`./${file}`, import.meta.url), 'utf8');
    assert.match(source, /<nav class="app-nav" aria-label="Primary navigation">/);
    assert.match(source, /class="skip-link"/);
    assert.match(source, /Skip to main content/);
    assert.match(source, /prefers-reduced-motion/);
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
    assert.match(source, /overflow-x:\s*hidden/);
    assert.match(source, /:focus-visible/);
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
const dashboardCss = readFileSync(new URL('./dashboard.css', import.meta.url), 'utf8');
assert.match(dashboardCss, /dashboard-auth-dialog/);
assert.match(dashboardCss, /var\(--dash-surface\)/);
assert.match(dashboardCss, /var\(--dash-border\)/);
assert.match(dashboardCss, /var\(--dash-radius-lg\)/);
assert.match(dashboardCss, /var\(--dash-z-dialog\)/);
assert.match(dashboardCss, /var\(--dash-shadow-dialog\)/);
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
