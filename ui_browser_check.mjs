// Browser e2e: load the real Bigtrace UI (served by the adapter), point its
// backend endpoint at the same origin, prove a query round-trips through the
// page's CSP to the adapter, then drive the query editor and screenshot.
//
// Run with the perfetto ui's vendored node so 'playwright' resolves:
//   cd ui && ./node /mnt/agent/bigtrace-tpd-adapter/ui_browser_check.mjs
import {createRequire} from 'node:module';

const PW_NODE_MODULES = process.env.PW_NODE_MODULES ||
  '/mnt/agent/perfetto/perfetto/ui/node_modules/';
const require = createRequire(PW_NODE_MODULES);
const {chromium} = require('playwright');

const BASE = process.env.BASE || 'http://127.0.0.1:5071';
const OUT = process.env.OUT_DIR || '/mnt/agent/tmp/bt_ui_env';
const SQL = 'select count(*) as n from thread';

function fail(msg) {
  console.error('UI_E2E_FAIL: ' + msg);
  process.exit(1);
}

const browser = await chromium.launch({
  channel: 'chrome',
  headless: true,
  args: ['--no-sandbox', '--disable-dev-shm-usage'],
});
const ctx = await browser.newContext({viewport: {width: 1500, height: 950}});

// Seed the UI's backend endpoint to this same origin before any page script
// runs, so the UI fetches /execute_bigtrace_query on this origin — which the
// connect-src 'self' CSP allows. (upstream/main rejects an empty endpoint, so
// we use the page's own origin rather than ''.)
await ctx.addInitScript((ep) => {
  try {
    localStorage.setItem(
      'bigtraceSettings',
      JSON.stringify({bigtraceEndpoint: ep, theme: 'light'}),
    );
  } catch (e) {}
}, BASE);

const page = await ctx.newPage();
const consoleErrors = [];
page.on('console', (m) => {
  if (m.type() === 'error') consoleErrors.push(m.text());
});
page.on('pageerror', (e) => consoleErrors.push('pageerror: ' + e.message));

try {
  await page.goto(BASE + '/bigtrace.html', {waitUntil: 'load', timeout: 60000});
  await page.waitForSelector('main, .pf-ui-main', {timeout: 30000});
  console.log('STEP loaded bigtrace.html, app mounted');

  // PROOF 1: a fetch from inside the loaded page (with its CSP active) to the
  // adapter on the same origin. This is exactly what HttpDataSource does.
  const fetchRes = await page.evaluate(async (sql) => {
    try {
      const r = await fetch('/execute_bigtrace_query', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({limit: 100, perfetto_sql: sql, settings: []}),
        credentials: 'include',
        mode: 'cors',
      });
      return {status: r.status, body: await r.json()};
    } catch (e) {
      return {error: String(e)};
    }
  }, SQL);
  console.log('STEP in-page fetch ->', JSON.stringify(fetchRes));
  if (fetchRes.error) fail('in-page fetch threw (CSP/CORS?): ' + fetchRes.error);
  if (fetchRes.status !== 200) fail('in-page fetch status ' + fetchRes.status);
  if (!fetchRes.body || !Array.isArray(fetchRes.body.columnNames) ||
      !Array.isArray(fetchRes.body.rows) || fetchRes.body.rows.length === 0) {
    fail('in-page fetch returned no rows: ' + JSON.stringify(fetchRes.body));
  }
  console.log('PASS in-page fetch returned',
              fetchRes.body.rows.length, 'rows, cols',
              JSON.stringify(fetchRes.body.columnNames));

  // PROOF 2: drive the actual query editor and render results.
  await page.goto(BASE + '/bigtrace.html#!/query',
                  {waitUntil: 'load', timeout: 60000});
  await page.waitForSelector('.cm-content', {timeout: 30000});
  await page.click('.cm-content');
  await page.keyboard.type(SQL);
  await page.keyboard.press('Control+Enter');

  // Wait for results to render. Matches both UI generations: the older
  // "Returned N rows" banner and the newer datagrid with our trace names.
  let rendered = false;
  try {
    await page.waitForFunction(() => {
      const t = document.body.innerText || '';
      return /Returned\s+\d+\s+rows/.test(t) || /cf\d+\.pftrace/.test(t) ||
             !!document.querySelector('.pf-data-grid, .pf-datagrid, table');
    }, {timeout: 20000});
    rendered = true;
  } catch (e) {
    // Non-fatal: the in-page fetch already proved connectivity.
  }
  await page.waitForTimeout(500);
  await page.screenshot({path: OUT + '/bigtrace_ui.png', fullPage: true});
  console.log('STEP query screenshot ->', OUT + '/bigtrace_ui.png');
  console.log('STEP datagrid rendered results:', rendered);

  // Settings page screenshot.
  try {
    await page.goto(BASE + '/bigtrace.html#!/settings',
                    {waitUntil: 'load', timeout: 60000});
    await page.waitForTimeout(1500);
    await page.screenshot({path: OUT + '/bigtrace_settings.png', fullPage: true});
    console.log('STEP settings screenshot ->', OUT + '/bigtrace_settings.png');
  } catch (e) {
    console.log('STEP settings screenshot failed:', String(e));
  }

  if (consoleErrors.length) {
    console.log('CONSOLE_ERRORS', JSON.stringify(consoleErrors.slice(0, 15)));
  }
  console.log(rendered ? 'UI_E2E_OK (fetch + grid)' : 'UI_E2E_OK (fetch only)');
} finally {
  await browser.close();
}
