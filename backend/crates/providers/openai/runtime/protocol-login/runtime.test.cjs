// 真实 Camoufox 使用隔离 TLS 代理验证设备复用、原生请求、SDK 和禁止直连
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createPrivateKey, X509Certificate } = require('node:crypto');
const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
const net = require('node:net');
const path = require('node:path');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
const os = require('node:os');
const readline = require('node:readline');
const { gunzipSync } = require('node:zlib');

process.env.CAMOUFOX_INSTALL_DIR = path.join(__dirname, 'camoufox');
process.env.PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = '1';
const { firefox } = require('playwright-core');
const { BrowserSession, browserOptions } = require('./browser-runtime.cjs');
const fixtures = path.join(__dirname, '../../tests/admin/protocol_login');

async function proxy(t) {
  const requests = [];
  const sockets = new Set();
  const key = createPrivateKey({ key: fs.readFileSync(path.join(fixtures, 'key.der')), format: 'der', type: 'pkcs8' }).export({ format: 'pem', type: 'pkcs8' });
  const cert = new X509Certificate(fs.readFileSync(path.join(fixtures, 'cert.der'))).toString();
  const origin = https.createServer({ key, cert }, async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    requests.push({ url: request.url, headers: request.headers, body });
    const common = { 'access-control-allow-origin': 'https://auth.openai.com', 'access-control-allow-credentials': 'true' };
    if (request.url.includes('sdk.js')) {
      response.writeHead(200, { ...common, 'content-type': 'application/javascript' });
      const name = request.url.includes('/sentinel/synthetic/') ? 'sentinel-sdk.js' : 'sentinel-loader.js';
      response.end(fs.readFileSync(path.join(fixtures, name)));
    } else if (request.url === '/backend-api/sentinel/req') {
      response.writeHead(200, { ...common, 'content-type': 'application/json' });
      response.end(JSON.stringify({ token: 'synthetic-challenge', so: { required: true } }));
    } else if (request.url === '/echo') {
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ body, cookie: request.headers.cookie, agent: request.headers['user-agent'], referrer: request.headers.referer }));
    } else if (request.url === '/oauth/authorize') {
      response.writeHead(302, { location: 'http://127.0.0.1:1455/auth/callback?code=synthetic&state=synthetic-state' });
      response.end();
    } else {
      response.writeHead(200, { 'content-type': 'text/html' });
      response.end('<!doctype html><html><head><title>Isolated login fixture</title></head><body></body></html>');
    }
  });
  const server = http.createServer();
  server.on('connection', socket => { sockets.add(socket); socket.on('close', () => sockets.delete(socket)); });
  server.on('connect', (request, socket) => {
    const host = request.url.split(':')[0];
    if (!['chatgpt.com', 'auth.openai.com', 'sentinel.openai.com'].includes(host)) return socket.destroy();
    socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
    origin.emit('connection', socket);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { for (const socket of sockets) socket.destroy(); server.close(); origin.close(); });
  return { url: `http://127.0.0.1:${server.address().port}`, requests };
}

async function launch(t, proxy, device) {
  const result = await browserOptions({ proxy, timezone: 'America/Chicago', device });
  const browser = await firefox.launch(result.options);
  t.after(() => browser.close());
  // 只有隔离夹具信任自签名测试证书，生产运行时始终验证上游证书
  const context = await browser.newContext({ viewport: null, serviceWorkers: 'block', ignoreHTTPSErrors: true });
  const session = new BrowserSession(context, result.device);
  await session.initialize();
  return { browser, context, session, device: result.device };
}

test('saved Linux device survives a fresh browser without retaining login cookies', { timeout: 120_000 }, async t => {
  const gateway = await proxy(t);
  const first = await launch(t, gateway.url);
  await first.session.navigate('https://chatgpt.com/');
  const page = first.context.pages()[0];
  const read = async () => ({
    platform: navigator.platform, agent: navigator.userAgent, width: screen.width,
    height: screen.height, cpus: navigator.hardwareConcurrency,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  });
  const before = await page.evaluate(`mw:(${read.toString()})()`);
  assert.match(before.agent, /Linux.*Firefox\//);
  assert.match(before.platform, /Linux/);
  assert.equal(before.timezone, 'America/Chicago');
  await first.context.addCookies([{ name: 'login-secret', value: 'synthetic-only', domain: 'chatgpt.com', path: '/' }]);
  const response = await first.session.request({ url: 'https://chatgpt.com/echo', method: 'POST', headers: { 'content-type': 'application/json', referer: 'https://chatgpt.com/account/login' }, body: '{"fixture":true}' });
  assert.equal(response.status, 200);
  const echoed = JSON.parse(response.body);
  assert.match(echoed.cookie, new RegExp(`oai-did=${first.device.id}`));
  assert.equal(echoed.agent, before.agent);
  assert.equal(echoed.body, '{"fixture":true}');
  assert.equal(echoed.referrer, 'https://chatgpt.com/account/login');
  await first.browser.close();
  const second = await launch(t, gateway.url, first.device);
  await second.session.navigate('https://chatgpt.com/');
  const after = await second.context.pages()[0].evaluate(`mw:(${read.toString()})()`);
  assert.deepEqual(after, before);
  assert.deepEqual(second.device, first.device);
  const cookies = await second.context.cookies();
  assert.equal(cookies.some(cookie => cookie.name === 'login-secret'), false);
  const replacement = await browserOptions({ proxy: gateway.url, timezone: 'America/Chicago' });
  assert.notEqual(replacement.device.id, first.device.id);
  assert.notDeepEqual(replacement.device.config, first.device.config);
});

test('official SDK executes in the browser and OAuth callback never reaches loopback', { timeout: 120_000 }, async t => {
  const gateway = await proxy(t);
  const login = await launch(t, gateway.url);
  await login.session.navigate('https://auth.openai.com/');
  await login.session.navigate('https://auth.openai.com/log-in/password');
  assert.equal(await login.context.pages()[0].url(), 'https://auth.openai.com/log-in/password');
  const tokens = await login.session.sentinel();
  assert.equal(JSON.parse(tokens.token).id, login.device.id);
  assert.equal(JSON.parse(tokens.sessionObserver).id, login.device.id);
  assert.ok(gateway.requests.some(request => request.url === '/backend-api/sentinel/req'));
  const result = await login.session.navigate('https://auth.openai.com/oauth/authorize');
  assert.equal(result.status, 302);
  assert.equal(result.headers.location, 'http://127.0.0.1:1455/auth/callback?code=synthetic&state=synthetic-state');
  assert.equal(gateway.requests.some(request => request.url.startsWith('/auth/callback')), false);
  await assert.rejects(login.session.navigate('https://example.invalid/blocked'), /untrusted_url/);
});

test('failed proxy cannot fall back to a direct connection', { timeout: 120_000 }, async t => {
  const unused = net.createServer().listen(0, '127.0.0.1');
  await once(unused, 'listening');
  const port = unused.address().port;
  await new Promise(resolve => unused.close(resolve));
  const login = await launch(t, `http://127.0.0.1:${port}`);
  await assert.rejects(login.session.navigate('https://chatgpt.com/'));
});

test('closing the managed input reclaims the browser and its temporary profile', { timeout: 120_000 }, async t => {
  const gateway = await proxy(t);
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'cpr-browser-cleanup-'));
  const bootstrap = "const lines=require('readline').createInterface({input:process.stdin});lines.once('line',source=>{const module={exports:{}};new Function('require','module','exports','__dirname',JSON.parse(source))(require,module,module.exports,process.cwd());module.exports(lines);});";
  const child = spawn(process.execPath, ['--eval', bootstrap], {
    cwd: directory, windowsHide: true, stdio: ['pipe', 'pipe', 'ignore'],
    env: {
      TZ: 'America/Chicago', NODE_PATH: path.join(__dirname, 'node_modules'),
      CAMOUFOX_INSTALL_DIR: path.join(__dirname, 'camoufox'), PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD: '1',
      TEMP: directory, TMP: directory, TMPDIR: directory,
      ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}),
    },
  });
  const exited = once(child, 'exit');
  t.after(async () => {
    if (child.exitCode === null) child.kill();
    await exited;
    const scratch = path.join(directory, 'mozilla-temp-files');
    if (fs.existsSync(scratch)) await fs.promises.rmdir(scratch);
    await fs.promises.rmdir(directory);
  });
  const output = readline.createInterface({ input: child.stdout });
  const ready = once(output, 'line');
  // 仅在隔离夹具中信任测试证书，直接执行随二进制交付的压缩资源
  const trustedFixture = `const fixtureBrowserType=require('playwright-core').firefox;
    const fixtureLaunch=fixtureBrowserType.launch.bind(fixtureBrowserType);
    fixtureBrowserType.launch=async options=>{
      const browser=await fixtureLaunch(options);
      const context=browser.newContext.bind(browser);
      browser.newContext=options=>context({...options,ignoreHTTPSErrors:true});
      return browser;
    };`;
  const program = trustedFixture + gunzipSync(fs.readFileSync(path.join(__dirname, 'browser-runtime.generated.gz'))).toString('utf8');
  child.stdin.write(`${JSON.stringify(program)}\n`);
  child.stdin.write(`${JSON.stringify({ type: 'init', proxy: gateway.url, timezone: 'America/Chicago' })}\n`);
  const initialized = JSON.parse((await ready)[0]);
  assert.equal(initialized.type, 'ready');
  const navigation = once(output, 'line');
  child.stdin.write(`${JSON.stringify({ type: 'navigate', url: 'https://auth.openai.com/log-in/password' })}\n`);
  assert.equal(JSON.parse((await navigation)[0]).status, 200);
  const verification = once(output, 'line');
  child.stdin.write(`${JSON.stringify({ type: 'sentinel' })}\n`);
  assert.equal(JSON.parse(JSON.parse((await verification)[0]).token).id, initialized.device.id);
  child.stdin.end();
  assert.equal((await exited)[0], 0);
  assert.equal(fs.readdirSync(directory).some(name => name.startsWith('playwright_')), false);
});
