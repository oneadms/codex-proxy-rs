// 验证内嵌运行时的网络转发、页面通信与观察器，不连接真实上游
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const fs = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const readline = require('node:readline');
const { gunzipSync } = require('node:zlib');

const bootstrap = "const lines=require('readline').createInterface({input:process.stdin});lines.once('line',source=>{const module={exports:{}};new Function('require','module','exports','__dirname',JSON.parse(source))(require,module,module.exports,process.cwd());module.exports(lines);});";
const fixtureDirectory = path.join(__dirname, '../../tests/admin/protocol_login');

async function runtime(t, sdkSource) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'cpr-sentinel-'));
  const child = spawn(process.execPath, ['--eval', bootstrap], {
    cwd: directory, env: { TZ: 'Asia/Tokyo', ...(process.env.SystemRoot ? { SystemRoot: process.env.SystemRoot } : {}) },
    stdio: ['pipe', 'pipe', 'ignore'], windowsHide: true,
  });
  const exited = once(child, 'exit');
  const pending = [], messages = [];
  readline.createInterface({ input: child.stdout }).on('line', line => {
    const message = JSON.parse(line);
    if (pending.length) pending.shift()(message);
    else messages.push(message);
  });
  const send = async value => {
    if (!child.stdin.write(`${JSON.stringify(value)}\n`)) await once(child.stdin, 'drain');
  };
  const receive = () => messages.length ? Promise.resolve(messages.shift()) : new Promise(resolve => pending.push(resolve));
  t.after(async () => { child.kill(); await exited; await fs.rmdir(directory); });
  const program = gunzipSync(await fs.readFile(path.join(__dirname, 'sentinel-runtime.generated.gz'))).toString('utf8');
  await send(program);
  await send({
    url: 'https://auth.openai.com/log-in/password', userAgent: 'synthetic-browser',
    cookies: { 'oai-did': 'synthetic-device' },
    loaderUrl: 'https://sentinel.openai.com/backend-api/sentinel/sdk.js',
    loaderSource: await fs.readFile(path.join(fixtureDirectory, 'sentinel-loader.js'), 'utf8'),
    sdkUrl: 'https://sentinel.openai.com/sentinel/synthetic/sdk.js', sdkSource,
  });
  return { send, receive };
}

test('bundled runtime relays fetch and returns bound primary and observer tokens', { timeout: 10000 }, async t => {
  const sdk = await fs.readFile(path.join(fixtureDirectory, 'sentinel-sdk.js'), 'utf8');
  const session = await runtime(t, sdk);
  await session.send({ type: 'call', method: 'token', flow: 'password_verify' });
  const request = await session.receive();
  assert.equal(request.type, 'request');
  assert.equal(request.request.kind, 'fetch');
  assert.equal(JSON.parse(request.request.body).id, 'synthetic-device');
  await session.send({ id: request.request.id, status: 200, headers: {}, body: JSON.stringify({ token: 'synthetic-challenge', so: { required: true } }) });
  const result = await session.receive();
  assert.deepEqual(result.errors, []);
  const token = JSON.parse(result.value);
  assert.equal(token.id, 'synthetic-device');
  assert.equal(token.c, 'synthetic-challenge');
  await session.send({ type: 'call', method: 'sessionObserverToken', flow: 'password_verify' });
  const observer = JSON.parse((await session.receive()).value);
  assert.equal(observer.id, token.id);
  assert.equal(observer.c, token.c);
  assert.equal(observer.so, 'synthetic-observer');
});

test('iframe XHR uses the same relay and preserves its browser event realm', { timeout: 10000 }, async t => {
  const sdk = `(() => {
    if (window.top === window) {
      const frame = document.createElement('iframe');
      frame.src = 'https://sentinel.openai.com/backend-api/sentinel/frame.html';
      document.body.appendChild(frame);
      window.SentinelSDK = {
        token(flow) { return new Promise(resolve => {
          window.addEventListener('message', event => { if (event.data.type === 'synthetic-reply') resolve(event.data.value); });
          frame.addEventListener('load', () => frame.contentWindow.postMessage({ type: 'synthetic-call', flow }, '*'), { once: true });
        }); },
        async sessionObserverToken() { return null; },
      };
    } else {
      window.addEventListener('message', event => {
        if (event.data.type !== 'synthetic-call') return;
        const request = new XMLHttpRequest();
        request.open('POST', '/backend-api/sentinel/req');
        request.responseType = 'json';
        request.onload = () => window.parent.postMessage({ type: 'synthetic-reply', value: JSON.stringify({
          c: request.response.token, timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
        }) }, '*');
        request.send('synthetic-xhr-body');
      });
    }
  })();`;
  const session = await runtime(t, sdk);
  await session.send({ type: 'call', method: 'token', flow: 'password_verify' });
  assert.equal((await session.receive()).type, 'frame');
  const request = await session.receive();
  assert.equal(request.request.kind, 'xhr');
  assert.equal(new URL(request.request.sourceUrl).hostname, 'sentinel.openai.com');
  await session.send({ id: request.request.id, status: 200, headers: { 'content-type': 'application/json' }, body: '{"token":"synthetic-xhr-response"}' });
  const result = await session.receive();
  assert.deepEqual(result.errors, []);
  assert.deepEqual(JSON.parse(result.value), { c: 'synthetic-xhr-response', timezone: 'Asia/Tokyo' });
});

test('SDK fetch and WebSocket never open a direct connection', { timeout: 10000 }, async t => {
  let directConnections = 0;
  const trap = net.createServer(socket => { directConnections++; socket.destroy(); });
  await new Promise(resolve => trap.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => trap.close(resolve)));
  const target = `http://127.0.0.1:${trap.address().port}/no-direct`;
  const sdk = `window.SentinelSDK={async token(){
    let blocked=false;try{new WebSocket(${JSON.stringify(target.replace('http:', 'ws:'))});}catch(error){blocked=error.name==='SecurityError';}
    await fetch(${JSON.stringify(target)},{method:'POST'});return JSON.stringify({blocked});
  }};`;
  const session = await runtime(t, sdk);
  await session.send({ type: 'call', method: 'token', flow: 'password_verify' });
  const request = await session.receive();
  assert.equal(request.request.url, target);
  await session.send({ id: request.request.id, status: 200, body: '' });
  assert.deepEqual(JSON.parse((await session.receive()).value), { blocked: true });
  assert.equal(directConnections, 0);
});
