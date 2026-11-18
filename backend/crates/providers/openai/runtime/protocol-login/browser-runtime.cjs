// 登录接口和 Sentinel 使用同一浏览器网络栈，设备参数与临时会话分别管理
const { randomUUID } = require('node:crypto');
const { launchOptions } = require('camoufox-js');
const { installedVerStr } = require('camoufox-js/dist/pkgman.js');
const { firefox } = require('playwright-core');

const LOGIN_HOSTS = new Set(['chatgpt.com', 'auth.openai.com', 'sentinel.openai.com']);
const PAGE_HOSTS = new Set([...LOGIN_HOSTS, 'cdn.oaistatic.com', 'challenges.cloudflare.com']);
const MAX_BODY_BYTES = 4 * 1024 * 1024;

function loginUrl(value) {
  const url = new URL(value);
  if (url.protocol !== 'https:' || !LOGIN_HOSTS.has(url.hostname)
    || url.username || url.password || (url.port && url.port !== '443'))
    throw new Error('untrusted_url');
  return url;
}

function proxyOptions(value) {
  const url = new URL(value);
  if (!['http:', 'https:', 'socks5:', 'socks5h:'].includes(url.protocol))
    throw new Error('invalid_proxy');
  return {
    server: `${url.protocol.replace('socks5h:', 'socks5:')}//${url.host}`,
    ...(url.username ? { username: decodeURIComponent(url.username) } : {}),
    ...(url.password ? { password: decodeURIComponent(url.password) } : {}),
  };
}

function configFromEnvironment(env) {
  return JSON.parse(Object.keys(env).filter(key => /^CAMOU_CONFIG_\d+$/.test(key))
    .sort((a, b) => Number(a.slice(13)) - Number(b.slice(13)))
    .map(key => env[key]).join(''));
}

function evaluate(page, fn, input) {
  return page.evaluate(`mw:(${fn.toString()})(${JSON.stringify(input) ?? ''})`);
}

async function browserOptions(input) {
  const saved = input.device;
  const config = saved ? structuredClone(saved.config) : {};
  // 只更新版本数字，保留设备原有的发行版及架构标记
  const version = installedVerStr().split('.')[0];
  for (const key of ['navigator.userAgent', 'navigator.appVersion']) {
    if (config[key]) config[key] = config[key].replace(/(rv:|Firefox\/)\d+(?:\.\d+)*/g, `$1${version}.0`);
  }
  config.timezone = input.timezone;
  const options = await launchOptions({
    os: 'linux', headless: true, block_webrtc: true,
    exclude_addons: ['UBO'], geoip: false, locale: 'en-US',
    main_world_eval: true, config, proxy: proxyOptions(input.proxy),
    firefox_user_prefs: {
      ...(saved?.webgl2 === undefined ? {} : { 'webgl.enable-webgl2': saved.webgl2 }),
      'network.proxy.failover_direct': false,
      'network.proxy.no_proxies_on': '',
      'network.proxy.allow_hijacking_localhost': true,
      'network.trr.mode': 5,
      'network.dns.disablePrefetch': true,
      'network.prefetch-next': false,
      'network.http.speculative-parallel-limit': 0,
    },
    i_know_what_im_doing: true,
  });
  const resolved = configFromEnvironment(options.env);
  if (!resolved['navigator.userAgent']?.includes('Linux')
    || !resolved['navigator.userAgent']?.includes('Firefox/'))
    throw new Error('invalid_device');
  return {
    options,
    device: { id: saved?.id || randomUUID(), createdAt: saved?.createdAt ?? Date.now(), config: resolved, webgl2: options.firefoxUserPrefs['webgl.enable-webgl2'] },
  };
}

class BrowserSession {
  constructor(context, device) {
    this.context = context;
    this.device = device;
    this.pages = new Map();
  }

  async initialize() {
    await this.context.addCookies(['chatgpt.com', 'auth.openai.com'].map(domain => ({
      name: 'oai-did', value: this.device.id, domain, path: '/', secure: true, sameSite: 'Lax',
    })));
    await this.context.route('**/*', async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      if (url.protocol !== 'https:' || !PAGE_HOSTS.has(url.hostname)
        || url.username || url.password || (url.port && url.port !== '443'))
        return route.abort();
      return route.continue();
    });
  }

  async pageFor(url) {
    let page = this.pages.get(url.origin);
    if (!page) {
      page = await this.context.newPage();
      page.setDefaultTimeout(60_000);
      this.pages.set(url.origin, page);
    }
    if (new URL(page.url()).origin !== url.origin) {
      // 仅建立同源执行文档，登录 HTTP 请求仍由下方原生 fetch 发出
      await page.route(url.href, route => route.fulfill({
        status: 200, contentType: 'text/html',
        body: '<!doctype html><html><head></head><body></body></html>',
      }), { times: 1 });
      await page.goto(url.href, { waitUntil: 'domcontentloaded', timeout: 60_000 });
    }
    return page;
  }

  async navigate(value) {
    const url = loginUrl(value);
    const page = await this.pageFor(url);
    await evaluate(page, href => history.replaceState(null, '', href), url.href);
    return this.request({ url: value, method: 'GET', headers: {
      accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }, body: '' });
  }

  async request(input) {
    const url = loginUrl(input.url);
    const page = await this.pageFor(url);
    const headers = { ...input.headers };
    const referrer = headers.referer;
    if (referrer && loginUrl(referrer).origin !== url.origin)
      throw new Error('untrusted_url');
    for (const name of Object.keys(headers)) {
      if (/^(?:sec-|origin$|referer$|user-agent$|accept-language$|content-length$|host$|cookie$)/i.test(name))
        delete headers[name];
    }
    const observed = page.waitForResponse(response => response.url() === url.href
      && response.request().method() === input.method, { timeout: 60_000 });
    const fetching = evaluate(page, async ({ url, method, headers, body, maximum, referrer }) => {
      const response = await fetch(url, {
        method, headers, body: ['GET', 'HEAD'].includes(method) ? undefined : body,
        credentials: 'include', redirect: 'manual', signal: AbortSignal.timeout(60_000),
        ...(referrer ? { referrer } : {}),
      });
      const reader = response.body?.getReader();
      const chunks = [];
      let bytes = 0;
      if (reader) {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          bytes += value.byteLength;
          if (bytes > maximum) { await reader.cancel(); throw new Error('response_limit'); }
          chunks.push(value);
        }
      }
      const buffer = new Uint8Array(bytes);
      let offset = 0;
      for (const chunk of chunks) { buffer.set(chunk, offset); offset += chunk.byteLength; }
      return {
        status: response.status, url: response.url,
        headers: Object.fromEntries(response.headers.entries()), body: new TextDecoder().decode(buffer),
      };
    }, { url: url.href, method: input.method, headers, body: input.body, maximum: MAX_BODY_BYTES, referrer });
    // fetch 的手动跳转返回 opaque response，真实状态和 Location 从浏览器网络事件读取
    const [response, result] = await Promise.all([observed, fetching]);
    return { ...result, status: response.status(), url: url.href, headers: await response.allHeaders() };
  }

  async sentinel() {
    const url = new URL('https://auth.openai.com/log-in/password');
    const page = await this.pageFor(url);
    // 页面及内嵌页运行实时官方 SDK，所有验证请求由浏览器自行发出
    return evaluate(page, async () => {
      if (!window.SentinelSDK) {
        await new Promise((resolve, reject) => {
          const script = document.createElement('script');
          script.src = 'https://sentinel.openai.com/backend-api/sentinel/sdk.js';
          script.onload = resolve;
          script.onerror = () => reject(new Error('sentinel_unavailable'));
          document.head.appendChild(script);
        });
      }
      const deadline = Date.now() + 15_000;
      while (!window.SentinelSDK?.token) {
        if (Date.now() > deadline) throw new Error('sentinel_unavailable');
        await new Promise(resolve => setTimeout(resolve, 25));
      }
      return {
        token: await window.SentinelSDK.token('password_verify'),
        sessionObserver: typeof window.SentinelSDK.sessionObserverToken === 'function'
          ? await window.SentinelSDK.sessionObserverToken('password_verify') : null,
      };
    });
  }
}

module.exports = function start(lines) {
  // 第三方启动信息可能包含路径或配置，标准输出只承载有界协议消息
  for (const method of ['log', 'info', 'debug', 'warn', 'error']) console[method] = () => {};
  let browser;
  let session;
  let closed = false;
  let pending = Promise.resolve();
  const emit = value => process.stdout.write(`${JSON.stringify(value)}\n`);
  const close = async () => {
    if (closed) return;
    closed = true;
    try { await browser?.close(); } catch (_) {}
    await pending.catch(() => {});
    try { await browser?.close(); } catch (_) {}
    process.exit(0);
  };
  lines.on('close', close);
  process.on('SIGTERM', close);
  process.on('SIGINT', close);
  lines.on('line', (line) => {
    pending = pending.then(async () => {
      const input = JSON.parse(line);
      if (closed) return;
      try {
        if (input.type === 'init') {
          const launch = await browserOptions(input);
          browser = await firefox.launch(launch.options);
          if (closed) { await browser.close(); return; }
          const context = await browser.newContext({ viewport: null, serviceWorkers: 'block' });
          session = new BrowserSession(context, launch.device);
          await session.initialize();
          emit({ type: 'ready', device: launch.device });
        } else if (input.type === 'navigate') {
          emit({ type: 'response', ...await session.navigate(input.url) });
        } else if (input.type === 'request') {
          emit({ type: 'response', ...await session.request(input) });
        } else if (input.type === 'sentinel') {
          emit({ type: 'sentinel', ...await session.sentinel() });
        } else if (input.type === 'shutdown') {
          setImmediate(close);
        } else {
          throw new Error('invalid_command');
        }
      } catch (error) {
        const safe = new Set(['invalid_proxy', 'invalid_device', 'untrusted_url', 'response_limit']);
        emit({ type: 'error', code: safe.has(error.message) ? error.message
          : input.type === 'init' ? 'browser_unavailable'
          : input.type === 'sentinel' ? 'sentinel_failed' : 'browser_request_failed' });
      }
    }).catch(() => { emit({ type: 'error', code: 'invalid_command' }); });
  });
};

module.exports.BrowserSession = BrowserSession;
module.exports.browserOptions = browserOptions;
