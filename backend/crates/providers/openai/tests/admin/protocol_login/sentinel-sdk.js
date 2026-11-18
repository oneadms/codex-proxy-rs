// 通过浏览器原生请求读取虚构服务响应，覆盖令牌与观察器两条路径
(() => {
  let requirements;
  const deviceId = () => /(?:^|;\s*)oai-did=([^;]+)/.exec(document.cookie)?.[1];
  window.SentinelSDK = {
    async token(flow) {
      const response = await fetch('https://sentinel.openai.com/backend-api/sentinel/req', {
        method: 'POST', headers: { 'content-type': 'text/plain;charset=UTF-8' },
        body: JSON.stringify({ id: deviceId(), flow, p: 'synthetic-requirements' }),
      });
      requirements = await response.json();
      if (requirements.turnstile?.required) throw new Error('synthetic-security-challenge');
      return JSON.stringify({ p: 'synthetic-proof', t: null, c: requirements.token, id: deviceId(), flow });
    },
    async sessionObserverToken(flow) {
      return requirements.so?.required
        ? JSON.stringify({ so: 'synthetic-observer', c: requirements.token, id: deviceId(), flow })
        : null;
    },
  };
})();
