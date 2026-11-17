// 隔离服务的 SDK 加载器，仅使用虚构地址与测试令牌
const script = document.createElement('script');
script.src = 'https://sentinel.openai.com/sentinel/synthetic/sdk.js';
document.head.appendChild(script);
