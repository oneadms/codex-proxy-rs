# 协议登录脚本运行时

OpenAI Provider 实时下载官方 Sentinel SDK，将其交给内嵌的页面运行环境执行。Host 管理 Node 进程，fetch 和 XHR 通过消息通道交回 Provider 的固定代理客户端，WebSocket 不能直连

`sentinel-runtime.generated.gz` 包含运行时及其依赖，随 Rust 二进制交付。运行及执行资源测试只需 Node.js 24 或更高版本；修改源码或依赖后，在此目录执行：

```sh
npm ci
npm run build
npm test
```

构建同时生成第三方版权说明并嵌入资源。页面适配参考 toSub2 1.7.1，原始 MIT 许可证见 `LICENSE.tosub2`；SDK 源码、Cookie 和安全令牌均在运行时通过内存传递，不写入资源文件
