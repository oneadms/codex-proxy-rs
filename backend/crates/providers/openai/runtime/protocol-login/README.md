# 协议登录脚本运行时

OpenAI Provider 通过受管 Node 进程启动 Camoufox，每个账号登录独占浏览器和指定节点代理。登录接口在对应 origin 使用原生 fetch，实时 Sentinel SDK 在同一浏览器执行，网络不回退到 Node HTTP 或部署默认出口

`browser-runtime.generated.gz` 包含 Provider 脚本，随 Rust 二进制交付。Node.js 24 或更高版本、npm 依赖及固定版本 Camoufox 浏览器需安装到 Host 的 `module_directory`；安装和源码验证在此目录执行：

```sh
npm ci
npm run fetch
npm run build
npm test
```

Linux 还需 `npx playwright-core install-deps firefox` 所列系统库，容器构建负责安装。登录阶段禁止自动下载浏览器或扩展，不进行外部 GeoIP 查询

设备配置保存实际生效的 Linux 硬件、屏幕和指纹种子，复用设备时 UA 跟随浏览器版本，时区取账号节点。网页登录状态仅存在于本次临时浏览器配置，关闭或取消时清理。`npm test` 使用真实浏览器与本机隔离 TLS 代理，覆盖设备复用、Cookie 隔离、SDK、回调及代理拒绝
