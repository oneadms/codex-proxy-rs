// 构建随二进制内嵌的运行时，官方 SDK 与账号材料均在运行时通过内存通道传入
import { build } from 'esbuild';
import fs from 'node:fs/promises';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';

const directory = path.dirname(fileURLToPath(import.meta.url));
const output = await build({
  absWorkingDir: directory,
  entryPoints: ['sentinel-runtime.cjs'],
  bundle: true,
  platform: 'node',
  format: 'cjs',
  target: 'node24',
  minify: true,
  write: false,
  legalComments: 'inline',
  plugins: [{
    name: 'managed-network-only',
    setup(builder) {
      // SDK 仅使用受管 HTTP 转发，移除 jsdom 自带 XHR、同步 Worker 和直连 WebSocket
      builder.onLoad({ filter: /jsdom[\\/]lib[\\/]jsdom[\\/]living[\\/]generated[\\/](?:XMLHttpRequest|WebSocket)\.js$/ }, () => ({
        contents: 'exports.install = () => {};', loader: 'js',
      }));
    },
  }],
});
const lock = JSON.parse(await fs.readFile(path.join(directory, 'package-lock.json'), 'utf8'));
const notices = ['# JavaScript runtime licenses', await fs.readFile(path.join(directory, 'LICENSE.tosub2'), 'utf8')];
for (const [entry, metadata] of Object.entries(lock.packages).sort(([left], [right]) => left.localeCompare(right))) {
  if (!entry.startsWith('node_modules/') || metadata.dev) continue;
  const moduleDirectory = path.join(directory, entry);
  const manifest = JSON.parse(await fs.readFile(path.join(moduleDirectory, 'package.json'), 'utf8'));
  const files = (await fs.readdir(moduleDirectory)).filter(file => /^(licen[sc]e|copying)(?:[-_.].*)?$/i.test(file)).sort();
  notices.push(`## ${manifest.name}@${manifest.version}`);
  // saxes 6.0.0 的 npm 发布包缺少许可证，保留其对应源码 Tag 中的原文
  if (!files.length && manifest.name === 'saxes' && manifest.version === '6.0.0') {
    notices.push(await fs.readFile(path.join(directory, 'LICENSE.saxes'), 'utf8'));
    continue;
  }
  if (!files.length) throw new Error(`Missing license for ${manifest.name}`);
  for (const file of files) notices.push(await fs.readFile(path.join(moduleDirectory, file), 'utf8'));
}
const notice = notices.join('\n\n').trimEnd();
await fs.writeFile(path.join(directory, 'THIRD_PARTY_NOTICES.md'), `${notice}\n`);
// 版权说明同时随内嵌资源交付，二进制运行不依赖源码目录
const program = Buffer.concat([Buffer.from(`/*!\n${notice.replaceAll('*/', '* /')}\n*/\n`), output.outputFiles[0].contents]);
await fs.writeFile(path.join(directory, 'sentinel-runtime.generated.gz'), gzipSync(program, { level: 9 }));
