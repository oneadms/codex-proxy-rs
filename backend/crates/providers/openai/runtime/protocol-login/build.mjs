// 构建随二进制内嵌的运行时，官方 SDK 与账号材料均在运行时通过内存通道传入
import { build } from 'esbuild';
import fs from 'node:fs/promises';
import path from 'node:path';
import { gzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';

const directory = path.dirname(fileURLToPath(import.meta.url));
const output = await build({
  absWorkingDir: directory,
  entryPoints: ['browser-runtime.cjs'],
  bundle: true,
  platform: 'node',
  format: 'cjs',
  target: 'node24',
  minify: true,
  write: false,
  legalComments: 'inline',
  packages: 'external',
});
const lock = JSON.parse(await fs.readFile(path.join(directory, 'package-lock.json'), 'utf8'));
const notices = ['# JavaScript runtime licenses'];
for (const [entry, metadata] of Object.entries(lock.packages).sort(([left], [right]) => left.localeCompare(right))) {
  if (!entry.startsWith('node_modules/') || metadata.dev) continue;
  const moduleDirectory = path.join(directory, entry);
  const manifest = await fs.readFile(path.join(moduleDirectory, 'package.json'), 'utf8')
    .then(JSON.parse).catch(error => {
      if (!metadata.optional || error.code !== 'ENOENT') throw error;
      return { name: entry.split('node_modules/').at(-1), version: metadata.version, license: metadata.license };
    });
  const files = (await fs.readdir(moduleDirectory).catch(error => {
    if (!metadata.optional || error.code !== 'ENOENT') throw error;
    return [];
  })).filter(file => /^(licen[sc]e|copying)(?:[-_.].*)?$/i.test(file)).sort();
  notices.push(`## ${manifest.name}@${manifest.version}`);
  // 外部依赖不打包进脚本，未附许可证文件的 npm 包保留其 SPDX 声明和源码地址
  if (!files.length) {
    if (!manifest.license) throw new Error(`Missing license for ${manifest.name}`);
    notices.push(`License: ${manifest.license}\nSource: ${manifest.repository?.url || manifest.homepage || ''}`);
  }
  for (const file of files) notices.push(await fs.readFile(path.join(moduleDirectory, file), 'utf8'));
}
const notice = notices.join('\n\n').split(/\r?\n/).map(line => line.trimEnd()).join('\n').trimEnd();
await fs.writeFile(path.join(directory, 'THIRD_PARTY_NOTICES.md'), `${notice}\n`);
// 版权说明同时随内嵌资源交付，二进制运行不依赖源码目录
const program = Buffer.concat([Buffer.from(`/*!\n${notice.replaceAll('*/', '* /')}\n*/\n`), output.outputFiles[0].contents]);
await fs.writeFile(path.join(directory, 'browser-runtime.generated.gz'), gzipSync(program, { level: 9 }));
