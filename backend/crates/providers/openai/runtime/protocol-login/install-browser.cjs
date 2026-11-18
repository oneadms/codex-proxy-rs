// 浏览器在安装阶段下载，账号登录时只使用本地已安装的固定版本
const path = require('node:path');

process.env.CAMOUFOX_INSTALL_DIR = path.join(__dirname, 'camoufox');
const { CamoufoxFetcher } = require('camoufox-js/dist/pkgman.js');
// 登录不使用扩展及外部 GeoIP，安装时也只获取库固定的浏览器构建
new CamoufoxFetcher().install().catch(error => {
  console.error(error.message);
  process.exitCode = 1;
});
