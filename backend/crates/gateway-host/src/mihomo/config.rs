//! Mihomo 可执行文件配置与相对路径解析

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::ConfigError;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct MihomoConfig {
    pub executable: PathBuf,
}

impl Default for MihomoConfig {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("mihomo"),
        }
    }
}

impl MihomoConfig {
    pub fn resolve_and_validate(&mut self, source_dir: &Path) -> Result<(), ConfigError> {
        if self.executable.as_os_str().is_empty() {
            return Err(ConfigError::InvalidField("mihomo.executable"));
        }
        // 裸名称由系统 PATH 查找，显式相对路径沿用配置文件的路径基准
        if self.executable.is_relative() && self.executable.components().count() > 1 {
            self.executable = source_dir.join(&self.executable);
        }
        Ok(())
    }
}
