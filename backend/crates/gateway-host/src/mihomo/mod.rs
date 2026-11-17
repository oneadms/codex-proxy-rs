//! Mihomo 订阅读取与专属节点监听的宿主适配，不持有账号或配置数据库事实

mod config;
mod controller;
mod runtime;
mod subscription;

use std::fmt;

use serde_json::Value;

pub use config::MihomoConfig;
pub use runtime::MihomoHost;

pub const MAX_SUBSCRIPTION_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SUBSCRIPTION_NODES: usize = 1000;

#[derive(Clone, PartialEq)]
pub struct MihomoNodeConfig {
    pub name: String,
    pub kind: String,
    pub config: Value,
}

impl fmt::Debug for MihomoNodeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MihomoNodeConfig([REDACTED])")
    }
}

#[derive(Clone, PartialEq)]
pub struct MihomoRuntimeNode {
    pub id: String,
    pub port: u16,
    pub config: Value,
}

impl fmt::Debug for MihomoRuntimeNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MihomoRuntimeNode([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MihomoErrorKind {
    Invalid,
    Unavailable,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct MihomoError {
    kind: MihomoErrorKind,
    message: &'static str,
}

impl MihomoError {
    #[must_use]
    pub const fn kind(&self) -> MihomoErrorKind {
        self.kind
    }

    fn invalid(message: &'static str) -> Self {
        Self {
            kind: MihomoErrorKind::Invalid,
            message,
        }
    }

    fn unavailable(message: &'static str) -> Self {
        Self {
            kind: MihomoErrorKind::Unavailable,
            message,
        }
    }
}
