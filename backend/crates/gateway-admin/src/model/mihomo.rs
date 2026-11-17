//! Mihomo 订阅、稳定节点入口及脱敏管理投影

use chrono::{DateTime, Utc};
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::Value;

use super::{
    PageSize, Revision,
    proxies::{DetectedProxyLocation, ProxyTestResult},
};

#[derive(Debug, Clone)]
pub struct MihomoSubscription {
    pub id: String,
    pub name: String,
    pub url: SecretString,
    pub revision: Revision,
    pub node_count: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewMihomoSubscription {
    pub name: String,
    pub url: SecretString,
}

#[derive(Debug, Clone)]
pub struct UpdateMihomoSubscription {
    pub id: String,
    pub revision: Revision,
    pub name: Option<String>,
    pub url: Option<SecretString>,
}

#[derive(Clone, PartialEq)]
pub struct MihomoNodeConfig {
    pub name: String,
    pub kind: String,
    pub config: Value,
}

impl std::fmt::Debug for MihomoNodeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MihomoNodeConfig")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("config", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct MihomoRuntimeNode {
    pub id: String,
    pub port: u16,
    pub config: Value,
}

#[derive(Debug, Clone)]
pub struct MihomoRuntimeSnapshot {
    pub config_revision: Revision,
    pub nodes: Vec<MihomoRuntimeNode>,
}

impl std::fmt::Debug for MihomoRuntimeNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MihomoRuntimeNode")
            .field("id", &self.id)
            .field("port", &self.port)
            .field("config", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct MihomoNode {
    pub id: String,
    pub proxy_id: String,
    pub name: String,
    pub kind: String,
    pub available: bool,
    pub account_count: u64,
    pub endpoint: String,
    pub proxy_revision: Revision,
    pub auto_location: bool,
    pub location: Option<gateway_core::account::RequestLocation>,
    pub detected_location: Option<DetectedProxyLocation>,
    pub last_test: Option<ProxyTestResult>,
    pub last_test_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MihomoNodeSort {
    #[default]
    Name,
    LatencyAsc,
    LatencyDesc,
}

#[derive(Debug, Clone)]
pub struct MihomoNodeQuery {
    pub subscription_id: String,
    pub page: u32,
    pub page_size: PageSize,
    pub search: String,
    pub sort: MihomoNodeSort,
}

#[derive(Debug, Clone)]
pub struct MihomoNodePage {
    pub items: Vec<MihomoNode>,
    pub total: u64,
    pub page: u32,
    pub page_size: u16,
}

#[derive(Debug, Clone)]
pub struct MihomoRefreshResult {
    pub subscription: MihomoSubscription,
    pub config_revision: Revision,
}
