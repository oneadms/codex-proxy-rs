//! 将 Host Mihomo 运行时适配到 Admin 管理端口

use std::sync::Arc;

use async_trait::async_trait;
use gateway_admin::{
    model::{AdminError, AdminErrorKind, mihomo::MihomoNodeConfig},
    ports::mihomo::{MihomoRuntime, MihomoSubscriptionFetcher},
};
use gateway_host::mihomo::{MihomoError, MihomoErrorKind, MihomoHost, MihomoRuntimeNode};

pub(super) struct AdminMihomoAdapter {
    host: Arc<MihomoHost>,
}

impl AdminMihomoAdapter {
    pub(super) fn new(host: Arc<MihomoHost>) -> Self {
        Self { host }
    }
}

#[async_trait]
impl MihomoRuntime for AdminMihomoAdapter {
    async fn validate(&self, nodes: &[MihomoNodeConfig]) -> Result<(), AdminError> {
        self.host
            .validate_nodes(
                &nodes
                    .iter()
                    .map(|node| gateway_host::mihomo::MihomoNodeConfig {
                        name: node.name.clone(),
                        kind: node.kind.clone(),
                        config: node.config.clone(),
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(map_error)
    }

    async fn apply(
        &self,
        nodes: Vec<gateway_admin::model::mihomo::MihomoRuntimeNode>,
    ) -> Result<(), AdminError> {
        self.host
            .apply(nodes.into_iter().map(to_host_runtime_node).collect())
            .await
            .map_err(map_error)
    }

    async fn shutdown(&self) {
        self.host.shutdown().await;
    }
}

#[async_trait]
impl MihomoSubscriptionFetcher for AdminMihomoAdapter {
    async fn fetch(&self, url: &str) -> Result<Vec<MihomoNodeConfig>, AdminError> {
        self.host
            .fetch(url)
            .await
            .map(|nodes| {
                nodes
                    .into_iter()
                    .map(|node| MihomoNodeConfig {
                        name: node.name,
                        kind: node.kind,
                        config: node.config,
                    })
                    .collect()
            })
            .map_err(map_error)
    }
}

fn to_host_runtime_node(
    node: gateway_admin::model::mihomo::MihomoRuntimeNode,
) -> MihomoRuntimeNode {
    MihomoRuntimeNode {
        id: node.id,
        port: node.port,
        config: node.config,
    }
}

fn map_error(error: MihomoError) -> AdminError {
    let kind = match error.kind() {
        MihomoErrorKind::Invalid => AdminErrorKind::Invalid,
        MihomoErrorKind::Unavailable => AdminErrorKind::Unavailable,
    };
    AdminError::new(kind, error.to_string()).with_source(error)
}
