//! Mihomo 订阅持久化、受管进程和订阅解析端口

use async_trait::async_trait;

use crate::model::{
    AdminError, MutationContext, Revision,
    mihomo::{
        MihomoNodeConfig, MihomoNodePage, MihomoNodeQuery, MihomoRefreshResult, MihomoRuntimeNode,
        MihomoRuntimeSnapshot, MihomoSubscription, NewMihomoSubscription, UpdateMihomoSubscription,
    },
};

use super::store::AdminStoreResult;

#[async_trait]
pub trait MihomoStore: Send + Sync {
    async fn subscriptions(&self) -> AdminStoreResult<Vec<MihomoSubscription>>;
    async fn subscription(&self, id: &str) -> AdminStoreResult<MihomoSubscription>;
    async fn create(
        &self,
        command: NewMihomoSubscription,
        nodes: Vec<MihomoNodeConfig>,
        context: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult>;
    async fn update(
        &self,
        command: UpdateMihomoSubscription,
        nodes: Option<Vec<MihomoNodeConfig>>,
        context: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult>;
    async fn sync_nodes(
        &self,
        id: &str,
        revision: Revision,
        nodes: Vec<MihomoNodeConfig>,
        context: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult>;
    async fn delete(
        &self,
        id: &str,
        revision: Revision,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision>;
    async fn nodes(&self, query: MihomoNodeQuery) -> AdminStoreResult<MihomoNodePage>;
    async fn runtime_snapshot(&self) -> AdminStoreResult<MihomoRuntimeSnapshot>;
}

#[async_trait]
pub trait MihomoSubscriptionFetcher: Send + Sync {
    async fn fetch(&self, url: &str) -> Result<Vec<MihomoNodeConfig>, AdminError>;
}

#[async_trait]
pub trait MihomoRuntime: Send + Sync {
    async fn validate(&self, nodes: &[MihomoNodeConfig]) -> Result<(), AdminError>;
    async fn apply(&self, nodes: Vec<MihomoRuntimeNode>) -> Result<(), AdminError>;
    async fn shutdown(&self);
}
