//! Mihomo 订阅同步、运行配置发布与稳定节点入口管理

mod probe;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::future::BoxFuture;
use gateway_core::{
    runtime::SnapshotControl,
    task::{
        ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerKind,
        WorkerRegistration, WorkerRunnable, WorkerSchedule, WorkerTaskError,
    },
};
use secrecy::ExposeSecret;
use tokio::sync::Mutex;

use crate::{
    model::{AdminError, MutationContext, Revision, mihomo::*},
    ports::{mihomo::MihomoRuntime, mihomo::MihomoSubscriptionFetcher, store::AdminStoreError},
};

use super::{error::map_store_error, publication::publish_committed};

#[async_trait]
pub trait MihomoService: Send + Sync {
    async fn subscriptions(&self) -> Result<Vec<MihomoSubscription>, AdminError>;
    async fn create(
        &self,
        command: NewMihomoSubscription,
        context: &MutationContext,
    ) -> Result<MihomoRefreshResult, AdminError>;
    async fn update(
        &self,
        command: UpdateMihomoSubscription,
        context: &MutationContext,
    ) -> Result<MihomoRefreshResult, AdminError>;
    async fn refresh(
        &self,
        id: &str,
        revision: Revision,
        context: &MutationContext,
    ) -> Result<MihomoRefreshResult, AdminError>;
    async fn delete(
        &self,
        id: &str,
        revision: Revision,
        context: &MutationContext,
    ) -> Result<Revision, AdminError>;
    async fn nodes(&self, query: MihomoNodeQuery) -> Result<MihomoNodePage, AdminError>;
}

pub(crate) struct DefaultMihomoService {
    store: Arc<dyn crate::ports::mihomo::MihomoStore>,
    fetcher: Arc<dyn MihomoSubscriptionFetcher>,
    runtime: Arc<dyn MihomoRuntime>,
    snapshot: Arc<dyn SnapshotControl>,
    mutations: Mutex<Option<Revision>>,
}

impl DefaultMihomoService {
    pub(crate) fn new(
        store: Arc<dyn crate::ports::mihomo::MihomoStore>,
        fetcher: Arc<dyn MihomoSubscriptionFetcher>,
        runtime: Arc<dyn MihomoRuntime>,
        snapshot: Arc<dyn SnapshotControl>,
    ) -> Self {
        Self {
            store,
            fetcher,
            runtime,
            snapshot,
            mutations: Mutex::new(None),
        }
    }

    pub(crate) async fn initialize_runtime(&self) -> Result<(), AdminError> {
        let mut published_revision = self.mutations.lock().await;
        // Core 已在 Admin 之前发布初始快照，这里只恢复本机的节点监听
        *published_revision = Some(self.apply_persisted().await?);
        Ok(())
    }

    pub(crate) fn runtime_worker(self: &Arc<Self>) -> Result<WorkerContribution, AdminError> {
        let id = WorkerId::try_new(WorkerKind::RuntimeSnapshotReconciliation, "mihomo_runtime")
            .map_err(|_| AdminError::internal("Mihomo 运行时 Worker ID 不合法"))?;
        let schedule = WorkerSchedule::try_new(
            Duration::from_secs(5),
            Duration::from_secs(1),
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(10),
        )
        .map_err(|_| AdminError::internal("Mihomo 运行时 Worker 调度配置不合法"))?;
        let registration = WorkerRegistration::try_new(
            id,
            WorkerRunnable::Scheduled {
                schedule,
                lease: None,
                task: Box::new(MihomoRuntimeReconciliationTask(Arc::clone(self))),
            },
        )
        .map_err(|_| AdminError::internal("Mihomo 运行时 Worker 注册失败"))?;
        Ok(WorkerContribution::Registration(registration))
    }

    async fn reconcile_runtime(&self) -> Result<(), AdminError> {
        let mut published_revision = self.mutations.lock().await;
        self.reconcile_runtime_locked(&mut published_revision).await
    }

    async fn probe_targets(&self) -> Result<Vec<crate::model::proxies::TestProxy>, AdminError> {
        let mut published_revision = self.mutations.lock().await;
        // 确认监听已恢复后才取探测名单，探测阶段不占用订阅写锁
        self.reconcile_runtime_locked(&mut published_revision)
            .await?;
        self.store
            .probe_targets()
            .await
            .map_err(|error| map_store_error(error, "mihomo probe"))
    }

    async fn fetch_and_validate(&self, url: &str) -> Result<Vec<MihomoNodeConfig>, AdminError> {
        let nodes = self.fetcher.fetch(url).await?;
        self.runtime.validate(&nodes).await?;
        Ok(nodes)
    }

    fn validate_subscription_name(name: &str) -> Result<String, AdminError> {
        let trimmed = name.trim();
        let character_count = trimmed.chars().count();
        if !(1..=100).contains(&character_count) || name.chars().any(char::is_control) {
            return Err(AdminError::invalid(
                "订阅名称必须为 1 至 100 个字符且不能包含控制字符",
            ));
        }
        Ok(trimmed.to_owned())
    }

    async fn apply_persisted(&self) -> Result<Revision, AdminError> {
        let snapshot = self
            .store
            .runtime_snapshot()
            .await
            .map_err(|error| map_store_error(error, "mihomo runtime"))?;
        self.runtime.apply(snapshot.nodes).await?;
        Ok(snapshot.config_revision)
    }

    async fn reconcile_runtime_locked(
        &self,
        published_revision: &mut Option<Revision>,
    ) -> Result<(), AdminError> {
        let revision = self.apply_persisted().await?;
        if *published_revision != Some(revision) {
            publish_committed(self.snapshot.as_ref(), revision).await?;
            // 完成发布后才推进版本，取消或失败的周期仍可重放
            *published_revision = Some(revision);
        }
        Ok(())
    }
}

struct MihomoRuntimeReconciliationTask(Arc<DefaultMihomoService>);

impl ScheduledTask for MihomoRuntimeReconciliationTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            tokio::select! {
                () = context.cancellation().cancelled() => Ok(()),
                result = self.0.reconcile_runtime() => result.map_err(|error| {
                    WorkerTaskError::safe("Mihomo runtime reconciliation failed").with_source(error)
                }),
            }
        })
    }
}

#[async_trait]
impl MihomoService for DefaultMihomoService {
    async fn subscriptions(&self) -> Result<Vec<MihomoSubscription>, AdminError> {
        self.store
            .subscriptions()
            .await
            .map_err(|error| map_store_error(error, "mihomo subscription"))
    }

    async fn create(
        &self,
        mut command: NewMihomoSubscription,
        context: &MutationContext,
    ) -> Result<MihomoRefreshResult, AdminError> {
        command.name = Self::validate_subscription_name(&command.name)?;
        let mut published_revision = self.mutations.lock().await;
        let nodes = self.fetch_and_validate(command.url.expose_secret()).await?;
        let result = self
            .store
            .create(command, nodes, context)
            .await
            .map_err(|error| map_store_error(error, "mihomo subscription"))?;
        self.reconcile_runtime_locked(&mut published_revision)
            .await?;
        Ok(result)
    }

    async fn update(
        &self,
        mut command: UpdateMihomoSubscription,
        context: &MutationContext,
    ) -> Result<MihomoRefreshResult, AdminError> {
        if let Some(name) = command.name.as_mut() {
            *name = Self::validate_subscription_name(name)?;
        }
        let mut published_revision = self.mutations.lock().await;
        let nodes = match command.url.as_ref() {
            Some(url) => Some(self.fetch_and_validate(url.expose_secret()).await?),
            None => None,
        };
        let result = self
            .store
            .update(command, nodes, context)
            .await
            .map_err(|error| map_store_error(error, "mihomo subscription"))?;
        self.reconcile_runtime_locked(&mut published_revision)
            .await?;
        Ok(result)
    }

    async fn refresh(
        &self,
        id: &str,
        revision: Revision,
        context: &MutationContext,
    ) -> Result<MihomoRefreshResult, AdminError> {
        let mut published_revision = self.mutations.lock().await;
        let subscription = self
            .store
            .subscription(id)
            .await
            .map_err(|error| map_store_error(error, "mihomo subscription"))?;
        if subscription.revision != revision {
            return Err(AdminError::conflict("订阅已被修改，请刷新后重试"));
        }
        let nodes = self
            .fetch_and_validate(subscription.url.expose_secret())
            .await?;
        let result = self
            .store
            .sync_nodes(id, revision, nodes, context)
            .await
            .map_err(|error| map_store_error(error, "mihomo subscription"))?;
        self.reconcile_runtime_locked(&mut published_revision)
            .await?;
        Ok(result)
    }

    async fn delete(
        &self,
        id: &str,
        revision: Revision,
        context: &MutationContext,
    ) -> Result<Revision, AdminError> {
        let mut published_revision = self.mutations.lock().await;
        let result = self
            .store
            .delete(id, revision, context)
            .await
            .map_err(|error| map_store_error(error, "mihomo subscription"))?;
        self.reconcile_runtime_locked(&mut published_revision)
            .await?;
        Ok(result)
    }

    async fn nodes(&self, query: MihomoNodeQuery) -> Result<MihomoNodePage, AdminError> {
        self.store
            .nodes(query)
            .await
            .map_err(|error: AdminStoreError| map_store_error(error, "mihomo node"))
    }
}
