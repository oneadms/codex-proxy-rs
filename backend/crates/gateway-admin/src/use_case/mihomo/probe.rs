//! 节点出口位置与延迟的有界后台探测，复用代理测试的并发与持久化规则

use std::{sync::Arc, time::Duration};

use futures::future::BoxFuture;
use gateway_core::task::{
    ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerKind,
    WorkerRegistration, WorkerRunnable, WorkerSchedule, WorkerTaskError,
};

use super::DefaultMihomoService;
use crate::{
    model::{
        AdminError, AdminErrorKind, MutationActor, MutationContext, proxies::ProxyBatchResult,
    },
    use_case::proxies::ProxiesService,
};

impl DefaultMihomoService {
    pub(crate) fn probe_worker(
        self: &Arc<Self>,
        proxies: Arc<dyn ProxiesService>,
    ) -> Result<WorkerContribution, AdminError> {
        let id = WorkerId::try_new(WorkerKind::QuotaCatalogHealth, "mihomo_nodes")
            .map_err(|_| AdminError::internal("Mihomo 探测 Worker ID 不合法"))?;
        let schedule = WorkerSchedule::try_new(
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(20),
        )
        .map_err(|_| AdminError::internal("Mihomo 探测 Worker 调度配置不合法"))?;
        let registration = WorkerRegistration::try_new(
            id,
            WorkerRunnable::Scheduled {
                schedule,
                lease: None,
                task: Box::new(NodeProbeTask {
                    mihomo: Arc::clone(self),
                    proxies,
                }),
            },
        )
        .map_err(|_| AdminError::internal("Mihomo 探测 Worker 注册失败"))?;
        Ok(WorkerContribution::Registration(registration))
    }
}

struct NodeProbeTask {
    mihomo: Arc<DefaultMihomoService>,
    proxies: Arc<dyn ProxiesService>,
}

impl NodeProbeTask {
    async fn probe(&self) -> Result<(), AdminError> {
        let targets = self.mihomo.probe_targets().await?;
        if targets.is_empty() {
            return Ok(());
        }
        let context = MutationContext {
            actor: MutationActor::System,
            request_id: "mihomo-node-probe".to_owned(),
        };
        let results = self
            .proxies
            .test_batch(targets.into_iter().map(Ok).collect(), &context)
            .await?;
        for result in results {
            if let ProxyBatchResult::Completed(Err(error)) = result
                && !matches!(
                    error.kind(),
                    AdminErrorKind::Conflict | AdminErrorKind::RateLimited
                )
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

impl ScheduledTask for NodeProbeTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            tokio::select! {
                () = context.cancellation().cancelled() => Ok(()),
                result = self.probe() => result.map_err(|error| {
                    WorkerTaskError::safe("Mihomo node probe failed").with_source(error)
                }),
            }
        })
    }
}
