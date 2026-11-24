//! Codex 请求遥测的共享生命周期与后台导出，隔离指标与 analytics 出口

mod analytics;
mod hosted_tools;
mod metrics;
mod session;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{StreamExt as _, future::BoxFuture};
use gateway_core::task::{
    DaemonRestartPolicy, DaemonTask, WorkerContribution, WorkerDaemonContext,
    WorkerDefinitionError, WorkerId, WorkerKind, WorkerRegistration, WorkerRunnable,
    WorkerTaskError,
};

use reqwest::header::HeaderMap;
use serde_json::{Map, Value};

use super::{CodexBackendClient, CodexRequestContext, profile::CodexWireProfile};
use crate::config::{CodexMetricsExporter, CodexTelemetrySettings};

pub(super) use session::SessionTelemetry;

const EXPORT_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) struct CodexTelemetry {
    settings: CodexTelemetrySettings,
    exporters: Mutex<HashMap<String, Arc<metrics::Metrics>>>,
    analytics: Arc<analytics::Analytics>,
}

impl CodexBackendClient {
    /// 为客户端绑定官方遥测出口及交给 Host 监督的导出任务
    /// 调试构建沿官方规则关闭隐式 Statsig，显式 OTLP 配置仍生效
    pub fn with_telemetry(
        mut self,
        settings: CodexTelemetrySettings,
    ) -> Result<(Self, WorkerContribution), WorkerDefinitionError> {
        let telemetry = Arc::new(CodexTelemetry {
            settings,
            exporters: Mutex::new(HashMap::new()),
            analytics: Arc::new(analytics::Analytics::new()),
        });
        self.telemetry = Some(Arc::clone(&telemetry));
        let worker = WorkerRegistration::try_new(
            WorkerId::try_new(WorkerKind::OpsFlush, "openai-telemetry")?,
            WorkerRunnable::Daemon {
                restart: DaemonRestartPolicy::try_new(
                    Duration::from_secs(1),
                    Duration::from_secs(60),
                )?,
                task: Box::new(TelemetryTask(telemetry)),
            },
        )?;
        Ok((self, WorkerContribution::Registration(worker)))
    }

    pub(super) fn response_telemetry(
        &self,
        body: &Map<String, Value>,
        headers: &HeaderMap,
        profile: &CodexWireProfile,
        context: CodexRequestContext<'_>,
    ) -> Option<SessionTelemetry> {
        if self.peer_route.is_some() {
            return None;
        }
        let auth_mode = match self.protocol {
            super::client::OpenAiUpstreamProtocol::Codex => "chatgpt",
            super::client::OpenAiUpstreamProtocol::ResponsesApi => "api_key",
            super::client::OpenAiUpstreamProtocol::MessagesApi => return None,
        };
        let telemetry = self.telemetry.as_ref()?;
        if !telemetry.settings.enabled {
            return None;
        }
        let metrics = self.response_metrics(telemetry, profile);
        // 官方只允许带插件归因的少数事件使用 API Key；这两类宿主工具事件仅走 Codex OAuth
        let analytics = (telemetry.settings.analytics_enabled
            && self.protocol == super::client::OpenAiUpstreamProtocol::Codex)
            .then(|| {
                hosted_tools::HostedTools::new(
                    Arc::clone(&telemetry.analytics),
                    self,
                    profile,
                    body,
                    headers,
                    context,
                )
            })
            .flatten();
        if metrics.is_none() && analytics.is_none() {
            return None;
        }
        Some(SessionTelemetry::new(
            metrics, profile, body, auth_mode, analytics,
        ))
    }

    fn response_metrics(
        &self,
        telemetry: &CodexTelemetry,
        profile: &CodexWireProfile,
    ) -> Option<Arc<metrics::Metrics>> {
        if matches!(
            telemetry.settings.metrics_exporter,
            CodexMetricsExporter::None
        ) || (cfg!(debug_assertions)
            && matches!(
                telemetry.settings.metrics_exporter,
                CodexMetricsExporter::Statsig
            ))
        {
            return None;
        }
        let key = format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            self.egress_key,
            profile.originator,
            profile.codex_version,
            profile.os_type,
            profile.os_version,
            profile.arch
        );
        let mut exporters = telemetry
            .exporters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let metrics = match exporters.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => Arc::clone(entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let metrics = match metrics::Metrics::new(
                    &telemetry.settings,
                    profile,
                    self.client.clone(),
                ) {
                    Ok(metrics) => Arc::new(metrics),
                    Err(_) => {
                        tracing::warn!("Codex 指标出口初始化失败");
                        return None;
                    }
                };
                entry.insert(Arc::clone(&metrics));
                metrics
            }
        };
        Some(metrics)
    }
}

struct TelemetryTask(Arc<CodexTelemetry>);

impl DaemonTask for TelemetryTask {
    fn run(&self, context: WorkerDaemonContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + EXPORT_INTERVAL,
                EXPORT_INTERVAL,
            );
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut receiver = self.0.analytics.receiver.lock().await;
            loop {
                tokio::select! {
                    biased;
                    _ = context.cancellation().cancelled() => {
                        // 停机后拒绝新事件，在官方总期限内排空已接收事件并导出指标
                        receiver.close();
                        let result = tokio::time::timeout(analytics::FLUSH_TIMEOUT, async {
                            let drain = async {
                                let mut failed = false;
                                while let Some(delivery) = receiver.recv().await {
                                    failed |= delivery.send().await.is_err();
                                }
                                if failed {
                                    Err(WorkerTaskError::safe("Codex analytics 上报失败"))
                                } else {
                                    Ok(())
                                }
                            };
                            let (metrics, analytics) = tokio::join!(self.0.flush(), drain);
                            metrics.and(analytics)
                        }).await.unwrap_or_else(|_| Err(WorkerTaskError::safe("Codex 遥测清理超时")));
                        // 到期未发送的事件一并释放，避免停机后继续持有认证快照
                        while receiver.try_recv().is_ok() {}
                        return result;
                    }
                    _ = interval.tick() => {
                        self.0.flush().await?;
                        context.succeeded();
                    }
                    Some(delivery) = receiver.recv() => {
                        if delivery.send().await.is_ok() {
                            context.succeeded();
                        } else {
                            // analytics 沿官方规则丢弃失败事件，不重启共享指标调度
                            tracing::warn!("Codex analytics 上报失败");
                        }
                    }
                }
            }
        })
    }
}

impl CodexTelemetry {
    async fn flush(&self) -> Result<(), WorkerTaskError> {
        let exporters: Vec<_> = self
            .exporters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        // 每个出口独立计时；单个代理失效不能阻塞其他账号或无限延长停机
        let results: Vec<_> = futures::stream::iter(exporters)
            .map(|metrics| async move { metrics.export().await })
            .buffer_unordered(8)
            .collect()
            .await;
        // 在途流强持有自己的聚合器；空闲出口在最后一次导出后释放
        self.exporters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|_, metrics| Arc::strong_count(metrics) > 1 || metrics.has_pending());
        if results.iter().any(Result::is_err) {
            return Err(WorkerTaskError::safe("Codex 指标上报失败"));
        }
        Ok(())
    }
}

fn sanitize_tag(value: &str) -> String {
    // 对齐 codex-utils-string::sanitize_metric_tag_value 的字符与长度合同
    let value: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '/') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let value = value.trim_matches('_');
    if value.is_empty() || !value.chars().any(|ch| ch.is_ascii_alphanumeric()) {
        "unspecified".to_owned()
    } else {
        value[..value.len().min(256)].to_owned()
    }
}
