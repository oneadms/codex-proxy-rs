//! Codex 请求遥测的共享生命周期与后台导出，隔离指标与 analytics 出口

mod activity;
mod analytics;
mod catalog;
mod facts;
mod hosted_tools;
mod local;
mod metadata;
mod metrics;
mod record;
mod responses;
mod session;
mod turn;

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
    activity: Arc<activity::Activity>,
}

impl CodexBackendClient {
    /// 为客户端绑定官方遥测出口及交给 Host 监督的导出任务
    /// 调试构建沿官方规则关闭隐式 Statsig，显式 OTLP 配置仍生效
    pub fn with_telemetry(
        mut self,
        settings: CodexTelemetrySettings,
    ) -> Result<(Self, WorkerContribution), WorkerDefinitionError> {
        let (telemetry, worker) = CodexTelemetry::initialize(settings)?;
        self.telemetry = Some(telemetry);
        Ok((self, worker))
    }

    pub(super) fn response_telemetry(
        &self,
        body: &Map<String, Value>,
        headers: &HeaderMap,
        profile: &CodexWireProfile,
        context: CodexRequestContext<'_>,
    ) -> Option<SessionTelemetry> {
        if self.peer_route.is_some() || !self.telemetry_enabled {
            return None;
        }
        let auth_mode = match self.protocol {
            super::client::OpenAiUpstreamProtocol::Codex => "Chatgpt",
            super::client::OpenAiUpstreamProtocol::ResponsesApi => "ApiKey",
            super::client::OpenAiUpstreamProtocol::MessagesApi => return None,
        };
        let telemetry = self.telemetry.as_ref()?;
        if !telemetry.settings.enabled {
            return None;
        }
        let metadata = metadata::RequestMetadata::new(body, headers);
        let analytics_enabled = telemetry.settings.analytics_enabled
            && metadata
                .get("analytics_enabled", "analyticsEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(true);
        let metrics = self.response_metrics(telemetry, profile, analytics_enabled);
        // 官方只允许带插件归因的少数事件使用 API Key；这两类宿主工具事件仅走 Codex OAuth
        let analytics = (analytics_enabled
            && self.protocol == super::client::OpenAiUpstreamProtocol::Codex)
            .then(|| {
                hosted_tools::HostedTools::new(
                    Arc::clone(&telemetry.analytics),
                    self,
                    profile,
                    &metadata,
                    headers,
                    context,
                )
            })
            .flatten();
        if metrics.is_none() && analytics.is_none() {
            return None;
        }
        let mut session =
            SessionTelemetry::new(metrics, profile, &metadata, headers, auth_mode, analytics);
        if let Some(recorder) = &session.recorder {
            let namespace = format!(
                "{}\0{}\0{}\0{}\0{}",
                self.egress_key, auth_mode, profile.os_type, profile.os_version, profile.arch
            );
            session.observation = Some(Arc::new(telemetry.activity.request(
                recorder.clone(),
                &metadata,
                headers,
                &namespace,
                context.request_id,
            )));
        }
        Some(session)
    }

    fn response_metrics(
        &self,
        telemetry: &CodexTelemetry,
        profile: &CodexWireProfile,
        analytics_enabled: bool,
    ) -> Option<Arc<metrics::Metrics>> {
        if matches!(
            telemetry.settings.metrics_exporter,
            CodexMetricsExporter::None
        ) || ((!analytics_enabled || cfg!(debug_assertions))
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
            let mut deliveries = Box::pin(analytics::deliver(&mut receiver, &context));
            loop {
                tokio::select! {
                    biased;
                    _ = context.cancellation().cancelled() => {
                        self.0.activity.expire(true);
                        // 停机后拒绝新事件，在官方总期限内排空已接收事件并导出指标
                        let result = tokio::time::timeout(analytics::FLUSH_TIMEOUT, async {
                            let (metrics, analytics) = tokio::join!(self.0.flush(), &mut deliveries);
                            metrics.and(analytics)
                        }).await.unwrap_or_else(|_| Err(WorkerTaskError::safe("Codex 遥测清理超时")));
                        drop(deliveries);
                        receiver.close();
                        // 到期未发送的事件一并释放，避免停机后继续持有认证快照
                        while receiver.try_recv().is_ok() {}
                        return result;
                    }
                    _ = interval.tick() => {
                        self.0.activity.expire(false);
                        self.0.flush().await?;
                        context.succeeded();
                    }
                    result = &mut deliveries => return result,
                }
            }
        })
    }
}

impl CodexTelemetry {
    /// 一个 Provider 代次共享同一队列与导出任务，预热请求只绑定已有 owner
    pub(crate) fn initialize(
        settings: CodexTelemetrySettings,
    ) -> Result<(Arc<Self>, WorkerContribution), WorkerDefinitionError> {
        let telemetry = Arc::new(Self {
            settings,
            exporters: Mutex::new(HashMap::new()),
            analytics: Arc::new(analytics::Analytics::new()),
            activity: Arc::new(activity::Activity::new()),
        });
        let worker = WorkerRegistration::try_new(
            WorkerId::try_new(WorkerKind::OpsFlush, "openai-telemetry")?,
            WorkerRunnable::Daemon {
                restart: DaemonRestartPolicy::try_new(
                    Duration::from_secs(1),
                    Duration::from_secs(60),
                )?,
                task: Box::new(TelemetryTask(Arc::clone(&telemetry))),
            },
        )?;
        Ok((telemetry, WorkerContribution::Registration(worker)))
    }

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
        // 在途流和未过期的客户端状态持有聚合器，其余出口在最后一次导出后释放
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
