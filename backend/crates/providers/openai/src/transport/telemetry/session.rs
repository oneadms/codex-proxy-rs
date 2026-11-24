//! 单次 Codex 模型请求的传输指标与托管工具事件，共用原始响应解码结果

use std::{sync::Arc, time::Duration};

use opentelemetry::KeyValue;
use reqwest::header::HeaderMap;
use serde_json::Value;

use super::{
    CodexWireProfile, hosted_tools::HostedTools, metadata::RequestMetadata, metrics::Metrics,
    record::Recorder, sanitize_tag,
};

#[derive(Clone)]
pub(crate) struct SessionTelemetry {
    analytics: Option<Arc<HostedTools>>,
    pub(super) recorder: Option<Recorder>,
    pub(super) observation: Option<Arc<super::activity::RequestObservation>>,
    continuation: super::responses::Continuation,
    reauth: bool,
}

impl SessionTelemetry {
    pub(super) fn new(
        metrics: Option<Arc<Metrics>>,
        profile: &CodexWireProfile,
        metadata: &RequestMetadata<'_>,
        headers: &HeaderMap,
        auth_mode: &'static str,
        analytics: Option<HostedTools>,
    ) -> Self {
        let inferred_source = match profile.originator.as_str() {
            "codex_cli_rs" | "codex-tui" | "codex-cli" => "cli",
            "codex_exec" => "exec",
            "codex_vscode" | "codex_desktop" => "vscode",
            "codex_mcp_server" => "mcp",
            _ => "unknown",
        };
        let source = match metadata
            .get("session_source", "sessionSource")
            .or_else(|| metadata.get("thread_source", "threadSource"))
        {
            Some(value) => value.as_str().unwrap_or("unknown").to_owned(),
            None => metadata
                .text("subagent_source", "subagentSource")
                .map(|source| format!("subagent_{source}"))
                .unwrap_or_else(|| inferred_source.to_owned()),
        };
        let mut attributes = vec![
            KeyValue::new("auth_mode", auth_mode),
            KeyValue::new("session_source", sanitize_tag(&source)),
            KeyValue::new("originator", bounded_originator(&profile.originator)),
            KeyValue::new(
                "model",
                sanitize_tag(
                    metadata
                        .body
                        .get("model")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                ),
            ),
            KeyValue::new("app.version", sanitize_tag(&profile.codex_version)),
        ];
        if let Some(service) = metadata
            .get("service_name", "serviceName")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            attributes.push(KeyValue::new("service_name", sanitize_tag(service)));
        }
        let product_sku = headers
            .get("x-openai-product-sku")
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(|value| if value == "codex" { "codex" } else { "other" }.to_owned());
        let recorder = metrics.as_ref().map(|metrics| Recorder {
            metrics: Arc::clone(metrics),
            attributes: attributes.clone(),
            product_sku,
            excluded: Vec::new(),
        });
        Self {
            analytics: analytics.map(Arc::new),
            recorder,
            observation: None,
            continuation: super::responses::Continuation::new(metadata),
            reauth: metadata
                .fact("websocket_connect")
                .and_then(|value| value.get("reauth"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }

    fn pair(
        &self,
        counter: &'static str,
        histogram: &'static str,
        elapsed: Duration,
        tags: &[(&'static str, &str)],
    ) {
        let Some(recorder) = &self.recorder else {
            return;
        };
        recorder.count(counter, 1, tags);
        recorder.sample(
            histogram,
            elapsed.as_millis().min(i64::MAX as u128) as f64,
            tags,
        );
    }

    pub(crate) fn api_request(&self, duration: Duration, status: Option<u16>, success: bool) {
        let status = status.map_or_else(|| "none".to_owned(), |status| status.to_string());
        self.pair(
            "codex.api_request",
            "codex.api_request.duration_ms",
            duration,
            &[("status", &status), ("success", boolean(success))],
        );
    }

    pub(crate) fn websocket_connect(&self, duration: Duration, success: bool) {
        self.pair(
            "codex.websocket.connect",
            "codex.websocket.connect.duration_ms",
            duration,
            &[
                ("success", boolean(success)),
                ("reauth", boolean(self.reauth)),
            ],
        );
    }

    pub(crate) fn websocket_request(&self, duration: Duration, success: bool) {
        self.pair(
            "codex.websocket.request",
            "codex.websocket.request.duration_ms",
            duration,
            &[("success", boolean(success))],
        );
    }

    pub(crate) fn websocket_continuation(&self) {
        if let Some(recorder) = &self.recorder {
            self.continuation.record(
                recorder,
                self.observation
                    .as_ref()
                    .is_some_and(|observation| observation.after_prewarm()),
            );
        }
    }

    fn observe(&self, value: &Value) {
        if let Some(analytics) = &self.analytics {
            analytics.observe(value);
        }
        if let Some(observation) = &self.observation {
            observation.observe(value);
        }
    }

    pub(crate) fn sse_event(&self, kind: &str, data: &str, duration: Duration) {
        let parsed = serde_json::from_str::<Value>(data);
        if let Ok(value) = &parsed {
            self.observe(value);
        }
        let success = data.trim() == "[DONE]"
            || (kind != "response.failed"
                && parsed.as_ref().is_ok_and(|value| {
                    kind != "response.output_item.done"
                        || value.get("type").and_then(Value::as_str).is_some()
                }));
        self.pair(
            "codex.sse_event",
            "codex.sse_event.duration_ms",
            duration,
            &[("kind", kind), ("success", boolean(success))],
        );
    }

    pub(crate) fn sse_failure(&self, duration: Duration) {
        self.pair(
            "codex.sse_event",
            "codex.sse_event.duration_ms",
            duration,
            &[("kind", "unknown"), ("success", "false")],
        );
    }

    pub(crate) fn websocket_event(&self, bytes: &[u8], duration: Duration) {
        let parsed = serde_json::from_slice::<Value>(bytes);
        if let Ok(value) = &parsed {
            self.observe(value);
        }
        let kind = match &parsed {
            Ok(value) => value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            Err(_) => "parse_error",
        };
        self.pair(
            "codex.websocket.event",
            "codex.websocket.event.duration_ms",
            duration,
            &[
                ("kind", kind),
                (
                    "success",
                    boolean(parsed.is_ok() && kind != "response.failed"),
                ),
            ],
        );
        if kind == "responsesapi.websocket_timing"
            && let Ok(value) = parsed
        {
            self.websocket_timing(&value);
        }
    }

    pub(crate) fn websocket_failure(&self, duration: Duration) {
        self.pair(
            "codex.websocket.event",
            "codex.websocket.event.duration_ms",
            duration,
            &[("kind", "unknown"), ("success", "false")],
        );
    }

    fn websocket_timing(&self, value: &Value) {
        let Some(recorder) = &self.recorder else {
            return;
        };
        let Some(timing) = value.get("timing_metrics") else {
            return;
        };
        for (field, metric, fractional) in [
            (
                "responses_duration_excl_engine_and_client_tool_time_ms",
                "codex.responses_api_overhead.duration_ms",
                false,
            ),
            (
                "engine_service_total_ms",
                "codex.responses_api_inference_time.duration_ms",
                false,
            ),
            (
                "engine_iapi_ttft_total_ms",
                "codex.responses_api_engine_iapi_ttft.duration_ms",
                false,
            ),
            (
                "engine_service_ttft_total_ms",
                "codex.responses_api_engine_service_ttft.duration_ms",
                false,
            ),
            (
                "engine_iapi_tbt_across_engine_calls_ms",
                "codex.responses_api_engine_iapi_tbt.duration_ms",
                true,
            ),
            (
                "engine_service_tbt_across_engine_calls_ms",
                "codex.responses_api_engine_service_tbt.duration_ms",
                true,
            ),
        ] {
            if let Some(value) = timing
                .get(field)
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && *value >= 0.0)
            {
                let value = value.min(u64::MAX as f64);
                let value = if fractional {
                    value
                } else {
                    (value.round() as u64).min(i64::MAX as u64) as f64
                };
                recorder.sample(metric, value, &[]);
            }
        }
    }
}

fn boolean(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn bounded_originator(value: &str) -> &'static str {
    let value = sanitize_tag(value);
    [
        "codex_desktop",
        "codex-app-server",
        "codex_mcp_server",
        "codex_cli_rs",
        "codex-tui",
        "codex_vscode",
        "none",
        "codex_exec",
        "codex-cli",
        "codex_sdk_ts",
        "codex-app-server-sdk",
    ]
    .into_iter()
    .find(|known| *known == value)
    .unwrap_or("other")
}
