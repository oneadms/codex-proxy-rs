//! 单次 Codex 模型请求的传输指标与托管工具事件，共用原始响应解码结果

use std::{sync::Arc, time::Duration};

use opentelemetry::KeyValue;
use serde_json::{Map, Value};

use super::{CodexWireProfile, hosted_tools::HostedTools, metrics::Metrics, sanitize_tag};

#[derive(Clone)]
pub(crate) struct SessionTelemetry {
    metrics: Option<Arc<Metrics>>,
    attributes: Vec<KeyValue>,
    analytics: Option<Arc<HostedTools>>,
}

impl SessionTelemetry {
    pub(super) fn new(
        metrics: Option<Arc<Metrics>>,
        profile: &CodexWireProfile,
        body: &Map<String, Value>,
        auth_mode: &'static str,
        analytics: Option<HostedTools>,
    ) -> Self {
        let source = match profile.originator.as_str() {
            "codex_cli_rs" | "codex-tui" | "codex-cli" => "cli",
            "codex_exec" => "exec",
            "codex_vscode" | "codex_desktop" => "vscode",
            "codex_mcp_server" => "mcp",
            _ => "unknown",
        };
        Self {
            metrics,
            analytics: analytics.map(Arc::new),
            attributes: vec![
                KeyValue::new("auth_mode", auth_mode),
                KeyValue::new("session_source", source),
                KeyValue::new("originator", bounded_originator(&profile.originator)),
                KeyValue::new(
                    "model",
                    sanitize_tag(
                        body.get("model")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    ),
                ),
                KeyValue::new("app.version", sanitize_tag(&profile.codex_version)),
            ],
        }
    }

    fn pair(
        &self,
        counter: &'static str,
        histogram: &'static str,
        elapsed: Duration,
        tags: &[(&'static str, &str)],
    ) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let mut attributes = self.attributes.clone();
        attributes.extend(
            tags.iter()
                .map(|(key, value)| KeyValue::new(*key, sanitize_tag(value))),
        );
        metrics.counter(counter, &attributes);
        metrics.duration(
            histogram,
            elapsed.as_millis().min(i64::MAX as u128) as f64,
            &attributes,
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
            &[("success", boolean(success)), ("reauth", "false")],
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

    pub(crate) fn sse_event(&self, kind: &str, data: &str, duration: Duration) {
        let parsed = serde_json::from_str::<Value>(data);
        if let (Some(analytics), Ok(value)) = (&self.analytics, &parsed) {
            analytics.observe(value);
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
        if let (Some(analytics), Ok(value)) = (&self.analytics, &parsed) {
            analytics.observe(value);
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
        let Some(metrics) = &self.metrics else {
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
                metrics.duration(metric, value, &self.attributes);
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
