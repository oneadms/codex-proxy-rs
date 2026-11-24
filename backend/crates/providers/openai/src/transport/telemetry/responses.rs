//! Responses 诊断与已准备的 WebSocket 请求指标，不保留消息正文

use serde_json::Value;

use super::{
    metadata::RequestMetadata,
    record::{Recorder, boolean},
};

pub(super) fn reasoning_replay(recorder: &Recorder, response: &Value) {
    const REASONS: [&str; 6] = [
        "organization_mismatch",
        "end_user_mismatch",
        "model_mismatch",
        "prefix_mismatch",
        "turn_boundary",
        "other",
    ];
    let transformations = response
        .get("input_transformations")
        .and_then(Value::as_array);
    let mut counts = [0_u64; 6];
    for transformation in transformations.into_iter().flatten() {
        if transformation["type"] != "reasoning" || transformation["action"] != "removed" {
            continue;
        }
        let reason = transformation["reason"].as_str().unwrap_or("other");
        let index = REASONS
            .iter()
            .position(|value| *value == reason)
            .unwrap_or(5);
        counts[index] += 1;
    }
    let report = if transformations.is_none() {
        "unavailable"
    } else if counts[3] > 0 {
        "prefix_mismatch_reported"
    } else if counts.iter().any(|count| *count > 0) {
        "removals_reported"
    } else {
        "no_removals_reported"
    };
    let mut tags = vec![("report", report)];
    if let Some(sku) = &recorder.product_sku {
        tags.push(("product_sku", sku));
    }
    recorder.count("codex.responses.reasoning_replay", 1, &tags);
    if transformations.is_some() {
        for (reason, count) in REASONS.into_iter().zip(counts) {
            tags[0] = ("reason", reason);
            recorder.sample(
                "codex.responses.reasoning_items_removed",
                count as f64,
                &tags,
            );
        }
    }
}

#[derive(Clone)]
pub(super) struct Continuation {
    incremental: bool,
    warmup: bool,
    after_prewarm: bool,
    reason: &'static str,
    previous_item_type: &'static str,
    current_item_type: &'static str,
}

impl Continuation {
    pub(super) fn new(metadata: &RequestMetadata<'_>) -> Self {
        let incremental = metadata
            .body
            .get("previous_response_id")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty());
        let details = metadata.fact("websocket_continuation");
        let text = |field: &str, fallback: &'static str| {
            details
                .and_then(|value| value.get(field))
                .and_then(Value::as_str)
                .unwrap_or(fallback)
        };
        Self {
            incremental,
            warmup: metadata.body.get("generate").and_then(Value::as_bool) == Some(false),
            after_prewarm: details
                .and_then(|value| value.get("after_prewarm"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            reason: if incremental {
                "incremental"
            } else {
                bounded(
                    text("reason", "no_previous_request"),
                    &[
                        "no_previous_request",
                        "no_previous_response",
                        "no_previous_response_id",
                        "input_shortened",
                        "input_mismatch",
                        "restored_history",
                        "model_changed",
                        "tools_changed",
                        "tool_choice_changed",
                        "parallel_tool_calls_changed",
                        "reasoning_changed",
                        "store_changed",
                        "stream_changed",
                        "include_changed",
                        "service_tier_changed",
                        "prompt_cache_key_changed",
                        "prompt_cache_options_changed",
                        "text_changed",
                    ],
                    "other",
                )
            },
            previous_item_type: item_type(text("previous_item_type", "none")),
            current_item_type: item_type(text("current_item_type", "none")),
        }
    }

    pub(super) fn record(&self, recorder: &Recorder, after_prewarm: bool) {
        recorder.count(
            "codex.websocket.continuation",
            1,
            &[
                (
                    "mode",
                    if self.incremental {
                        "incremental"
                    } else {
                        "full"
                    },
                ),
                ("reason", self.reason),
                ("previous_item_type", self.previous_item_type),
                ("current_item_type", self.current_item_type),
                ("phase", if self.warmup { "warmup" } else { "generation" }),
                (
                    "after_prewarm",
                    boolean(self.after_prewarm || after_prewarm),
                ),
            ],
        );
    }
}

fn item_type(value: &str) -> &'static str {
    bounded(
        value,
        &[
            "none",
            "message",
            "reasoning",
            "function_call",
            "function_call_output",
            "custom_tool_call",
            "custom_tool_call_output",
            "local_shell_call",
            "web_search_call",
            "image_generation_call",
            "tool_search_call",
            "tool_search_output",
            "compaction",
            "context_compaction",
        ],
        "other",
    )
}

fn bounded(value: &str, known: &[&'static str], fallback: &'static str) -> &'static str {
    known
        .iter()
        .copied()
        .find(|known| *known == value)
        .unwrap_or(fallback)
}
