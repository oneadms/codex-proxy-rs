//! 客户端启动、Hook 和 Guardian 事实及缺失本地测量的模拟值

mod tools;
pub(super) use tools::Tools;

use serde_json::Value;

use super::{
    catalog, facts,
    metadata::RequestMetadata,
    record::{Recorder, boolean},
    turn::{Usage, number},
};

pub(super) fn process_start(recorder: &Recorder, metadata: &RequestMetadata<'_>) {
    let originator = recorder
        .attributes
        .iter()
        .find(|tag| tag.key.as_str() == "originator")
        .map(|tag| tag.value.to_string())
        .unwrap_or_else(|| "other".to_owned());
    facts::emit(
        recorder,
        "codex.process.start",
        1.,
        &[("originator", &originator)],
    );
    let startup = metadata.fact("startup").unwrap_or(&Value::Null);
    if let Some(phases) = startup["phases"].as_array() {
        for phase in phases.iter().take(32) {
            facts::emit(
                recorder,
                "codex.startup.phase.duration_ms",
                number(phase, "duration_ms").unwrap_or(1.),
                &[
                    (
                        "phase",
                        phase["phase"].as_str().unwrap_or("configure_session"),
                    ),
                    ("status", phase["status"].as_str().unwrap_or("success")),
                ],
            );
        }
    } else {
        // 网关看不到客户端启动前的计时，虚拟启动只产生一次确定的阶段样本
        facts::emit(
            recorder,
            "codex.startup.phase.duration_ms",
            1.,
            &[("phase", "configure_session"), ("status", "success")],
        );
    }
    if let Some(sync) = metadata.fact("plugin_sync") {
        let tags = [
            ("transport", sync["transport"].as_str().unwrap_or("http")),
            ("status", sync["status"].as_str().unwrap_or("success")),
        ];
        facts::emit(recorder, "codex.plugins.startup_sync", 1., &tags);
        facts::emit(recorder, "codex.plugins.startup_sync.final", 1., &tags);
    }
}

pub(super) fn thread_start(recorder: &Recorder, metadata: &RequestMetadata<'_>) {
    let thread = metadata.fact("thread").unwrap_or(&Value::Null);
    facts::emit(
        recorder,
        "codex.thread.started",
        1.,
        &[
            (
                "is_git",
                boolean(thread["is_git"].as_bool().unwrap_or(false)),
            ),
            (
                "is_worktree",
                thread["is_worktree"]
                    .as_bool()
                    .map(boolean)
                    .unwrap_or("unknown"),
            ),
        ],
    );
    catalog::skills(recorder, metadata);
}

pub(super) fn request_facts(recorder: &Recorder, metadata: &RequestMetadata<'_>) {
    for hook in metadata
        .fact("hooks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(64)
    {
        let tags = [
            (
                "hook_name",
                hook["hook_name"].as_str().unwrap_or("PreModelRequest"),
            ),
            ("source", hook["source"].as_str().unwrap_or("unknown")),
            ("status", hook["status"].as_str().unwrap_or("completed")),
            (
                "handler_type",
                hook["handler_type"].as_str().unwrap_or("command"),
            ),
            (
                "execution_mode",
                hook["execution_mode"].as_str().unwrap_or("sync"),
            ),
        ];
        facts::emit(recorder, "codex.hooks.run", 1., &tags);
        facts::emit(
            recorder,
            "codex.hooks.run.duration_ms",
            number(hook, "duration_ms").unwrap_or(1.),
            &tags,
        );
    }
    if let Some(artifact) = metadata.fact("artifact_operation") {
        let tags = [
            ("skill", artifact["skill"].as_str().unwrap_or("unknown")),
            (
                "artifact_type",
                artifact["artifact_type"].as_str().unwrap_or("unknown"),
            ),
            (
                "operation_kind",
                artifact["operation_kind"].as_str().unwrap_or("create"),
            ),
            (
                "output_format",
                artifact["output_format"].as_str().unwrap_or("unknown"),
            ),
            (
                "execution_backend",
                artifact["execution_backend"]
                    .as_str()
                    .unwrap_or("unified_exec"),
            ),
        ];
        facts::emit(recorder, "codex.artifact.operation.started", 1., &tags);
        facts::emit(
            recorder,
            "codex.artifact.operation.expected_output_count",
            number(artifact, "expected_output_count").unwrap_or(1.),
            &tags,
        );
    }
}

pub(super) fn guardian(
    recorder: &Recorder,
    facts: &Value,
    response: &Value,
    elapsed_ms: f64,
    ttft: Option<f64>,
) {
    let tags = [
        (
            "context_mode",
            facts["context_mode"].as_str().unwrap_or("unknown"),
        ),
        ("decision", facts["decision"].as_str().unwrap_or("approved")),
        (
            "terminal_status",
            facts["terminal_status"].as_str().unwrap_or("approved"),
        ),
        (
            "failure_reason",
            facts["failure_reason"].as_str().unwrap_or("none"),
        ),
        (
            "approval_request_source",
            facts["approval_request_source"]
                .as_str()
                .unwrap_or("main_turn"),
        ),
        ("action", facts["action"].as_str().unwrap_or("unknown")),
        (
            "session_kind",
            facts["session_kind"].as_str().unwrap_or("none"),
        ),
        (
            "guardian_model",
            response["model"].as_str().unwrap_or("unknown"),
        ),
        (
            "had_prior_review_context",
            facts["had_prior_review_context"]
                .as_bool()
                .map(boolean)
                .unwrap_or("unknown"),
        ),
        (
            "reviewed_action_truncated",
            boolean(
                facts["reviewed_action_truncated"]
                    .as_bool()
                    .unwrap_or(false),
            ),
        ),
        ("risk_level", facts["risk_level"].as_str().unwrap_or("none")),
        (
            "user_authorization",
            facts["user_authorization"].as_str().unwrap_or("none"),
        ),
        ("outcome", facts["outcome"].as_str().unwrap_or("none")),
        (
            "guardian_reasoning_effort",
            facts["guardian_reasoning_effort"]
                .as_str()
                .unwrap_or("none"),
        ),
    ];
    let mut counter_tags = tags.to_vec();
    counter_tags.extend([
        (
            "context_changed",
            boolean(facts["context_changed"].as_bool().unwrap_or(false)),
        ),
        (
            "turn_origin",
            facts["turn_origin"].as_str().unwrap_or("main_turn"),
        ),
    ]);
    facts::emit(recorder, "codex.guardian.review", 1., &counter_tags);
    facts::emit(
        recorder,
        "codex.guardian.review.duration_ms",
        number(facts, "duration_ms").unwrap_or(elapsed_ms),
        &tags,
    );
    if let Some(value) = number(facts, "ttft_ms").or(ttft) {
        facts::emit(
            recorder,
            "codex.guardian.review.ttft.duration_ms",
            value,
            &tags,
        );
    }
    if let Some(usage) = Usage::parse(&response["usage"]) {
        usage.emit(recorder, "codex.guardian.review.token_usage", &tags);
        let mut non_cached = tags.to_vec();
        non_cached.push(("token_type", "non_cached_input"));
        recorder.sample(
            "codex.guardian.review.token_usage",
            usage.0[1]
                .saturating_sub(usage.0[2])
                .saturating_sub(usage.0[3]) as f64,
            &non_cached,
        );
    }
    let mut totals = std::collections::BTreeMap::new();
    for truncation in facts["truncations"]
        .as_array()
        .into_iter()
        .flatten()
        .take(32)
    {
        let Some(original) = number(truncation, "original_bytes") else {
            continue;
        };
        let retained = number(truncation, "retained_bytes")
            .unwrap_or(original)
            .min(original);
        if retained == original {
            continue;
        }
        let component = truncation["component"]
            .as_str()
            .unwrap_or("reviewed_action");
        let disposition = if retained == 0. {
            "omitted"
        } else {
            "truncated"
        };
        let total = totals.entry((component, disposition)).or_insert((0., 0.));
        total.0 += original;
        total.1 += retained;
    }
    for ((component, disposition), (original, retained)) in totals {
        let tags = [("component", component), ("disposition", disposition)];
        facts::emit(recorder, "codex.guardian.review.truncation", 1., &tags);
        for (measurement, bytes) in [
            ("original", original),
            ("retained", retained),
            ("omitted", original - retained),
        ] {
            let mut tags = tags.to_vec();
            tags.push(("measurement", measurement));
            facts::emit(
                recorder,
                "codex.guardian.review.truncation.bytes",
                bytes,
                &tags,
            );
        }
    }
}

pub(super) fn guardian_facts(value: &Value) -> Value {
    // 在途观测仅保留指标维度和数值，不延长其他请求字段的生命周期
    let mut fields = serde_json::Map::new();
    for key in [
        "context_mode",
        "decision",
        "terminal_status",
        "failure_reason",
        "approval_request_source",
        "action",
        "session_kind",
        "had_prior_review_context",
        "reviewed_action_truncated",
        "risk_level",
        "user_authorization",
        "outcome",
        "guardian_reasoning_effort",
        "context_changed",
        "turn_origin",
        "duration_ms",
        "ttft_ms",
    ] {
        if let Some(field) = value.get(key) {
            let field = match field {
                Value::String(text) => Value::String(super::sanitize_tag(text)),
                Value::Number(_) | Value::Bool(_) => field.clone(),
                _ => continue,
            };
            fields.insert(key.to_owned(), field);
        }
    }
    if let Some(truncations) = value["truncations"].as_array() {
        fields.insert("truncations".to_owned(), Value::Array(truncations.iter().take(32).map(|truncation| serde_json::json!({"component":super::sanitize_tag(truncation["component"].as_str().unwrap_or("reviewed_action")), "original_bytes":number(truncation,"original_bytes"), "retained_bytes":number(truncation,"retained_bytes")})).collect()));
    }
    Value::Object(fields)
}
