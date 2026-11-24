//! 接收客户端明确提供的官方指标事实，限制名称、维度和重复事件

use lru::LruCache;
use serde_json::Value;

use super::{metadata::RequestMetadata, record::Recorder};

pub(super) struct Spec {
    name: &'static str,
    counter: bool,
    global: bool,
    tags: &'static [&'static str],
}

const GUARDIAN: &[&str] = &[
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
    "guardian_model",
    "guardian_reasoning_effort",
    "context_changed",
    "turn_origin",
    "tool",
    "token_type",
];

pub(super) fn spec(name: &str) -> Option<Spec> {
    let (name, counter, global, tags): (&'static str, bool, bool, &'static [&'static str]) =
        match name {
            "codex.artifact.operation.started" => (
                "codex.artifact.operation.started",
                true,
                false,
                &[
                    "skill",
                    "artifact_type",
                    "operation_kind",
                    "output_format",
                    "execution_backend",
                ],
            ),
            "codex.artifact.operation.expected_output_count" => (
                "codex.artifact.operation.expected_output_count",
                false,
                false,
                &[
                    "skill",
                    "artifact_type",
                    "operation_kind",
                    "output_format",
                    "execution_backend",
                ],
            ),
            "codex.exec_command.calls" => (
                "codex.exec_command.calls",
                true,
                false,
                &["success", "product_sku"],
            ),
            "codex.exec_command.duration_total_ms" => (
                "codex.exec_command.duration_total_ms",
                true,
                false,
                &["success", "product_sku"],
            ),
            "codex.tool.call" => (
                "codex.tool.call",
                true,
                false,
                &["tool", "success", "product_sku"],
            ),
            "codex.tool.call.duration_ms" => (
                "codex.tool.call.duration_ms",
                false,
                false,
                &["tool", "success", "product_sku"],
            ),
            "codex.tool.unified_exec" => ("codex.tool.unified_exec", true, false, &["tty"]),
            "exec_server_client_requests_total" => {
                ("exec_server_client_requests_total", true, true, &["method"])
            }
            "codex.goal.created" => ("codex.goal.created", true, true, &["reasoning_effort"]),
            "codex.goal.resumed" => ("codex.goal.resumed", true, true, &[]),
            "codex.goal.completed" => ("codex.goal.completed", true, true, &[]),
            "codex.goal.blocked" => ("codex.goal.blocked", true, true, &[]),
            "codex.goal.budget_limited" => ("codex.goal.budget_limited", true, true, &[]),
            "codex.goal.usage_limited" => ("codex.goal.usage_limited", true, true, &[]),
            "codex.goal.duration_s" => ("codex.goal.duration_s", false, true, &["status"]),
            "codex.goal.token_count" => ("codex.goal.token_count", false, true, &["status"]),
            "codex.guardian.review" => ("codex.guardian.review", true, false, GUARDIAN),
            "codex.guardian.review.duration_ms" => {
                ("codex.guardian.review.duration_ms", false, false, GUARDIAN)
            }
            "codex.guardian.review.ttft.duration_ms" => (
                "codex.guardian.review.ttft.duration_ms",
                false,
                false,
                GUARDIAN,
            ),
            "codex.guardian.review.token_usage" => {
                ("codex.guardian.review.token_usage", false, false, GUARDIAN)
            }
            "codex.guardian.review.truncation" => (
                "codex.guardian.review.truncation",
                true,
                false,
                &["component", "disposition"],
            ),
            "codex.guardian.review.truncation.bytes" => (
                "codex.guardian.review.truncation.bytes",
                false,
                false,
                &["component", "disposition", "measurement"],
            ),
            "codex.hooks.run" => (
                "codex.hooks.run",
                true,
                false,
                &[
                    "hook_name",
                    "source",
                    "status",
                    "handler_type",
                    "execution_mode",
                ],
            ),
            "codex.hooks.run.duration_ms" => (
                "codex.hooks.run.duration_ms",
                false,
                false,
                &[
                    "hook_name",
                    "source",
                    "status",
                    "handler_type",
                    "execution_mode",
                ],
            ),
            "codex.multi_agent.spawn.failure" => (
                "codex.multi_agent.spawn.failure",
                true,
                true,
                &[
                    "fork_mode",
                    "multi_agent_version",
                    "reason",
                    "detail",
                    "error_kind",
                    "product_sku",
                ],
            ),
            "codex.multi_agent.spawn.phase.duration_ms" => (
                "codex.multi_agent.spawn.phase.duration_ms",
                false,
                true,
                &[
                    "fork_mode",
                    "multi_agent_version",
                    "history_mode",
                    "phase",
                    "product_sku",
                ],
            ),
            "codex.multi_agent.spawned" => (
                "codex.multi_agent.spawned",
                true,
                true,
                &["originator", "product_sku"],
            ),
            "codex.multi_agent.stop_accepted" => (
                "codex.multi_agent.stop_accepted",
                true,
                true,
                &[
                    "caller",
                    "action",
                    "previous_status",
                    "originator",
                    "product_sku",
                ],
            ),
            "codex.multi_agent.turn.lifecycle" => (
                "codex.multi_agent.turn.lifecycle",
                true,
                true,
                &[
                    "turn_kind",
                    "status",
                    "error_kind",
                    "abort_reason",
                    "originator",
                    "product_sku",
                ],
            ),
            "codex.plugins.install_elicitation.sent" => (
                "codex.plugins.install_elicitation.sent",
                true,
                false,
                &["tool_type"],
            ),
            "codex.plugins.install_suggestion" => (
                "codex.plugins.install_suggestion",
                true,
                false,
                &["tool_type", "response_action", "completed"],
            ),
            "codex.plugins.startup_sync" => (
                "codex.plugins.startup_sync",
                true,
                true,
                &["transport", "status"],
            ),
            "codex.plugins.startup_sync.final" => (
                "codex.plugins.startup_sync.final",
                true,
                true,
                &["transport", "status"],
            ),
            "codex.process.start" => ("codex.process.start", true, true, &["originator"]),
            "codex.thread.started" => (
                "codex.thread.started",
                true,
                false,
                &["is_git", "is_worktree"],
            ),
            "codex.startup.phase.duration_ms" => (
                "codex.startup.phase.duration_ms",
                false,
                false,
                &["phase", "status"],
            ),
            "codex.startup_prewarm.duration_ms" => (
                "codex.startup_prewarm.duration_ms",
                false,
                false,
                &["status"],
            ),
            "codex.startup_prewarm.age_at_first_turn_ms" => (
                "codex.startup_prewarm.age_at_first_turn_ms",
                false,
                false,
                &["status"],
            ),
            "codex.thread.skills.enabled_total" => (
                "codex.thread.skills.enabled_total",
                false,
                false,
                &["catalog_surface"],
            ),
            "codex.thread.skills.kept_total" => (
                "codex.thread.skills.kept_total",
                false,
                false,
                &["catalog_surface"],
            ),
            "codex.thread.skills.description_truncated_chars" => (
                "codex.thread.skills.description_truncated_chars",
                false,
                false,
                &["catalog_surface"],
            ),
            "codex.thread.skills.truncated" => (
                "codex.thread.skills.truncated",
                false,
                false,
                &["catalog_surface"],
            ),
            _ => return None,
        };
    Some(Spec {
        name,
        counter,
        global,
        tags,
    })
}

pub(super) fn emit(recorder: &Recorder, name: &str, value: f64, tags: &[(&str, &str)]) {
    let Some(spec) = spec(name) else {
        return;
    };
    let global = Recorder {
        attributes: Vec::new(),
        ..recorder.clone()
    };
    let originator = recorder
        .attributes
        .iter()
        .find(|tag| tag.key.as_str() == "originator")
        .map(|tag| tag.value.to_string())
        .unwrap_or_else(|| "other".to_owned());
    let mut tags: Vec<_> = tags
        .iter()
        .copied()
        .filter(|(key, _)| spec.tags.contains(key))
        .collect();
    if matches!(
        name,
        "codex.multi_agent.spawned"
            | "codex.multi_agent.stop_accepted"
            | "codex.multi_agent.turn.lifecycle"
    ) {
        tags.retain(|(key, _)| !matches!(*key, "originator" | "product_sku"));
        tags.extend([
            ("originator", originator.as_str()),
            (
                "product_sku",
                recorder.product_sku.as_deref().unwrap_or("unknown"),
            ),
        ]);
    }
    let recorder = if spec.global { &global } else { recorder };
    if spec.counter {
        recorder.count(spec.name, value.min(i64::MAX as f64) as u64, &tags);
    } else {
        recorder.sample(spec.name, value, &tags);
    }
}

pub(super) fn import(
    recorder: &Recorder,
    metadata: &RequestMetadata<'_>,
    sample_id: &str,
    seen: &mut LruCache<String, ()>,
) -> Vec<&'static str> {
    let mut provided = Vec::new();
    for (index, event) in metadata
        .fact("events")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(256)
        .enumerate()
    {
        let Some(name) = event["name"].as_str() else {
            continue;
        };
        let Some(spec) = spec(name) else {
            continue;
        };
        let Some(value) = event["value"]
            .as_f64()
            .filter(|value| value.is_finite() && *value >= 0.)
        else {
            continue;
        };
        if (spec.counter || !spec.name.ends_with("_ms")) && value.fract() != 0. {
            continue;
        }
        if !provided.contains(&spec.name) {
            provided.push(spec.name);
        }
        let key = event["id"]
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 256)
            .map(|id| format!("{name}\0{id}"))
            .unwrap_or_else(|| format!("{name}\0{sample_id}\0{index}"));
        if seen.put(key, ()).is_some() {
            continue;
        }
        let values: Vec<_> = spec
            .tags
            .iter()
            .filter_map(|key| {
                let value = event["tags"].get(*key)?;
                let text = match value {
                    Value::String(value) => value.clone(),
                    Value::Bool(value) => value.to_string(),
                    _ => return None,
                };
                Some((
                    *key,
                    if *key == "product_sku" && text != "codex" {
                        "other".to_owned()
                    } else {
                        text
                    },
                ))
            })
            .collect();
        let tags: Vec<_> = values
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect();
        emit(recorder, name, value, &tags);
    }
    provided
}
