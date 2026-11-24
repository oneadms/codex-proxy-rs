//! 可见工具目录、上下文和 Skills 摘要采样，仅保留定义哈希和尺寸

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    metadata::RequestMetadata,
    record::{Recorder, boolean},
    sanitize_tag,
    turn::number,
};

const EXPOSURES: [&str; 6] = [
    "direct",
    "direct_model_only",
    "deferred",
    "deferred_model_only",
    "code_mode_only",
    "hidden",
];

#[derive(Default)]
pub(super) struct Catalog {
    previous: Option<BTreeMap<String, Tool>>,
}

struct Tool {
    hash: [u8; 32],
    description_bytes: Option<usize>,
    rendered_bytes: usize,
    namespace: String,
    exposure: usize,
}

impl Catalog {
    pub(super) fn observe(&mut self, recorder: &Recorder, metadata: &RequestMetadata<'_>) {
        let mut current = BTreeMap::new();
        for tool in metadata
            .body
            .get("tools")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .take(2048)
        {
            if tool["type"] == "namespace" {
                let namespace = tool["name"].as_str().unwrap_or_default();
                for child in tool["tools"].as_array().into_iter().flatten().take(2048) {
                    add_tool(&mut current, child, namespace);
                }
            } else {
                add_tool(&mut current, tool, "functions");
            }
            if current.len() >= 2048 {
                break;
            }
        }
        let facts = metadata.fact("tools").unwrap_or(&Value::Null);
        let mode = facts["tool_mode"]
            .as_str()
            .filter(|value| matches!(*value, "direct" | "code_mode" | "code_mode_only"))
            .unwrap_or("direct");
        let incremental = facts["incremental_tools"].as_bool().unwrap_or(false);
        for (index, exposure) in EXPOSURES.into_iter().enumerate() {
            let count = facts["registrations"][exposure]
                .as_u64()
                .unwrap_or_else(|| {
                    current
                        .values()
                        .filter(|tool| tool.exposure == index)
                        .count() as u64
                });
            recorder.sample(
                "codex.tools.registered",
                count as f64,
                &[
                    ("exposure", exposure),
                    ("tool_mode", mode),
                    ("incremental_tools", boolean(incremental)),
                ],
            );
        }
        let kind = if self.previous.is_some() {
            "delta"
        } else {
            "snapshot"
        };
        let namespaces = current
            .values()
            .map(|tool| tool.namespace.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        // 请求只有结构化定义时模拟渲染后的目录尺寸，客户端提供的渲染事实优先
        let bytes: usize = current.values().map(|tool| tool.rendered_bytes).sum();
        for stage in ["before", "after"] {
            let tags = [("stage", stage), ("kind", kind)];
            recorder.sample(
                "codex.thread.tools.namespaces_total",
                facts["namespaces_total"][stage]
                    .as_f64()
                    .unwrap_or(namespaces as f64),
                &tags,
            );
            recorder.sample(
                "codex.thread.tools.fragment_bytes",
                facts["fragment_bytes"][stage]
                    .as_f64()
                    .unwrap_or(bytes as f64),
                &tags,
            );
        }
        if incremental && let Some(previous) = &self.previous {
            let mut updated_bytes = 0;
            for (name, tool) in &current {
                let action = match previous.get(name) {
                    None => "added",
                    Some(old) if old.hash != tool.hash => "schema_changed",
                    _ => continue,
                };
                recorder.count(
                    "codex.tools.incremental_updates",
                    1,
                    &[("action", action), ("tool", name)],
                );
                if action == "schema_changed"
                    && let Some(bytes) = tool.description_bytes
                {
                    recorder.sample(
                        "codex.tools.spec_description_bytes",
                        bytes as f64,
                        &[("tool", name)],
                    );
                }
                if mode != "direct" {
                    recorder.count("codex.tools.exec_entry_updates", 1, &[("action", action)]);
                    updated_bytes += tool.rendered_bytes;
                }
            }
            for name in previous.keys().filter(|name| !current.contains_key(*name)) {
                recorder.count(
                    "codex.tools.incremental_updates",
                    1,
                    &[("action", "removed"), ("tool", name)],
                );
                if mode != "direct" {
                    recorder.count(
                        "codex.tools.exec_entry_updates",
                        1,
                        &[("action", "removed")],
                    );
                }
            }
            if updated_bytes > 0 {
                recorder.sample(
                    "codex.tools.exec_update_bytes",
                    number(facts, "exec_update_bytes").unwrap_or(updated_bytes as f64),
                    &[],
                );
            }
        }
        self.previous = Some(current);
        additional_context(recorder, metadata);
    }
}

fn add_tool(tools: &mut BTreeMap<String, Tool>, raw: &Value, namespace: &str) {
    if tools.len() >= 2048 || !matches!(raw["type"].as_str(), Some("function" | "custom")) {
        return;
    }
    let definition = raw.get("function").unwrap_or(raw);
    let Some(name) = definition["name"].as_str() else {
        return;
    };
    let name = sanitize_tag(&format!("{namespace}.{name}"));
    let description_bytes = definition["description"].as_str().map(str::len);
    let exposure = raw["exposure"]
        .as_str()
        .and_then(|value| EXPOSURES.iter().position(|exposure| *exposure == value))
        .unwrap_or_else(|| usize::from(raw["defer_loading"].as_bool() == Some(true)) * 2);
    let encoded = serde_json::to_vec(definition).unwrap_or_default();
    tools.insert(
        name.clone(),
        Tool {
            hash: Sha256::digest(&encoded).into(),
            description_bytes,
            rendered_bytes: name.len() + description_bytes.unwrap_or(0) + 16,
            namespace: sanitize_tag(namespace),
            exposure,
        },
    );
}

fn additional_context(recorder: &Recorder, metadata: &RequestMetadata<'_>) {
    for entry in metadata
        .fact("additional_context")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(256)
    {
        let kind = if entry["kind"] == "untrusted" {
            "untrusted"
        } else {
            "application"
        };
        let before = number(entry, "before_bytes")
            .or_else(|| entry["value"].as_str().map(|value| value.len() as f64));
        let Some(before) = before else {
            continue;
        };
        let after = number(entry, "after_bytes").unwrap_or(before);
        let mut tags = vec![
            ("kind", kind),
            (
                "truncated",
                boolean(entry["truncated"].as_bool().unwrap_or(after < before)),
            ),
        ];
        if let Some(name) = entry["name"].as_str() {
            tags.push(("name", name));
        }
        recorder.count("codex.additional_context", 1, &tags);
        for (stage, bytes) in [("before", before), ("after", after)] {
            let mut size_tags = tags.clone();
            size_tags.push(("stage", stage));
            recorder.sample("codex.additional_context.value_bytes", bytes, &size_tags);
        }
    }
}

pub(super) fn skills(recorder: &Recorder, metadata: &RequestMetadata<'_>) {
    let facts = metadata.fact("skills").unwrap_or(&Value::Null);
    let enabled = facts["enabled_total"]
        .as_u64()
        .or_else(|| {
            facts["entries"]
                .as_array()
                .map(|entries| entries.len() as u64)
        })
        .unwrap_or(0);
    let kept = facts["kept_total"].as_u64().unwrap_or(enabled);
    let surface = facts["catalog_surface"]
        .as_str()
        .unwrap_or("thread_context");
    for (name, value) in [
        ("codex.thread.skills.enabled_total", enabled as f64),
        ("codex.thread.skills.kept_total", kept as f64),
        (
            "codex.thread.skills.description_truncated_chars",
            number(facts, "description_truncated_chars").unwrap_or(0.),
        ),
        (
            "codex.thread.skills.truncated",
            f64::from(u8::from(kept < enabled)),
        ),
    ] {
        recorder.sample(name, value, &[("catalog_surface", surface)]);
    }
}
