//! 聚合同一客户端轮次的采样、实际模型用量和可见关键路径

use std::{
    collections::{BTreeMap, HashSet},
    time::{Duration, Instant},
};

use gateway_core::metering::Decimal;
use serde_json::Value;

use super::{
    metadata::RequestMetadata,
    record::{Recorder, boolean},
};
use crate::transport::usage::{OpenAiBillingUsage, openai_billing_breakdown};

pub(super) struct Turn {
    pub(super) recorder: Recorder,
    pub(super) last_seen: Instant,
    pub(super) finished: bool,
    pub(super) retired: bool,
    pub(super) inflight: usize,
    pub(super) awaiting_tools: bool,
    pub(super) final_response: bool,
    started: Instant,
    ttft: Option<f64>,
    ttfm: Option<f64>,
    sampling_ms: f64,
    tool_calls: u64,
    usage: BTreeMap<String, Usage>,
    responses: HashSet<String>,
    turn_id: Option<String>,
    thread_id: Option<String>,
    effort: String,
    speed: Option<String>,
    facts: Value,
    estimated_usd: Option<Decimal>,
    all_priced: bool,
    observed_usage: bool,
    subagent_kind: Option<String>,
    pending_tools: HashSet<String>,
}

#[derive(Default, Clone)]
pub(super) struct Usage(pub(super) [u64; 6]);

impl Usage {
    pub(super) fn parse(value: &Value) -> Option<Self> {
        value.as_object()?;
        let input = value.get("input_tokens").and_then(Value::as_u64)?;
        let output = value.get("output_tokens").and_then(Value::as_u64)?;
        let cached = value
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let written = value
            .pointer("/input_tokens_details/cache_write_tokens")
            .or_else(|| value.get("cache_write_input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let reasoning = value
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        Some(Self([
            value
                .get("total_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| input.saturating_add(output)),
            input,
            cached,
            written,
            output,
            reasoning,
        ]))
    }

    pub(super) fn emit(&self, recorder: &Recorder, name: &'static str, extra: &[(&str, &str)]) {
        for (token_type, value) in [
            "total",
            "input",
            "cached_input",
            "cache_write_input",
            "output",
            "reasoning_output",
        ]
        .into_iter()
        .zip(self.0)
        {
            let mut tags = extra.to_vec();
            tags.push(("token_type", token_type));
            recorder.sample(name, value.min(i64::MAX as u64) as f64, &tags);
        }
    }
}

impl Turn {
    pub(super) fn new(
        recorder: Recorder,
        metadata: &RequestMetadata<'_>,
        thread_id: Option<String>,
        turn_id: Option<String>,
        subagent_kind: Option<&str>,
    ) -> Self {
        let now = Instant::now();
        if let Some(kind) = subagent_kind {
            super::facts::emit(
                &recorder,
                "codex.multi_agent.turn.lifecycle",
                1.,
                &[("turn_kind", kind), ("status", "started")],
            );
        }
        Self {
            recorder,
            started: now,
            last_seen: now,
            finished: false,
            retired: false,
            inflight: 0,
            awaiting_tools: false,
            final_response: false,
            ttft: None,
            ttfm: None,
            sampling_ms: 0.,
            tool_calls: 0,
            usage: BTreeMap::new(),
            responses: HashSet::new(),
            turn_id,
            thread_id,
            effort: metadata
                .body
                .get("reasoning")
                .and_then(|value| value.get("effort"))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            speed: metadata
                .body
                .get("service_tier")
                .and_then(Value::as_str)
                .map(str::to_owned),
            facts: turn_facts(metadata),
            estimated_usd: Some(Decimal::ZERO),
            all_priced: true,
            observed_usage: false,
            subagent_kind: subagent_kind.map(str::to_owned),
            pending_tools: HashSet::new(),
        }
    }

    pub(super) fn update_facts(&mut self, metadata: &RequestMetadata<'_>) {
        if metadata.fact("turn").is_some() {
            let updates = turn_facts(metadata);
            if let (Some(current), Some(updates)) =
                (self.facts.as_object_mut(), updates.as_object())
            {
                current.extend(updates.clone());
            } else {
                self.facts = updates;
            }
        }
    }

    pub(super) fn output(&mut self, value: &Value) {
        if self.finished {
            return;
        }
        let (token, message) = output_timing(value);
        if token && self.ttft.is_none() {
            self.ttft = Some(self.started.elapsed().as_secs_f64() * 1000.);
        }
        if self.ttfm.is_none() && message {
            self.ttfm = Some(self.started.elapsed().as_secs_f64() * 1000.);
        }
    }

    pub(super) fn response(
        &mut self,
        value: &Value,
        elapsed: Duration,
        fallback_model: &str,
    ) -> bool {
        if self.finished {
            return false;
        }
        let response = &value["response"];
        if let Some(id) = response["id"].as_str() {
            if self.responses.contains(id) {
                return false;
            }
            if self.responses.len() < 4096 {
                self.responses.insert(id.to_owned());
            }
        }
        self.last_seen = Instant::now();
        self.sampling_ms += elapsed.as_secs_f64() * 1000.;
        let model = response["model"].as_str().unwrap_or(fallback_model);
        if let Some(usage) = Usage::parse(&response["usage"]) {
            self.observed_usage = true;
            if let Some(price) = openai_billing_breakdown(
                model,
                OpenAiBillingUsage::new(usage.0[1], usage.0[4], usage.0[2], usage.0[3]),
                response["service_tier"].as_str().or(self.speed.as_deref()),
            ) {
                self.estimated_usd = self
                    .estimated_usd
                    .and_then(|amount| amount.checked_add(price.total_amount().amount()));
            } else {
                self.all_priced = false;
            }
            let total = self.usage.entry(model.to_owned()).or_default();
            for (total, value) in total.0.iter_mut().zip(usage.0) {
                *total = total.saturating_add(value);
            }
        } else {
            self.all_priced = false;
        }
        let output = response["output"].as_array();
        let tools = output.into_iter().flatten().any(|item| {
            matches!(
                item["type"].as_str(),
                Some("function_call" | "custom_tool_call")
            )
        });
        for item in output.into_iter().flatten().filter(|item| {
            matches!(
                item["type"].as_str(),
                Some("function_call" | "custom_tool_call")
            )
        }) {
            if let Some(id) = item["call_id"].as_str()
                && self.pending_tools.len() < 4096
            {
                self.pending_tools.insert(id.to_owned());
            }
        }
        self.awaiting_tools = tools || !self.pending_tools.is_empty();
        let final_answer = output
            .into_iter()
            .flatten()
            .any(|item| item["phase"] == "final_answer");
        let commentary = output
            .into_iter()
            .flatten()
            .any(|item| item["phase"] == "commentary");
        // commentary 仍可能继续采样；缺少 phase 时使用无待执行本地工具的虚拟终点
        self.final_response = self.facts["completed"]
            .as_bool()
            .unwrap_or(final_answer || (!tools && !commentary));
        if let Some(items) = output {
            for item in items {
                let (token, message) = item_timing(item);
                let elapsed = self.started.elapsed().as_secs_f64() * 1000.;
                if token {
                    self.ttft.get_or_insert(elapsed);
                }
                if message {
                    self.ttfm.get_or_insert(elapsed);
                }
            }
        }
        true
    }

    pub(super) fn tool_completed(&mut self, call_id: &str) {
        self.pending_tools.remove(call_id);
        self.awaiting_tools = !self.pending_tools.is_empty();
        if !self.finished {
            self.tool_calls = self.tool_calls.saturating_add(1);
        }
    }

    pub(super) fn finish(&mut self, interrupted: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(kind) = &self.subagent_kind {
            super::facts::emit(
                &self.recorder,
                "codex.multi_agent.turn.lifecycle",
                1.,
                &[
                    ("turn_kind", kind),
                    (
                        "status",
                        if interrupted {
                            "interrupted"
                        } else {
                            "completed"
                        },
                    ),
                ],
            );
        }
        let duration = number(&self.facts, "e2e_duration_ms").unwrap_or_else(|| {
            if interrupted {
                self.last_seen.saturating_duration_since(self.started)
            } else {
                self.started.elapsed()
            }
            .as_secs_f64()
                * 1000.
        });
        self.recorder
            .sample("codex.turn.e2e_duration_ms", duration, &[]);
        for (name, field, observed) in [
            ("codex.turn.ttft.duration_ms", "ttft_ms", self.ttft),
            ("codex.turn.ttfm.duration_ms", "ttfm_ms", self.ttfm),
        ] {
            if let Some(value) = number(&self.facts, field).or(observed) {
                self.recorder.sample(name, value, &[]);
            }
        }
        let tmp_mem = (
            "tmp_mem_enabled",
            boolean(
                self.facts["tmp_mem_enabled"]
                    .as_bool()
                    .or_else(|| self.facts["memory_feature_enabled"].as_bool())
                    .unwrap_or(false),
            ),
        );
        self.recorder.sample(
            "codex.turn.tool.call",
            number(&self.facts, "tool_calls").unwrap_or(self.tool_calls as f64),
            &[tmp_mem],
        );
        // 官方只在整轮终点补缺失 usage 的零样本，不能在每次空响应上创建零样本
        if self.usage.is_empty() {
            Usage::default().emit(&self.recorder, "codex.turn.token_usage", &[tmp_mem]);
        }
        for (model, usage) in &self.usage {
            usage.emit(
                &self.recorder.with_model(model),
                "codex.turn.token_usage",
                &[tmp_mem],
            );
        }
        let cost = match self.facts.get("cost_microusd") {
            Some(value) => value.as_u64(),
            None => (self.all_priced && self.observed_usage)
                .then_some(self.estimated_usd)
                .flatten()
                .and_then(|amount| decimal_microusd(&amount.to_string())),
        };
        if let (Some(cost), Some(turn), Some(thread)) =
            (cost, self.turn_id.as_deref(), self.thread_id.as_deref())
        {
            let mut tags = vec![
                ("turn.id", turn),
                ("conversation.id", thread),
                ("turn.interrupted", boolean(interrupted)),
                ("reasoning_effort", &self.effort),
            ];
            if let Some(speed) = &self.speed {
                tags.push(("speed", speed));
            }
            // 本地价目仅作为不含网关倍率的估值；客户端给出的结算值优先
            self.recorder.count("codex.turn.cost_microusd", cost, &tags);
        }
        self.recorder.count(
            "codex.turn.network_proxy",
            1,
            &[
                (
                    "active",
                    boolean(
                        self.facts["network_proxy_active"]
                            .as_bool()
                            .unwrap_or(false),
                    ),
                ),
                tmp_mem,
            ],
        );
        let feature = self.facts["memory_feature_enabled"]
            .as_bool()
            .unwrap_or(false);
        let config = self.facts["config_use_memories"].as_bool().unwrap_or(false);
        self.recorder.count(
            "codex.turn.memory",
            1,
            &[
                ("read_allowed", boolean(feature && config)),
                ("feature_enabled", boolean(feature)),
                ("config_use_memories", boolean(config)),
                (
                    "has_citations",
                    boolean(
                        self.facts["has_memory_citations"]
                            .as_bool()
                            .unwrap_or(false),
                    ),
                ),
            ],
        );
        self.recorder.count(
            "codex.turn.unified_exec.running_processes",
            self.facts["running_processes"].as_u64().unwrap_or(0),
            &[],
        );
        self.critical_path(duration);
    }

    fn critical_path(&self, duration: f64) {
        let path = &self.facts["critical_path"];
        let complete = path.is_object();
        let sku = self.recorder.product_sku.as_deref().unwrap_or("unknown");
        let tags = [
            ("product_sku", sku),
            ("reasoning_effort", self.effort.as_str()),
        ];
        let mut capture = tags.to_vec();
        // 代理只覆盖采样阶段，默认虚拟关键路径把其余时段归入 unknown
        let status = path["status"].as_str().unwrap_or("complete");
        capture.push(("capture_status", status));
        self.recorder
            .count("codex.turn.critical_path.capture", 1, &capture);
        if status != "complete" {
            return;
        }
        self.recorder.sample(
            "codex.turn.critical_path.duration_ms",
            number(path, "duration_ms").unwrap_or(duration),
            &tags,
        );
        for component in [
            "inference",
            "auto_review",
            "tools",
            "user_wait",
            "overhead",
            "unknown",
        ] {
            let value = if complete {
                number(path, component).unwrap_or(0.)
            } else {
                match component {
                    "inference" => self.sampling_ms.min(duration),
                    "unknown" => (duration - self.sampling_ms).max(0.),
                    _ => 0.,
                }
            };
            let mut component_tags = tags.to_vec();
            component_tags.push(("component", component));
            self.recorder.sample(
                "codex.turn.critical_path.component_ms",
                value,
                &component_tags,
            );
        }
    }
}

pub(super) fn number(value: &Value, field: &str) -> Option<f64> {
    value
        .get(field)?
        .as_f64()
        .filter(|value| value.is_finite() && *value >= 0.0)
}

pub(super) fn output_timing(value: &Value) -> (bool, bool) {
    let kind = value["type"].as_str().unwrap_or_default();
    if matches!(
        kind,
        "response.output_text.delta"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
    ) {
        return (true, kind == "response.output_text.delta");
    }
    if !matches!(
        kind,
        "response.output_item.added" | "response.output_item.done"
    ) {
        return (false, false);
    }
    item_timing(&value["item"])
}

fn item_timing(item: &Value) -> (bool, bool) {
    let text = |field: &str| {
        item[field]
            .as_array()
            .into_iter()
            .flatten()
            .any(|part| part["text"].as_str().is_some_and(|text| !text.is_empty()))
    };
    match item["type"].as_str() {
        Some("message") => (
            item["role"] == "assistant" && text("content"),
            item["role"] == "assistant",
        ),
        Some("reasoning") => (text("summary") || text("content"), false),
        Some(
            "function_call"
            | "custom_tool_call"
            | "local_shell_call"
            | "tool_search_call"
            | "web_search_call"
            | "image_generation_call"
            | "compaction"
            | "context_compaction",
        ) => (true, false),
        _ => (false, false),
    }
}

fn turn_facts(metadata: &RequestMetadata<'_>) -> Value {
    let Some(value) = metadata.fact("turn") else {
        return Value::Null;
    };
    let mut fields = serde_json::Map::new();
    for key in [
        "completed",
        "e2e_duration_ms",
        "ttft_ms",
        "ttfm_ms",
        "tool_calls",
        "cost_microusd",
        "tmp_mem_enabled",
        "network_proxy_active",
        "memory_feature_enabled",
        "config_use_memories",
        "has_memory_citations",
        "running_processes",
    ] {
        if let Some(value) = value
            .get(key)
            .filter(|value| value.is_number() || value.is_boolean() || value.is_null())
        {
            fields.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(path) = value["critical_path"].as_object() {
        fields.insert(
            "critical_path".to_owned(),
            Value::Object(
                path.iter()
                    .filter(|(key, value)| {
                        matches!(
                            key.as_str(),
                            "duration_ms"
                                | "inference"
                                | "auto_review"
                                | "tools"
                                | "user_wait"
                                | "overhead"
                                | "unknown"
                        ) && value.is_number()
                            || key.as_str() == "status"
                                && matches!(
                                    value.as_str(),
                                    Some("complete" | "incomplete" | "disabled")
                                )
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ),
        );
    }
    Value::Object(fields)
}

fn decimal_microusd(value: &str) -> Option<u64> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if !fraction.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    let digits = fraction
        .bytes()
        .take(6)
        .fold(0_u64, |result, digit| result * 10 + u64::from(digit - b'0'));
    whole
        .parse::<u64>()
        .ok()?
        .checked_mul(1_000_000)?
        .checked_add(digits * 10_u64.pow(6_usize.saturating_sub(fraction.len()) as u32))?
        .checked_add(u64::from(
            fraction
                .as_bytes()
                .get(6)
                .is_some_and(|digit| *digit >= b'5'),
        ))
}
