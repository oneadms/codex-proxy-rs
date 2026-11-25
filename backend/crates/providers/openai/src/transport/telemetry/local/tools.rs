//! 将已观察到的工具调用与后续结果关联，不把模型提出调用当成执行完成

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Instant,
};

use lru::LruCache;
use serde_json::Value;

use super::super::{
    facts,
    metadata::RequestMetadata,
    record::{Recorder, boolean},
    turn::{Turn, number},
};

pub(in super::super) struct Tools {
    pending: LruCache<String, Call>,
    completed: LruCache<String, ()>,
    goal_status: Option<String>,
    goal_started: Option<Instant>,
    goal_tokens: u64,
}

struct Call {
    name: String,
    started: Instant,
    turn: Arc<Mutex<Turn>>,
    tty: bool,
    fork_mode: &'static str,
    history_mode: &'static str,
    effort: String,
    status: Option<String>,
}

impl Tools {
    pub(in super::super) fn new() -> Self {
        Self {
            pending: LruCache::new(NonZeroUsize::new(256).unwrap()),
            completed: LruCache::new(NonZeroUsize::new(4096).unwrap()),
            goal_status: None,
            goal_started: None,
            goal_tokens: 0,
        }
    }

    pub(in super::super) fn output(&mut self, response: &Value, turn: &Arc<Mutex<Turn>>) {
        if self.goal_status.as_deref() == Some("active")
            && let Some(usage) = super::super::turn::Usage::parse(&response["usage"])
        {
            self.goal_tokens = self.goal_tokens.saturating_add(usage.0[0]);
        }
        for item in response["output"]
            .as_array()
            .into_iter()
            .flatten()
            .take(256)
        {
            if !matches!(
                item["type"].as_str(),
                Some("function_call" | "custom_tool_call")
            ) {
                continue;
            }
            let (Some(id), Some(name)) = (item["call_id"].as_str(), item["name"].as_str()) else {
                continue;
            };
            if self.completed.contains(id) || self.pending.contains(id) {
                continue;
            }
            let arguments = item["arguments"]
                .as_str()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .unwrap_or(Value::Null);
            self.pending.put(
                id.to_owned(),
                Call {
                    name: name.to_owned(),
                    started: Instant::now(),
                    turn: Arc::clone(turn),
                    tty: arguments["tty"].as_bool().unwrap_or(false),
                    fork_mode: if arguments["fork_context"].as_bool() == Some(true) {
                        "all"
                    } else {
                        "none"
                    },
                    history_mode: "legacy",
                    effort: arguments["reasoning_effort"]
                        .as_str()
                        .unwrap_or("unknown")
                        .to_owned(),
                    status: arguments["status"].as_str().map(str::to_owned),
                },
            );
        }
    }

    pub(in super::super) fn input(&mut self, recorder: &Recorder, metadata: &RequestMetadata<'_>) {
        for item in metadata
            .body
            .get("input")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if !matches!(
                item["type"].as_str(),
                Some("function_call_output" | "custom_tool_call_output")
            ) {
                continue;
            }
            let Some(id) = item["call_id"].as_str() else {
                continue;
            };
            let Some(call) = self.pending.pop(id) else {
                continue;
            };
            if self.completed.put(id.to_owned(), ()).is_some() {
                continue;
            }
            // 回传即结束等待；执行成败可能只存在于客户端未序列化的内部状态
            call.turn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tool_completed(id);
            let decoded = item["output"]
                .as_str()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
            let result = decoded.as_ref().unwrap_or(&item["output"]);
            let success = if result["is_error"].as_bool() == Some(true)
                || result["success"].as_bool() == Some(false)
                || result.get("error").is_some_and(|error| !error.is_null())
            {
                Some(false)
            } else {
                result["success"]
                    .as_bool()
                    .or_else(|| result["is_error"].as_bool().map(|error| !error))
            };
            let Some(success) = success else {
                continue;
            };
            // 回传时延包含客户端调度和传输，只有明确 duration_ms 才是客户端实测耗时
            let elapsed = number(result, "duration_ms")
                .unwrap_or_else(|| call.started.elapsed().as_secs_f64() * 1000.);
            let tags = [("tool", call.name.as_str()), ("success", boolean(success))];
            let mut tool_tags = tags.to_vec();
            if let Some(sku) = &recorder.product_sku {
                tool_tags.push(("product_sku", sku));
            }
            facts::emit(recorder, "codex.tool.call", 1., &tool_tags);
            facts::emit(recorder, "codex.tool.call.duration_ms", elapsed, &tool_tags);
            let short = call.name.rsplit(['.', ':']).next().unwrap_or(&call.name);
            let short = short.rsplit("__").next().unwrap_or(short);
            match short {
                "exec_command" => {
                    let mut exec_tags = vec![("success", boolean(success))];
                    if let Some(sku) = &recorder.product_sku {
                        exec_tags.push(("product_sku", sku));
                    }
                    facts::emit(recorder, "codex.exec_command.calls", 1., &exec_tags);
                    facts::emit(
                        recorder,
                        "codex.exec_command.duration_total_ms",
                        elapsed.floor(),
                        &exec_tags,
                    );
                    facts::emit(
                        recorder,
                        "codex.tool.unified_exec",
                        1.,
                        &[("tty", boolean(call.tty))],
                    );
                    facts::emit(
                        recorder,
                        "exec_server_client_requests_total",
                        1.,
                        &[
                            ("method", "process/start"),
                            ("outcome", if success { "success" } else { "error" }),
                        ],
                    );
                }
                "spawn_agent" => {
                    let tags = [("fork_mode", call.fork_mode), ("multi_agent_version", "v1")];
                    if success {
                        facts::emit(recorder, "codex.multi_agent.spawned", 1., &tags);
                        facts::emit(
                            recorder,
                            "codex.multi_agent.spawn.phase.duration_ms",
                            elapsed,
                            &[
                                ("fork_mode", call.fork_mode),
                                ("history_mode", call.history_mode),
                                ("multi_agent_version", "v1"),
                                ("phase", "total"),
                            ],
                        );
                    } else {
                        facts::emit(
                            recorder,
                            "codex.multi_agent.spawn.failure",
                            1.,
                            &[
                                ("fork_mode", call.fork_mode),
                                ("multi_agent_version", "v1"),
                                ("reason", "spawn_failed"),
                                ("detail", "unknown"),
                                ("error_kind", "unknown"),
                            ],
                        );
                    }
                }
                "close_agent" | "interrupt_agent" if success => facts::emit(
                    recorder,
                    "codex.multi_agent.stop_accepted",
                    1.,
                    &[
                        ("caller", "root"),
                        (
                            "action",
                            if short == "close_agent" {
                                "close"
                            } else {
                                "interrupt"
                            },
                        ),
                        ("previous_status", "unknown"),
                    ],
                ),
                "create_goal" | "update_goal" | "get_goal" if success => {
                    self.goal(recorder, short, &call, result)
                }
                "install_plugin" | "install_connector" if success => {
                    let tool_type = if short == "install_plugin" {
                        "plugin"
                    } else {
                        "connector"
                    };
                    facts::emit(
                        recorder,
                        "codex.plugins.install_elicitation.sent",
                        1.,
                        &[("tool_type", tool_type)],
                    );
                    facts::emit(
                        recorder,
                        "codex.plugins.install_suggestion",
                        1.,
                        &[
                            ("tool_type", tool_type),
                            ("response_action", "accept"),
                            ("completed", "true"),
                        ],
                    );
                }
                _ => {}
            }
        }
    }

    fn goal(&mut self, recorder: &Recorder, tool: &str, call: &Call, result: &Value) {
        let goal = result.get("goal").unwrap_or(result);
        if goal.is_null() {
            self.goal_status = None;
            self.goal_started = None;
            self.goal_tokens = 0;
            return;
        }
        let status = goal["status"]
            .as_str()
            .or(call.status.as_deref())
            .unwrap_or("active");
        if tool == "create_goal" {
            facts::emit(
                recorder,
                "codex.goal.created",
                1.,
                &[("reasoning_effort", &call.effort)],
            );
            self.goal_started = Some(Instant::now());
            self.goal_tokens = 0;
        }
        if self.goal_status.as_deref() != Some(status) {
            let name = match status {
                "complete" | "completed" => Some("codex.goal.completed"),
                "blocked" => Some("codex.goal.blocked"),
                "usage_limited" => Some("codex.goal.usage_limited"),
                "budget_limited" => Some("codex.goal.budget_limited"),
                "active"
                    if matches!(
                        self.goal_status.as_deref(),
                        Some("paused" | "blocked" | "usage_limited")
                    ) =>
                {
                    Some("codex.goal.resumed")
                }
                _ => None,
            };
            if let Some(name) = name {
                facts::emit(recorder, name, 1., &[]);
                if status != "active" {
                    let tags = [(
                        "status",
                        if status == "completed" {
                            "complete"
                        } else {
                            status
                        },
                    )];
                    facts::emit(
                        recorder,
                        "codex.goal.token_count",
                        number(goal, "tokens_used")
                            .or_else(|| number(goal, "tokensUsed"))
                            .unwrap_or(self.goal_tokens as f64),
                        &tags,
                    );
                    facts::emit(
                        recorder,
                        "codex.goal.duration_s",
                        number(goal, "time_used_seconds")
                            .or_else(|| number(goal, "timeUsedSeconds"))
                            .unwrap_or_else(|| {
                                self.goal_started
                                    .map_or(0., |start| start.elapsed().as_secs_f64().floor())
                            }),
                        &tags,
                    );
                }
            }
            self.goal_status = Some(status.to_owned());
        }
    }
}
