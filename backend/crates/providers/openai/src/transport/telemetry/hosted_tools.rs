//! 从 Responses 实际工具生命周期生成官方网页搜索与图片生成 analytics 事件

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use gateway_protocol::openai::{codex_account_session_id, codex_account_thread_id, codex_turn_id};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde_json::{Map, Value, json};

use super::{
    CodexBackendClient, CodexWireProfile,
    analytics::{Analytics, Event, Route},
};
use crate::transport::{
    CodexRequestContext, endpoints::endpoint_url, headers::build_codex_profile_headers,
    profile::identity::app_server_client_identity, request::TURN_METADATA_KEYS,
};

const MAX_STARTED_ITEMS: usize = 256;

pub(super) struct HostedTools {
    analytics: Arc<Analytics>,
    route: Arc<Route>,
    base: Map<String, Value>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    response_id: Option<String>,
    started: HashMap<String, (&'static str, u64)>,
    terminal: bool,
}

impl HostedTools {
    pub(super) fn new(
        analytics: Arc<Analytics>,
        client: &CodexBackendClient,
        profile: &CodexWireProfile,
        body: &Map<String, Value>,
        headers: &HeaderMap,
        context: CodexRequestContext<'_>,
    ) -> Option<Self> {
        if context.authorization.is_empty() {
            return None;
        }
        // 使用完成账号隔离和隐私规则后的出站身份，不能回退到本地原始 ID
        let identity: Map<String, Value> = [
            ("session_id", "session-id"),
            ("thread_id", "thread-id"),
            ("turn_metadata", "x-codex-turn-metadata"),
        ]
        .into_iter()
        .filter_map(|(field, header)| {
            Some((
                field.to_owned(),
                Value::String(headers.get(header)?.to_str().ok()?.to_owned()),
            ))
        })
        .collect();
        let thread_id = codex_account_thread_id(body, &identity)?;
        let session_id = codex_account_session_id(body, &identity)?;
        let turn_id = codex_turn_id(body, &identity)?;
        let body_metadata = TURN_METADATA_KEYS
            .iter()
            .find_map(|key| body.get(*key))
            .or_else(|| {
                TURN_METADATA_KEYS
                    .iter()
                    .find_map(|key| body.get("client_metadata")?.get(*key))
            })
            .and_then(|value| match value {
                Value::String(raw) => serde_json::from_str::<Value>(raw).ok(),
                Value::Object(_) => Some(value.clone()),
                _ => None,
            });
        let header_metadata = identity
            .get("turn_metadata")
            .and_then(Value::as_str)
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
        // 帧内元数据优先于握手快照，显式 null 不回退到旧线程或旧轮次的值
        let request_sources = [
            body.get("client_metadata").and_then(Value::as_object),
            Some(body),
            body_metadata.as_ref().and_then(Value::as_object),
            header_metadata.as_ref().and_then(Value::as_object),
        ];
        let request_value = |field: &str, alias: &str| {
            request_sources
                .iter()
                .flatten()
                .find_map(|source| source.get(field).or_else(|| source.get(alias)))
        };
        let mut auth_headers = build_codex_profile_headers(profile).ok()?;
        let mut authorization = HeaderValue::from_str(context.authorization).ok()?;
        authorization.set_sensitive(true);
        auth_headers.insert(AUTHORIZATION, authorization);
        if let Some(account_id) = context.account_id {
            auth_headers.insert(
                "chatgpt-account-id",
                HeaderValue::from_str(account_id).ok()?,
            );
        }
        if let Some(product) = headers.get("x-openai-product-sku") {
            auth_headers.insert("x-openai-product-sku", product.clone());
        }
        let client_info = auth_headers
            .get(USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .and_then(app_server_client_identity);
        let experimental_api_enabled = request_sources
            .iter()
            .flatten()
            .find_map(|source| {
                source
                    .get("experimental_api_enabled")
                    .or_else(|| source.get("experimentalApiEnabled"))
                    .or_else(|| source.get("capabilities")?.get("experimentalApi"))
            })
            .cloned()
            // 官方 TUI 与 Exec 的初始化声明均启用此能力，未知入口保留未知
            .unwrap_or_else(|| {
                json!(
                    client_info
                        .filter(|(name, _)| matches!(*name, "codex-tui" | "codex_exec"))
                        .map(|_| true)
                )
            });
        let base = json!({
            "thread_id": thread_id,
            "session_id": session_id,
            "turn_id": turn_id,
            "root_turn_id": request_value("root_turn_id", "rootTurnId"),
            "cell_id": null,
            "parent_call_id": null,
            "subsequent_response_id": null,
            "app_server_client": {
                "product_client_id": profile.originator,
                "client_name": client_info.map(|(name, _)| name),
                "client_version": client_info.map(|(_, version)| version),
                "rpc_transport": "in_process",
                "experimental_api_enabled": experimental_api_enabled
            },
            "runtime": {
                "codex_rs_version": profile.codex_version,
                "runtime_os": match profile.os_type.as_str() {
                    "Mac OS" | "macos" | "Darwin" => "macos",
                    "Linux" | "linux" => "linux",
                    "Windows" | "windows" => "windows",
                    other => other,
                },
                "runtime_os_version": profile.os_version,
                "runtime_arch": if profile.arch == "arm64" { "aarch64" } else { &profile.arch }
            },
            "thread_source": request_value("thread_source", "threadSource"),
            "subagent_source": request_value("subagent_source", "subagentSource"),
            "parent_thread_id": headers.get("x-codex-parent-thread-id").and_then(|value| value.to_str().ok()),
            "tool_event_type": "model_tool_call",
            "execution_duration_ms": null,
            "review_count": 0,
            "guardian_review_count": 0,
            "user_review_count": 0,
            "final_approval_outcome": "unknown",
            "requested_additional_permissions": false,
            "requested_network_access": false
        });
        Some(Self {
            analytics,
            route: Arc::new(Route {
                client: client.client.clone(),
                endpoint: endpoint_url(&client.base_url, "/codex/analytics-events/events"),
                headers: auth_headers,
                scope: format!(
                    "{}\0{}",
                    client.egress_key,
                    context.account_id.unwrap_or_default()
                ),
            }),
            base: base.as_object()?.clone(),
            state: Mutex::new(State::default()),
        })
    }

    pub(super) fn observe(&self, value: &Value) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.terminal {
            return;
        }
        match value.get("type").and_then(Value::as_str) {
            Some("response.created" | "response.in_progress") => {
                if let Some(id) = value.pointer("/response/id").and_then(Value::as_str) {
                    state.response_id = Some(id.to_owned());
                }
            }
            Some("response.output_item.added") => {
                let Some((id, kind)) = hosted_item(value) else {
                    return;
                };
                if state.started.len() < MAX_STARTED_ITEMS {
                    state
                        .started
                        .entry(id.to_owned())
                        .or_insert((kind, now_ms()));
                }
            }
            Some("response.output_item.done") => {
                let Some((id, kind)) = hosted_item(value) else {
                    return;
                };
                let Some((started_kind, started_at)) = state.started.remove(id) else {
                    return;
                };
                if kind != started_kind {
                    return;
                }
                let completed_at = now_ms();
                let item = &value["item"];
                let mut params = self.base.clone();
                params.insert("item_id".into(), json!(id));
                params.insert("originating_response_id".into(), json!(state.response_id));
                params.insert("started_at_ms".into(), json!(started_at));
                params.insert("completed_at_ms".into(), json!(completed_at));
                params.insert(
                    "duration_ms".into(),
                    json!(completed_at.checked_sub(started_at)),
                );
                let event_type = if kind == "web_search_call" {
                    web_search_params(&mut params, item);
                    "codex_web_search_event"
                } else {
                    image_generation_params(&mut params, item);
                    "codex_image_generation_event"
                };
                self.analytics.enqueue(
                    Arc::clone(&self.route),
                    Event {
                        event_type,
                        event_params: Value::Object(params),
                    },
                );
            }
            Some("response.completed" | "response.failed" | "response.incomplete") => {
                state.terminal = true;
                state.started.clear();
            }
            _ => {}
        }
    }
}

fn hosted_item(value: &Value) -> Option<(&str, &'static str)> {
    let item = value.get("item")?;
    let kind = match item.get("type")?.as_str()? {
        "web_search_call" => "web_search_call",
        "image_generation_call" => "image_generation_call",
        _ => return None,
    };
    let id = item.get("id")?.as_str()?;
    (!id.trim().is_empty()).then_some((id, kind))
}

fn web_search_params(params: &mut Map<String, Value>, item: &Value) {
    let action = &item["action"];
    let action_kind = match action.get("type").and_then(Value::as_str) {
        Some("search") => "search",
        Some("open_page") => "open_page",
        Some("find_in_page") => "find_in_page",
        _ => "other",
    };
    // 只计算查询存在性和数量，不保留搜索词、URL 或页面查找内容
    let query_present = match action_kind {
        "search" => {
            if let Some(query) = action
                .get("query")
                .and_then(Value::as_str)
                .filter(|query| !query.is_empty())
            {
                !query.trim().is_empty()
            } else {
                let queries = action.get("queries").and_then(Value::as_array);
                let first = queries
                    .and_then(|queries| queries.first())
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                !first.trim().is_empty()
                    || (!first.is_empty() && queries.is_some_and(|queries| queries.len() > 1))
            }
        }
        "open_page" => action
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(|url| !url.trim().is_empty()),
        "find_in_page" => {
            action.get("pattern").is_some_and(Value::is_string)
                || action
                    .get("url")
                    .and_then(Value::as_str)
                    .is_some_and(|url| !url.trim().is_empty())
        }
        _ => false,
    };
    let query_count = (action_kind == "search")
        .then(|| {
            action
                .get("queries")
                .and_then(Value::as_array)
                .map(Vec::len)
                .or_else(|| {
                    action
                        .get("query")
                        .filter(|value| value.is_string())
                        .map(|_| 1)
                })
        })
        .flatten();
    params.extend([
        ("tool_name".into(), json!("web_search")),
        ("web_search_action".into(), json!(action_kind)),
        ("query_present".into(), json!(query_present)),
        ("query_count".into(), json!(query_count)),
        ("terminal_status".into(), json!("completed")),
        ("failure_kind".into(), Value::Null),
    ]);
}

fn image_generation_params(params: &mut Map<String, Value>, item: &Value) {
    let failed = matches!(
        item.get("status").and_then(Value::as_str),
        Some("failed" | "error")
    );
    params.extend([
        ("tool_name".into(), json!("image_generation")),
        (
            "revised_prompt_present".into(),
            json!(item.get("revised_prompt").is_some_and(Value::is_string)),
        ),
        ("saved_path_present".into(), json!(false)),
        ("transparent_background".into(), Value::Null),
        ("imagegen_request_id".into(), Value::Null),
        ("generation_id".into(), Value::Null),
        (
            "terminal_status".into(),
            json!(if failed { "failed" } else { "completed" }),
        ),
        ("failure_kind".into(), json!(failed.then_some("tool_error"))),
    ]);
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
