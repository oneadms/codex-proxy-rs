//! 为 Codex 出站请求补齐缺失的会话与轮次身份，保留客户端已有字段

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::{
    protocol::responses::CodexResponsesRequest,
    request::{TURN_METADATA_KEYS, derive_conversation_anchor, serialize_ascii_turn_metadata},
};

pub(crate) fn complete_session_metadata(
    request: &mut CodexResponsesRequest,
    client_key_id: &str,
    request_id: &str,
    installation_id: &str,
) {
    // 此处只补出站副本；选号、续接归属和传输恢复键已按原始客户端身份冻结
    let body_metadata = request
        .client_metadata()
        .and_then(|metadata| metadata.get("x-codex-turn-metadata"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let supplied_metadata: Option<Value> = request
        .turn_metadata
        .as_deref()
        .or(body_metadata.as_deref())
        .and_then(|raw| serde_json::from_str(raw).ok());
    let supplied_window_id = request
        .client_metadata()
        .and_then(|metadata| metadata.get("x-codex-window-id"))
        .or_else(|| supplied_metadata.as_ref()?.get("window_id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let anchor = request
        .client_conversation_id
        .clone()
        .or_else(|| request.downstream_websocket_connection_id.clone())
        .or_else(|| request.local_conversation_id.clone())
        .or_else(|| derive_conversation_anchor(request).map(|(_, anchor)| anchor))
        .unwrap_or_else(|| request_id.to_owned());
    let session_id = request
        .client_session_id
        .get_or_insert_with(|| {
            request
                .client_account_session_id
                .clone()
                .or_else(|| request.client_thread_id.clone())
                .unwrap_or_else(|| generated_id("conversation", client_key_id, &anchor))
        })
        .clone();
    let thread_id = request
        .client_thread_id
        .get_or_insert_with(|| session_id.clone())
        .clone();
    let window_id = request
        .codex_window_id
        .get_or_insert_with(|| supplied_window_id.unwrap_or_else(|| format!("{thread_id}:0")))
        .clone();
    let turn_id = request
        .client_turn_id
        .clone()
        .or_else(|| {
            request
                .turn_metadata
                .as_deref()
                .or(body_metadata.as_deref())
                .and_then(gateway_protocol::openai::turn_metadata_turn_id)
        })
        .unwrap_or_else(|| generated_id("turn", client_key_id, request_id));
    if !request.body().contains_key("prompt_cache_key") {
        request.body_mut().insert(
            "prompt_cache_key".to_owned(),
            Value::String(session_id.clone()),
        );
    }
    let defaults = [
        ("installation_id", installation_id),
        ("session_id", session_id.as_str()),
        ("thread_id", thread_id.as_str()),
        ("window_id", window_id.as_str()),
        ("turn_id", turn_id.as_str()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), Value::String(value.to_owned())))
    .collect();
    if let Some(completed) = complete_turn_metadata(
        request
            .turn_metadata
            .as_deref()
            .or(body_metadata.as_deref()),
        &defaults,
    ) {
        request.turn_metadata = Some(completed);
    }
    for (name, value) in &mut request.passthrough_headers {
        if name == "x-codex-turn-metadata"
            && let Ok(raw) = value.to_str()
            && let Some(completed) = complete_turn_metadata(Some(raw), &defaults)
            && let Ok(completed) = reqwest::header::HeaderValue::from_str(&completed)
        {
            *value = completed;
        }
    }
    for key in TURN_METADATA_KEYS {
        if let Some(value) = request.body_mut().get_mut(*key) {
            complete_metadata_value(value, &defaults);
        }
    }
    let header_metadata = request.turn_metadata.clone();
    if let Some(Value::Object(metadata)) = request.body_mut().get_mut("client_metadata") {
        // 官方 client_metadata 单独投影逻辑身份；session-id 可能另作缓存路由，不能反向覆盖
        for (target, source) in [
            ("session_id", "session_id"),
            ("thread_id", "thread_id"),
            ("turn_id", "turn_id"),
            ("x-codex-window-id", "window_id"),
        ] {
            let value = supplied_metadata
                .as_ref()
                .and_then(|metadata| metadata.get(source))
                .filter(|value| value.is_string())
                .unwrap_or(&defaults[source]);
            metadata
                .entry(target.to_owned())
                .or_insert_with(|| value.clone());
        }
        if let Some(raw) = header_metadata {
            metadata
                .entry("x-codex-turn-metadata".to_owned())
                .or_insert(Value::String(raw));
        }
        for key in TURN_METADATA_KEYS {
            if let Some(value) = metadata.get_mut(*key) {
                complete_metadata_value(value, &defaults);
            }
        }
    }
}

fn complete_metadata_value(value: &mut Value, defaults: &Map<String, Value>) {
    match value {
        Value::Object(metadata) => {
            for (key, value) in defaults {
                metadata.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        Value::String(raw) => {
            if let Some(completed) = complete_turn_metadata(Some(raw), defaults) {
                *raw = completed;
            }
        }
        _ => {}
    }
}

fn complete_turn_metadata(raw: Option<&str>, defaults: &Map<String, Value>) -> Option<String> {
    let mut metadata: Map<String, Value> = match raw {
        Some(raw) => serde_json::from_str(raw).ok()?,
        None => Map::new(),
    };
    for (key, value) in defaults {
        metadata.entry(key.clone()).or_insert_with(|| value.clone());
    }
    serialize_ascii_turn_metadata(&Value::Object(metadata))
}

fn generated_id(kind: &str, client_key_id: &str, anchor: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"codex-generated-identity-v1\0");
    for part in [kind, client_key_id, anchor] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    let hash = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    // 内容锚点没有可信的创建时间，沿用 UUIDv4 形状，避免伪造 UUIDv7 时间
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}
