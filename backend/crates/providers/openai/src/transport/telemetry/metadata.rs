//! 本次 Responses 请求的遥测元数据来源与优先级

use reqwest::header::HeaderMap;
use serde_json::{Map, Value};

use crate::transport::request::TURN_METADATA_KEYS;

pub(super) struct RequestMetadata<'a> {
    pub(super) body: &'a Map<String, Value>,
    frame: Option<Value>,
    header: Option<Value>,
    decoded_telemetry: Option<Value>,
}

impl<'a> RequestMetadata<'a> {
    pub(super) fn new(body: &'a Map<String, Value>, headers: &HeaderMap) -> Self {
        let frame = TURN_METADATA_KEYS
            .iter()
            .find_map(|key| body.get(*key))
            .or_else(|| {
                TURN_METADATA_KEYS
                    .iter()
                    .find_map(|key| body.get("client_metadata")?.get(*key))
            })
            .and_then(|value| match value {
                Value::String(raw) => serde_json::from_str(raw).ok(),
                Value::Object(_) => Some(value.clone()),
                _ => None,
            });
        let header = headers
            .get("x-codex-turn-metadata")
            .and_then(|value| value.to_str().ok())
            .and_then(|raw| serde_json::from_str(raw).ok());
        let mut metadata = Self {
            body,
            frame,
            header,
            decoded_telemetry: None,
        };
        // 官方 client_metadata 的值为字符串，遥测结构在此解码后共用来源优先级
        metadata.decoded_telemetry = metadata
            .get("telemetry", "telemetry")
            .and_then(Value::as_str)
            .and_then(|raw| serde_json::from_str(raw).ok());
        metadata
    }

    /// 帧内字段优先于握手快照，显式 null 阻止旧线程或旧轮次回填
    pub(super) fn sources(&self) -> impl DoubleEndedIterator<Item = &Map<String, Value>> {
        [
            self.body.get("client_metadata").and_then(Value::as_object),
            Some(self.body),
            self.frame.as_ref().and_then(Value::as_object),
            self.header.as_ref().and_then(Value::as_object),
        ]
        .into_iter()
        .flatten()
    }

    pub(super) fn get(&self, field: &str, alias: &str) -> Option<&Value> {
        self.sources().find_map(|source| {
            source.get(field).or_else(|| source.get(alias)).or_else(|| {
                match field {
                    "parent_thread_id" => source.get("x-codex-parent-thread-id"),
                    // 官方 Responses metadata 使用 subagent_kind，analytics 使用 subagent_source
                    "subagent_source" => source
                        .get("subagent_kind")
                        .or_else(|| source.get("subagentKind"))
                        .or_else(|| source.get("x-openai-subagent")),
                    "experimental_api_enabled" => {
                        source.get("capabilities")?.get("experimentalApi")
                    }
                    _ => None,
                }
            })
        })
    }

    pub(super) fn fact(&self, field: &str) -> Option<&Value> {
        let value = self.get("telemetry", "telemetry")?;
        if value.is_string() {
            self.decoded_telemetry.as_ref()?.get(field)
        } else {
            value.get(field)
        }
    }

    pub(super) fn text(&self, field: &str, alias: &str) -> Option<&str> {
        self.get(field, alias)?
            .as_str()
            .filter(|value| !value.is_empty())
    }
}
