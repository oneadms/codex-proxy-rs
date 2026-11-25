//! 按账号固定派生设备身份，只改写协议身份容器和设备请求头

use std::{fmt, ops::Range};

use bytes::Bytes;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Deserializer, de::MapAccess, de::Visitor};
use serde_json::{Value, value::RawValue};
use sha2::{Digest, Sha256};

use super::{client::CodexBackendClient, request::serialize_ascii_turn_metadata};

pub(super) fn account_device_id(account_id: &str) -> String {
    // 设备身份只依赖账号记录，不使用客户端值、Key、UA、凭据版本或进程状态
    let mut digest = Sha256::new();
    digest.update(b"codex-account-device-v1\0");
    digest.update(account_id.as_bytes());
    let hash = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes).to_string()
}

impl CodexBackendClient {
    pub(super) fn scope_device_body(&self, body: &mut Value) {
        if let Some(id) = &self.device_id {
            scope_value(body, id);
        }
    }

    pub(super) fn scope_raw_device_body(&self, body: Bytes) -> Bytes {
        let Some(id) = &self.device_id else {
            return body;
        };
        scope_json(body, id)
    }

    pub(super) fn scope_device_headers(&self, headers: &mut HeaderMap) {
        let Some(id) = &self.device_id else {
            return;
        };
        for name in headers.keys().cloned().collect::<Vec<_>>() {
            if is_device_field(name.as_str()) {
                headers.insert(name, HeaderValue::from_str(id).expect("device UUID"));
            } else if name == "x-codex-turn-metadata" {
                let values = headers
                    .get_all(&name)
                    .iter()
                    .map(|value| {
                        let scoped = scope_json(Bytes::copy_from_slice(value.as_bytes()), id);
                        HeaderValue::from_bytes(&scoped).expect("scoped turn metadata")
                    })
                    .collect::<Vec<_>>();
                headers.remove(&name);
                for value in values {
                    headers.append(name.clone(), value);
                }
            }
        }
    }
}

fn is_device_field(name: &str) -> bool {
    matches!(
        name.strip_prefix("ws_request_header_").unwrap_or(name),
        "device_id"
            | "deviceId"
            | "deviceid"
            | "device-id"
            | "x-device-id"
            | "x-codex-device-id"
            | "oa-device-id"
            | "oai-device-id"
            | "oai-did"
            | "machine_id"
            | "machineId"
            | "machineid"
            | "machine-id"
            | "x-machine-id"
    )
}

fn is_identity_container(name: &str) -> bool {
    matches!(
        name,
        "client_metadata"
            | "metadata"
            | "turnMetadata"
            | "turn_metadata"
            | "x-codex-turn-metadata"
            | "telemetry"
            | "runtime"
    )
}

fn scope_value(value: &mut Value, id: &str) {
    match value {
        Value::Object(object) => {
            for (name, value) in object {
                if is_device_field(name) {
                    *value = Value::String(id.to_owned());
                } else if is_identity_container(name) {
                    scope_value(value, id);
                }
            }
        }
        Value::String(raw) => {
            let scoped = scope_json(Bytes::copy_from_slice(raw.as_bytes()), id);
            if scoped.as_ref() != raw.as_bytes() {
                *raw = String::from_utf8(scoped.into()).expect("scoped JSON is UTF-8");
            }
        }
        _ => {}
    }
}

fn scope_json(body: Bytes, id: &str) -> Bytes {
    let mut changes = Vec::new();
    collect_changes(&body, body.as_ptr() as usize, id, &mut changes);
    if changes.is_empty() {
        return body;
    }
    // 只替换设备字段的值，保留独立端点正文的空白、重复业务键及其他原始字节
    let mut scoped = Vec::with_capacity(body.len());
    let mut cursor = 0;
    for (range, replacement) in changes {
        scoped.extend_from_slice(&body[cursor..range.start]);
        scoped.extend_from_slice(replacement.as_bytes());
        cursor = range.end;
    }
    scoped.extend_from_slice(&body[cursor..]);
    scoped.into()
}

fn collect_changes(bytes: &[u8], base: usize, id: &str, changes: &mut Vec<(Range<usize>, String)>) {
    let Ok(Fields(fields)) = serde_json::from_slice::<Fields<'_>>(bytes) else {
        return;
    };
    for (name, raw) in fields {
        let replacement = if is_device_field(&name) {
            Some(format!("\"{id}\""))
        } else if is_identity_container(&name) {
            if raw.get().starts_with('{') {
                collect_changes(raw.get().as_bytes(), base, id, changes);
                None
            } else if let Ok(decoded) = serde_json::from_str::<String>(raw.get()) {
                let scoped = scope_json(Bytes::copy_from_slice(decoded.as_bytes()), id);
                (scoped.as_ref() != decoded.as_bytes())
                    .then(|| {
                        serialize_ascii_turn_metadata(&Value::String(
                            String::from_utf8(scoped.into()).expect("scoped JSON is UTF-8"),
                        ))
                    })
                    .flatten()
            } else {
                None
            }
        } else {
            None
        };
        if let Some(replacement) = replacement {
            let start = raw.get().as_ptr() as usize - base;
            changes.push((start..start + raw.get().len(), replacement));
        }
    }
}

// 保留重复身份字段的每次出现，避免同名键中的客户端设备值留在出站正文
struct Fields<'a>(Vec<(String, &'a RawValue)>);

impl<'de> Deserialize<'de> for Fields<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(FieldsVisitor)
    }
}

struct FieldsVisitor;

impl<'de> Visitor<'de> for FieldsVisitor {
    type Value = Fields<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields = Vec::new();
        while let Some(entry) = map.next_entry()? {
            fields.push(entry);
        }
        Ok(Fields(fields))
    }
}
