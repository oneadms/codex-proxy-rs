//! 有界读取 Mihomo 订阅，只提取节点配置并排除链式出口引用

use futures::StreamExt as _;
use reqwest::{Client, Url};
use serde_json::{Map, Value};
use yaml_rust2::{Yaml, YamlLoader};

use super::{MAX_SUBSCRIPTION_BYTES, MAX_SUBSCRIPTION_NODES, MihomoError, MihomoNodeConfig};

pub(super) async fn fetch(
    client: &Client,
    value: &str,
) -> Result<Vec<MihomoNodeConfig>, MihomoError> {
    let url = Url::parse(value).map_err(|_| MihomoError::invalid("Mihomo 订阅地址不合法"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || value.len() > 4096
    {
        return Err(MihomoError::invalid("Mihomo 订阅地址不合法"));
    }
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|_| MihomoError::unavailable("无法读取 Mihomo 订阅"))?;
    if !response.status().is_success() {
        return Err(MihomoError::unavailable("Mihomo 订阅服务返回失败状态"));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_SUBSCRIPTION_BYTES as u64)
    {
        return Err(MihomoError::invalid("Mihomo 订阅内容超过 8 MiB"));
    }
    let mut chunks = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| MihomoError::unavailable("无法完整读取 Mihomo 订阅"))?;
        if chunk.len() > MAX_SUBSCRIPTION_BYTES.saturating_sub(bytes.len()) {
            return Err(MihomoError::invalid("Mihomo 订阅内容超过 8 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    parse(&bytes)
}

fn parse(bytes: &[u8]) -> Result<Vec<MihomoNodeConfig>, MihomoError> {
    let invalid = || MihomoError::invalid("Mihomo 订阅必须包含合法的 proxies 节点列表");
    let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
    let documents = YamlLoader::load_from_str(text).map_err(|_| invalid())?;
    if documents.len() != 1 {
        return Err(invalid());
    }
    let entries = documents[0]["proxies"].as_vec().ok_or_else(invalid)?;
    if entries.is_empty() || entries.len() > MAX_SUBSCRIPTION_NODES {
        return Err(MihomoError::invalid("Mihomo 订阅须包含 1–1000 个节点"));
    }
    let mut names = std::collections::HashSet::with_capacity(entries.len());
    let mut nodes = Vec::with_capacity(entries.len());
    for entry in entries {
        let config = convert(entry, 0)?;
        validate_node(&config)?;
        let name = config["name"].as_str().ok_or_else(invalid)?.trim();
        if name.is_empty()
            || name.chars().count() > 100
            || name.chars().any(char::is_control)
            || !names.insert(name.to_owned())
        {
            return Err(MihomoError::invalid(
                "Mihomo 订阅节点名称为空、重复或超过长度限制",
            ));
        }
        nodes.push(MihomoNodeConfig {
            name: name.to_owned(),
            kind: config["type"].as_str().ok_or_else(invalid)?.to_owned(),
            config,
        });
    }
    Ok(nodes)
}

pub(super) fn validate_node(config: &Value) -> Result<(), MihomoError> {
    let object = config
        .as_object()
        .ok_or_else(|| MihomoError::invalid("Mihomo 节点必须是对象"))?;
    if object.contains_key("dialer-proxy") {
        return Err(MihomoError::invalid(
            "Mihomo 节点不支持 dialer-proxy 链式出口",
        ));
    }
    let kind = config["type"].as_str().unwrap_or_default();
    if kind.is_empty()
        || matches!(
            kind,
            "direct"
                | "reject"
                | "dns"
                | "relay"
                | "select"
                | "url-test"
                | "fallback"
                | "load-balance"
        )
        || !config["server"]
            .as_str()
            .is_some_and(|server| !server.trim().is_empty())
    {
        return Err(MihomoError::invalid("Mihomo 节点缺少有效的 type 或 server"));
    }
    Ok(())
}

fn convert(value: &Yaml, depth: usize) -> Result<Value, MihomoError> {
    let invalid = || MihomoError::invalid("Mihomo 节点配置结构不合法");
    if depth > 32 {
        return Err(invalid());
    }
    match value {
        Yaml::String(text) => Ok(Value::String(text.clone())),
        Yaml::Integer(number) => Ok(Value::from(*number)),
        Yaml::Boolean(value) => Ok(Value::Bool(*value)),
        Yaml::Real(number) => number
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(invalid),
        Yaml::Null => Ok(Value::Null),
        Yaml::Array(entries) => entries
            .iter()
            .map(|entry| convert(entry, depth + 1))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Yaml::Hash(entries) => {
            let mut object = Map::with_capacity(entries.len());
            for (key, entry) in entries {
                let key = key.as_str().ok_or_else(invalid)?;
                object.insert(key.to_owned(), convert(entry, depth + 1)?);
            }
            Ok(Value::Object(object))
        }
        Yaml::Alias(_) | Yaml::BadValue => Err(invalid()),
    }
}
