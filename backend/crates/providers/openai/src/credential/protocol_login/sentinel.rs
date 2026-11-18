//! 在当前登录浏览器内运行官方 Sentinel SDK 并校验设备归属

use gateway_admin::ports::provider::{ProviderAdminError, ProviderAdminErrorKind};
use serde_json::{Value, json};

use super::{client::ProtocolClient, failure};

pub(super) struct SentinelTokens {
    pub(super) token: String,
    pub(super) session_observer: Option<String>,
}

pub(super) async fn token(client: &ProtocolClient) -> Result<SentinelTokens, ProviderAdminError> {
    let response = client.call(json!({"type": "sentinel"})).await?;
    let token = response
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(runtime_error)?;
    let value: Value = serde_json::from_str(token).map_err(|_| runtime_error())?;
    if value.get("id").and_then(Value::as_str) != Some(client.device_id.as_str())
        || value.get("flow").and_then(Value::as_str) != Some("password_verify")
        || value.get("p").is_none()
        || value.get("t").is_none()
        || !value
            .get("c")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        || value
            .get("e")
            .is_some_and(|error| error.as_str().is_some_and(|value| !value.is_empty()))
    {
        return Err(runtime_error());
    }
    let session_observer = response.get("sessionObserver").and_then(Value::as_str);
    if let Some(observer) = session_observer {
        let value: Value = serde_json::from_str(observer).map_err(|_| runtime_error())?;
        if value.get("id").and_then(Value::as_str) != Some(client.device_id.as_str())
            || value.get("flow").and_then(Value::as_str) != Some("password_verify")
            || value.get("so").is_none()
            || !value
                .get("c")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        {
            return Err(runtime_error());
        }
    }
    Ok(SentinelTokens {
        token: token.to_owned(),
        session_observer: session_observer.map(str::to_owned),
    })
}

fn runtime_error() -> ProviderAdminError {
    failure(
        "浏览器安全验证未完成，需要人工确认",
        ProviderAdminErrorKind::BadGateway,
    )
}
