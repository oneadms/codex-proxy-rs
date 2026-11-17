//! 实时官方 Sentinel SDK 执行与当前账号代理、Cookie 的网络桥接

use std::io::Read as _;

use gateway_admin::{
    ports::javascript::{JavaScriptError, JavaScriptSession},
    ports::provider::{ProviderAdminError, ProviderAdminErrorKind},
};
use reqwest::Method;
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};
use url::Url;

use super::{
    client::{ProtocolClient, USER_AGENT},
    failure, invalid, text,
};

pub(super) struct SentinelTokens {
    pub(super) token: String,
    pub(super) session_observer: Option<String>,
}

pub(super) async fn token(client: &ProtocolClient) -> Result<SentinelTokens, ProviderAdminError> {
    let page = "https://auth.openai.com/log-in/password";
    let loader_url = "https://sentinel.openai.com/backend-api/sentinel/sdk.js";
    let (_, _, loader_source) = client
        .send(
            client
                .request(Method::GET, loader_url)?
                .header("accept", "*/*")
                .header("referer", page)
                .header("sec-fetch-dest", "script")
                .header("sec-fetch-mode", "no-cors")
                .header("sec-fetch-site", "cross-site"),
        )
        .await?;
    let matcher = regex::Regex::new(r#"https://sentinel\.openai\.com/sentinel/[^\"']+/sdk\.js"#)
        .map_err(|_| runtime_error())?;
    let sdk_url = matcher
        .find(&loader_source)
        .ok_or_else(runtime_error)?
        .as_str();
    let (_, _, sdk_source) = client
        .send(
            client
                .request(Method::GET, sdk_url)?
                .header("accept", "*/*")
                .header("referer", loader_url)
                .header("sec-fetch-dest", "script")
                .header("sec-fetch-mode", "no-cors")
                .header("sec-fetch-site", "same-origin"),
        )
        .await?;
    let mut program = String::new();
    flate2::read::GzDecoder::new(
        &include_bytes!("../../../runtime/protocol-login/sentinel-runtime.generated.gz")[..],
    )
    .read_to_string(&mut program)
    .map_err(|_| runtime_error())?;
    let mut runtime = client
        .javascript
        .start(&program, client.timezone.name())
        .await
        .map_err(map_runtime_error)?;
    send(
        runtime.as_mut(),
        &json!({
            "url": page, "userAgent": USER_AGENT, "platform": "MacIntel",
            "language": "en-US", "languages": ["en-US", "en"],
            "cookies": {"oai-did": client.device_id},
            "loaderUrl": loader_url, "loaderSource": loader_source,
            "sdkUrl": sdk_url, "sdkSource": sdk_source,
        }),
    )
    .await?;
    let mut tokens = SentinelTokens {
        token: String::new(),
        session_observer: None,
    };
    for method in ["token", "sessionObserverToken"] {
        send(
            runtime.as_mut(),
            &json!({"type": "call", "method": method, "flow": "password_verify"}),
        )
        .await?;
        loop {
            let message = runtime.receive().await.map_err(map_runtime_error)?;
            let event: Value =
                serde_json::from_str(message.expose_secret()).map_err(|_| runtime_error())?;
            match event.get("type").and_then(Value::as_str) {
                Some("request") => relay(client, runtime.as_mut(), &event["request"]).await?,
                Some("result") => {
                    if event.get("method").and_then(Value::as_str) != Some(method)
                        || event.get("flow").and_then(Value::as_str) != Some("password_verify")
                    {
                        return Err(runtime_error());
                    }
                    let has_value = event.get("value").and_then(Value::as_str).is_some();
                    let errors = event
                        .get("errors")
                        .and_then(Value::as_array)
                        .map_or(0, Vec::len);
                    tracing::debug!(target: "openai_protocol_login", method, has_value, errors, "Sentinel 运行结果");
                    if errors != 0 {
                        return Err(runtime_error());
                    }
                    let value = event.get("value").and_then(Value::as_str);
                    if method == "token" {
                        tokens.token = value.ok_or_else(runtime_error)?.to_owned();
                    } else {
                        tokens.session_observer = value.map(str::to_owned);
                    }
                    break;
                }
                Some("frame") => {}
                _ => return Err(runtime_error()),
            }
        }
    }
    runtime.close().await;
    let value: Value = serde_json::from_str(&tokens.token).map_err(|_| runtime_error())?;
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
    if let Some(observer) = &tokens.session_observer {
        let observer: Value = serde_json::from_str(observer).map_err(|_| runtime_error())?;
        if observer.get("id").and_then(Value::as_str) != Some(client.device_id.as_str())
            || observer.get("flow").and_then(Value::as_str) != Some("password_verify")
            || observer.get("so").is_none()
            || !observer
                .get("c")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        {
            return Err(runtime_error());
        }
    }
    Ok(tokens)
}

async fn relay(
    client: &ProtocolClient,
    runtime: &mut dyn JavaScriptSession,
    request: &Value,
) -> Result<(), ProviderAdminError> {
    let url = Url::parse(text(request, "url")?).map_err(|_| runtime_error())?;
    let source = Url::parse(text(request, "sourceUrl")?).map_err(|_| runtime_error())?;
    // SDK 的内嵌页仅拥有 Sentinel 网络能力，不借账号 Cookie 访问其他登录接口
    if url.host_str() != Some("sentinel.openai.com") {
        return Err(invalid("Sentinel 返回了不允许访问的地址"));
    }
    let verb =
        Method::from_bytes(text(request, "method")?.as_bytes()).map_err(|_| runtime_error())?;
    let mut outgoing = client
        .request(verb.clone(), url.as_str())?
        .header("referer", source.as_str())
        .header("origin", source.origin().ascii_serialization())
        .header(
            "sec-fetch-site",
            if source.origin() == url.origin() {
                "same-origin"
            } else {
                "cross-site"
            },
        )
        .header("sec-fetch-mode", "cors")
        .header("sec-fetch-dest", "empty");
    if let Some(headers) = request.get("headers").and_then(Value::as_object) {
        for (name, value) in headers {
            if let Some(value) = value.as_str() {
                outgoing = outgoing.header(name, value);
            }
        }
    }
    if verb != Method::GET {
        outgoing = outgoing
            .header("content-type", "text/plain;charset=UTF-8")
            .body(
                request
                    .get("body")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            );
    }
    let (status, _, body) = client.send(outgoing).await?;
    send(runtime, &json!({"id": request["id"], "status": status.as_u16(), "url": request["url"], "headers": {}, "body": body})).await
}

async fn send(
    runtime: &mut dyn JavaScriptSession,
    message: &Value,
) -> Result<(), ProviderAdminError> {
    let message = SecretString::from(serde_json::to_string(message).map_err(|_| runtime_error())?);
    runtime.send(&message).await.map_err(map_runtime_error)
}

fn runtime_error() -> ProviderAdminError {
    failure(
        "Sentinel 动态安全验证未完成",
        ProviderAdminErrorKind::BadGateway,
    )
}

fn map_runtime_error(error: JavaScriptError) -> ProviderAdminError {
    match error {
        JavaScriptError::Unavailable => failure(
            "协议登录需要可执行的 Node.js 运行时",
            ProviderAdminErrorKind::Unavailable,
        ),
        JavaScriptError::Capacity => failure(
            "协议登录安全验证运行时已达并发上限",
            ProviderAdminErrorKind::Unavailable,
        ),
        _ => runtime_error(),
    }
    .with_source(error)
}
