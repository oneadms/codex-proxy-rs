//! 浏览器协议登录通信、受限导航和会话清理结果校验

use std::{
    collections::{BTreeMap, HashSet},
    io::Read as _,
    sync::Arc,
};

use chrono_tz::Tz;
use gateway_admin::ports::{
    javascript::{JavaScriptError, JavaScriptSession},
    provider::{ProviderAdminError, ProviderAdminErrorKind},
};
use gateway_core::account::OutboundProxy;
use reqwest::{Client, Method, RequestBuilder};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

use super::{failure, invalid, text};

const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct ProtocolClient {
    client: Client,
    session: Mutex<Box<dyn JavaScriptSession>>,
    pub(super) device_id: String,
    pub(super) device: Value,
}

impl ProtocolClient {
    pub(super) async fn new(
        proxy: &OutboundProxy,
        timezone: Tz,
        javascript: Arc<dyn gateway_admin::ports::javascript::JavaScriptRuntime>,
        saved_device: Option<Value>,
    ) -> Result<Self, ProviderAdminError> {
        let mut program = String::new();
        flate2::read::GzDecoder::new(
            &include_bytes!("../../../runtime/protocol-login/browser-runtime.generated.gz")[..],
        )
        .read_to_string(&mut program)
        .map_err(|_| invalid("协议登录浏览器资源不合法"))?;
        let session = javascript
            .start(&program, timezone.name())
            .await
            .map_err(map_runtime_error)?;
        // reqwest 只编码表单及 JSON，不执行网络请求，实际请求统一交给浏览器
        let client = Client::builder()
            .no_proxy()
            .build()
            .map_err(|_| invalid("协议登录请求初始化失败"))?;
        let mut this = Self {
            client,
            session: Mutex::new(session),
            device_id: String::new(),
            device: Value::Null,
        };
        let ready = this.call(json!({
            "type": "init", "proxy": proxy.expose_url(), "timezone": timezone.name(), "device": saved_device,
        })).await?;
        let device = ready
            .get("device")
            .filter(|value| value.is_object())
            .ok_or_else(|| invalid("协议登录浏览器未返回设备参数"))?;
        let id = super::text(device, "id")?;
        Uuid::parse_str(id).map_err(|_| invalid("协议登录设备标识不合法"))?;
        this.device_id = id.to_owned();
        this.device = device.clone();
        Ok(this)
    }

    pub(super) async fn close(self) {
        self.session.into_inner().close().await;
    }

    pub(super) async fn call(&self, command: Value) -> Result<Value, ProviderAdminError> {
        let mut session = self.session.lock().await;
        let command =
            serde_json::to_string(&command).map_err(|_| invalid("协议登录浏览器请求不合法"))?;
        session
            .send(&SecretString::from(command))
            .await
            .map_err(map_runtime_error)?;
        let response = session.receive().await.map_err(map_runtime_error)?;
        let response: Value = serde_json::from_str(response.expose_secret())
            .map_err(|_| invalid("协议登录浏览器响应不合法"))?;
        if response.get("type").and_then(Value::as_str) == Some("error") {
            let (message, kind) = match response.get("code").and_then(Value::as_str) {
                Some("browser_unavailable") => (
                    "协议登录浏览器启动失败，请检查 Camoufox 运行环境",
                    ProviderAdminErrorKind::Unavailable,
                ),
                Some("invalid_device") => {
                    ("账号登录设备参数不合法", ProviderAdminErrorKind::Invalid)
                }
                Some("invalid_proxy") => {
                    ("协议登录代理配置不合法", ProviderAdminErrorKind::Invalid)
                }
                Some("untrusted_url") => (
                    "登录流程返回了不允许访问的地址",
                    ProviderAdminErrorKind::Invalid,
                ),
                Some("response_limit") => (
                    "上游登录响应超过大小限制",
                    ProviderAdminErrorKind::BadGateway,
                ),
                Some("sentinel_failed") => (
                    "浏览器安全验证未完成，需要人工确认",
                    ProviderAdminErrorKind::BadGateway,
                ),
                _ => (
                    "协议登录浏览器请求失败，操作结果未知",
                    ProviderAdminErrorKind::Ambiguous,
                ),
            };
            return Err(failure(message, kind));
        }
        Ok(response)
    }

    pub(super) fn request(
        &self,
        method: Method,
        url: &str,
    ) -> Result<RequestBuilder, ProviderAdminError> {
        let target = Url::parse(url).map_err(|_| invalid("上游登录地址不合法"))?;
        if target.scheme() != "https"
            || !matches!(
                target.host_str(),
                Some("chatgpt.com" | "auth.openai.com" | "sentinel.openai.com")
            )
            || !target.username().is_empty()
            || target.password().is_some()
            || target.port_or_known_default() != Some(443)
        {
            return Err(invalid("登录流程返回了不允许访问的地址"));
        }
        Ok(self
            .client
            .request(method, target.clone())
            .header("accept", "application/json, text/plain, */*")
            .header("origin", target.origin().ascii_serialization())
            .header(
                "referer",
                format!("{}/", target.origin().ascii_serialization()),
            )
            .header("oai-device-id", &self.device_id)
            .header("oai-did", &self.device_id))
    }

    pub(super) async fn send(
        &self,
        request: RequestBuilder,
    ) -> Result<(reqwest::StatusCode, Option<String>, String), ProviderAdminError> {
        let request = request
            .build()
            .map_err(|error| invalid("协议登录请求构建失败").with_source(error.without_url()))?;
        // 诊断只保留方法、路径和状态，不记录查询参数、请求正文或上游响应
        let diagnostic = format!("{} {}", request.method(), request.url().path());
        let headers: BTreeMap<_, _> = request
            .headers()
            .iter()
            .map(|(name, value)| {
                value
                    .to_str()
                    .map(|value| (name.as_str(), value))
                    .map_err(|_| invalid("协议登录请求头不合法"))
            })
            .collect::<Result<_, _>>()?;
        let body = request
            .body()
            .and_then(|body| body.as_bytes())
            .unwrap_or_default();
        let body = std::str::from_utf8(body).map_err(|_| invalid("协议登录请求正文不合法"))?;
        let response = self.call(json!({
            "type": "request", "url": request.url().as_str(), "method": request.method().as_str(),
            "headers": headers, "body": body,
        })).await.map_err(|error| error.with_message(diagnostic.clone()))?;
        self.response(&response, &diagnostic)
    }

    fn response(
        &self,
        response: &Value,
        diagnostic: &str,
    ) -> Result<(reqwest::StatusCode, Option<String>, String), ProviderAdminError> {
        let status = response
            .get("status")
            .and_then(Value::as_u64)
            .and_then(|status| u16::try_from(status).ok())
            .and_then(|status| reqwest::StatusCode::from_u16(status).ok())
            .ok_or_else(|| invalid("协议登录浏览器未返回状态码"))?;
        let headers = &response["headers"];
        let clock_delta_seconds = headers
            .get("date")
            .and_then(Value::as_str)
            .and_then(|value| chrono::DateTime::parse_from_rfc2822(value).ok())
            .map(|server| server.timestamp() - chrono::Utc::now().timestamp());
        let browser_challenge =
            headers.get("cf-mitigated").and_then(Value::as_str) == Some("challenge");
        tracing::debug!(target: "openai_protocol_login", request = %diagnostic, status = status.as_u16(), ?clock_delta_seconds, "协议登录上游响应");
        let location = headers
            .get("location")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let body = response
            .get("body")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("协议登录浏览器未返回响应正文"))?;
        if body.len() > MAX_RESPONSE_BYTES {
            return Err(failure(
                "上游登录响应超过大小限制",
                ProviderAdminErrorKind::BadGateway,
            ));
        }
        if status.is_client_error() || status.is_server_error() {
            let error = serde_json::from_str::<Value>(body).ok();
            let deactivated = error
                .as_ref()
                .and_then(|error| error.pointer("/error/code"))
                .and_then(Value::as_str)
                == Some("account_deactivated");
            let mut diagnostic = format!("{diagnostic}: HTTP {status}");
            if deactivated {
                diagnostic.push_str("; code=account_deactivated");
            }
            if browser_challenge {
                diagnostic.push_str("; browser_challenge=true");
            }
            return Err(failure(
                if deactivated {
                    "OpenAI 账号已停用，无法完成协议登录"
                } else {
                    "上游拒绝协议登录请求，请检查账号或安全验证要求"
                },
                ProviderAdminErrorKind::BadGateway,
            )
            .with_message(diagnostic));
        }
        Ok((status, location, body.to_owned()))
    }

    pub(super) async fn get_json(&self, url: &str) -> Result<Value, ProviderAdminError> {
        self.request_json(self.request(Method::GET, url)?).await
    }

    pub(super) async fn request_json(
        &self,
        request: RequestBuilder,
    ) -> Result<Value, ProviderAdminError> {
        let (status, _, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(failure(
                "上游登录接口未返回成功结果",
                ProviderAdminErrorKind::BadGateway,
            ));
        }
        serde_json::from_str(&body).map_err(|_| {
            failure(
                "上游登录接口未返回有效 JSON",
                ProviderAdminErrorKind::BadGateway,
            )
        })
    }

    pub(super) async fn form(
        &self,
        url: &str,
        fields: &[(&str, &str)],
    ) -> Result<Value, ProviderAdminError> {
        self.request_json(self.request(Method::POST, url)?.form(fields))
            .await
    }

    pub(super) async fn auth(
        &self,
        path: &str,
        body: &Value,
        security_token: bool,
        referer: &str,
    ) -> Result<Value, ProviderAdminError> {
        self.request_json(
            self.auth_request(path, security_token, referer)
                .await?
                .json(body),
        )
        .await
    }

    pub(super) async fn auth_request(
        &self,
        path: &str,
        security_token: bool,
        referer: &str,
    ) -> Result<RequestBuilder, ProviderAdminError> {
        let mut request = self
            .request(Method::POST, &format!("https://auth.openai.com{path}"))?
            .header("x-access-flow-invocation-id", Uuid::new_v4().to_string())
            .header("referer", referer)
            .header("sec-fetch-site", "same-origin")
            .header("sec-fetch-mode", "cors")
            .header("sec-fetch-dest", "empty");
        if security_token {
            let tokens = super::sentinel::token(self).await?;
            request = request.header("openai-sentinel-token", tokens.token);
            if let Some(observer) = tokens.session_observer {
                request = request.header("openai-sentinel-so-token", observer);
            }
        }
        Ok(request)
    }

    pub(super) async fn follow(&self, url: &str) -> Result<(Url, String), ProviderAdminError> {
        let mut target = Url::parse(url).map_err(|_| invalid("上游跳转地址不合法"))?;
        for _ in 0..12 {
            if is_callback(&target) {
                return Ok((target, String::new()));
            }
            drop(self.request(Method::GET, target.as_str())?);
            let diagnostic = format!("GET {}", target.path());
            let response = self
                .call(json!({"type": "navigate", "url": target.as_str()}))
                .await
                .map_err(|error| error.with_message(diagnostic.clone()))?;
            target = Url::parse(super::text(&response, "url")?)
                .map_err(|_| invalid("浏览器跳转地址不合法"))?;
            if is_callback(&target) {
                return Ok((target, String::new()));
            }
            drop(self.request(Method::GET, target.as_str())?);
            let (status, location, body) = self.response(&response, &diagnostic)?;
            let location = if status.is_redirection() {
                location.ok_or_else(|| invalid("上游跳转缺少地址"))?
            } else {
                // 授权入口也会通过 JSON 返回 GET 继续地址，仍须经过相同的地址检查
                let continuation = serde_json::from_str::<Value>(&body)
                    .ok()
                    .filter(|payload| {
                        payload
                            .get("method")
                            .and_then(Value::as_str)
                            .is_some_and(|method| method.eq_ignore_ascii_case("GET"))
                    })
                    .and_then(|payload| {
                        payload
                            .get("continue_url")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    });
                let Some(continuation) = continuation else {
                    return Ok((target, body));
                };
                continuation
            };
            target = target
                .join(&location)
                .map_err(|_| invalid("上游跳转地址不合法"))?;
        }
        Err(failure(
            "上游登录跳转次数超过限制",
            ProviderAdminErrorKind::BadGateway,
        ))
    }

    pub(super) async fn revoke_other_sessions(
        &self,
        access_token: &str,
        account_id: &str,
    ) -> Result<(), ProviderAdminError> {
        let sessions = self.sessions(access_token, account_id).await?;
        let devices = sessions
            .get("devices")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("会话列表格式不合法，未继续授权"))?;
        let current: Vec<_> = devices
            .iter()
            .filter(|device| device.get("is_current_device").and_then(Value::as_bool) == Some(true))
            .collect();
        if current.len() != 1 {
            return Err(invalid("无法唯一识别本次登录会话，未退出其他会话"));
        }
        let current_id = text(current[0], "session_id")?.to_owned();
        let mut revoked = HashSet::new();
        for device in devices {
            let id = text(device, "session_id")?;
            let is_current = device
                .get("is_current_device")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid("会话缺少当前设备标识，未继续授权"))?;
            if is_current || id == current_id || !revoked.insert(id) {
                continue;
            }
            self.request_json(
                self.request(
                    Method::POST,
                    "https://chatgpt.com/backend-api/accounts/sessions/revoke",
                )?
                .bearer_auth(access_token)
                .header("chatgpt-account-id", account_id)
                .json(&json!({ "session_id": id })),
            )
            .await?;
        }
        let remaining = self.sessions(access_token, account_id).await?;
        let devices = remaining
            .get("devices")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("无法确认其他会话是否已退出"))?;
        if devices.len() != 1
            || devices[0].get("is_current_device").and_then(Value::as_bool) != Some(true)
            || text(&devices[0], "session_id")? != current_id
        {
            return Err(failure(
                "其他会话尚未全部退出，未继续 Codex 授权",
                ProviderAdminErrorKind::BadGateway,
            ));
        }
        Ok(())
    }

    async fn sessions(
        &self,
        access_token: &str,
        account_id: &str,
    ) -> Result<Value, ProviderAdminError> {
        self.request_json(
            self.request(
                Method::GET,
                "https://chatgpt.com/backend-api/accounts/sessions",
            )?
            .bearer_auth(access_token)
            .header("chatgpt-account-id", account_id),
        )
        .await
    }
}

pub(super) fn is_callback(url: &Url) -> bool {
    url.scheme() == "http"
        && url.host_str() == Some("127.0.0.1")
        && url.port() == Some(1455)
        && url.path() == "/auth/callback"
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}

fn map_runtime_error(error: JavaScriptError) -> ProviderAdminError {
    let message = match error {
        JavaScriptError::Unavailable => "协议登录需要可执行的 Node.js 运行时",
        JavaScriptError::Capacity => "协议登录浏览器运行时已达并发上限",
        _ => "协议登录浏览器通信中断，操作结果未知",
    };
    failure(
        message,
        match error {
            JavaScriptError::Unavailable | JavaScriptError::Capacity => {
                ProviderAdminErrorKind::Unavailable
            }
            _ => ProviderAdminErrorKind::Ambiguous,
        },
    )
    .with_source(error)
}
