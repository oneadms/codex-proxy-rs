//! 协议登录独占 Cookie 和代理连接，限制重定向并校验会话清理结果

use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono_tz::Tz;
use gateway_admin::ports::provider::{ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::account::OutboundProxy;
use reqwest::{Client, Method, RequestBuilder, cookie::Jar, redirect::Policy};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

use super::{failure, invalid, text};

pub(super) const USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct ProtocolClient {
    pub(super) javascript: Arc<dyn gateway_admin::ports::javascript::JavaScriptRuntime>,
    client: Client,
    pub(super) device_id: String,
    pub(super) timezone: Tz,
}

impl ProtocolClient {
    pub(super) fn new(
        proxy: &OutboundProxy,
        timezone: Tz,
        javascript: Arc<dyn gateway_admin::ports::javascript::JavaScriptRuntime>,
    ) -> Result<Self, ProviderAdminError> {
        let jar = Arc::new(Jar::default());
        let device_id = Uuid::new_v4().to_string();
        for origin in ["https://chatgpt.com/", "https://auth.openai.com/"] {
            let url = Url::parse(origin).map_err(|_| invalid("登录地址不合法"))?;
            jar.add_cookie_str(&format!("oai-did={device_id}; Path=/; Secure"), &url);
        }
        let builder = gateway_egress::http_client(Some(proxy))
            .map_err(|_| invalid("协议登录代理配置不合法"))?
            .cookie_provider(jar)
            .redirect(Policy::none())
            .user_agent(USER_AGENT)
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(60));
        let client = crate::transport::tls::build_reqwest_client_with_custom_ca(builder).map_err(
            |error| {
                failure(
                    "协议登录无法初始化代理连接",
                    ProviderAdminErrorKind::Unavailable,
                )
                .with_source(error)
            },
        )?;
        Ok(Self {
            javascript,
            client,
            device_id,
            timezone,
        })
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
            .header("accept-language", "en-US,en;q=0.9")
            .header(
                "sec-ch-ua",
                "\"Chromium\";v=\"146\", \"Google Chrome\";v=\"146\", \"Not.A/Brand\";v=\"99\"",
            )
            .header("sec-ch-ua-mobile", "?0")
            .header("sec-ch-ua-platform", "\"macOS\"")
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
        let mut response = self.client.execute(request).await.map_err(|error| {
            let kind = if error.is_connect() {
                ProviderAdminErrorKind::Unavailable
            } else {
                ProviderAdminErrorKind::Ambiguous
            };
            failure("协议登录代理请求失败，请核对账号状态", kind)
                .with_message(diagnostic.clone())
                .with_source(error.without_url())
        })?;
        let status = response.status();
        let clock_delta_seconds = response
            .headers()
            .get(reqwest::header::DATE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| chrono::DateTime::parse_from_rfc2822(value).ok())
            .map(|server| server.timestamp() - chrono::Utc::now().timestamp());
        let browser_challenge = response
            .headers()
            .get("cf-mitigated")
            .is_some_and(|value| value == "challenge");
        tracing::debug!(target: "openai_protocol_login", request = %diagnostic, status = status.as_u16(), ?clock_delta_seconds, "协议登录上游响应");
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            failure(
                "协议登录响应未完整接收，操作结果未知",
                ProviderAdminErrorKind::Ambiguous,
            )
            .with_source(error.without_url())
        })? {
            if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(failure(
                    "上游登录响应超过大小限制",
                    ProviderAdminErrorKind::BadGateway,
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if status.is_client_error() || status.is_server_error() {
            let error = serde_json::from_slice::<Value>(&bytes).ok();
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
        let body = String::from_utf8(bytes)
            .map_err(|_| failure("上游登录响应格式不合法", ProviderAdminErrorKind::BadGateway))?;
        Ok((status, location, body))
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
            let request = self.request(Method::GET, target.as_str())?.header(
                "accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            );
            let (status, location, body) = self.send(request).await?;
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
