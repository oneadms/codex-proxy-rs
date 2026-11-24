//! 密码与 TOTP 登录、其他会话退出及固定代理的 Codex 授权编排

mod client;
mod recovery;
mod sentinel;
mod totp;

pub(crate) use recovery::CodexProtocolLoginRecovery;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_admin::ports::provider::{ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::account::{OutboundProxy, ProviderAccountStore, RequestLocation};
use secrecy::{ExposeSecret as _, SecretString};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;
use subtle::ConstantTimeEq as _;
use url::Url;
use uuid::Uuid;

use super::token_client::{OFFICIAL_CODEX_OAUTH_CLIENT_ID, OFFICIAL_CODEX_REDIRECT_URI};
use client::ProtocolClient;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LoginInput {
    #[serde(rename = "authentication_kind")]
    authentication_kind: String,
    username: String,
    password: SecretString,
    totp_secret: SecretString,
    #[serde(default)]
    change_device: bool,
}

pub(crate) struct PreparedProtocolLogin {
    pub(crate) tokens: Value,
    pub(crate) device: Value,
    pub(crate) credentials: Box<super::types::CodexProtocolLoginCredentials>,
}

pub(crate) async fn prepare_protocol_login(
    document: Value,
    proxy: Option<&OutboundProxy>,
    location: Option<&RequestLocation>,
    javascript: Arc<dyn gateway_admin::ports::javascript::JavaScriptRuntime>,
    accounts: &dyn ProviderAccountStore,
) -> Result<PreparedProtocolLogin, ProviderAdminError> {
    let input: LoginInput = serde_json::from_value(document)
        .map_err(|_| invalid("协议登录需提供用户名、密码和 2FA 密钥"))?;
    let credentials = Box::new(super::types::CodexProtocolLoginCredentials {
        username: input.username.clone(),
        password: input.password.expose_secret().to_owned(),
        totp_secret: input.totp_secret.expose_secret().to_owned(),
    });
    validate_credentials(&credentials)?;
    if input.authentication_kind != "protocol_login" {
        return Err(invalid("协议登录类型不合法"));
    }
    // 在任何网络请求之前检查出口和密钥，缺少节点时不能使用部署出口
    let proxy = proxy.ok_or_else(|| invalid("协议登录必须绑定节点代理"))?;
    let location = location.ok_or_else(|| invalid("协议登录节点尚未设置位置和时区"))?;
    let saved = if input.change_device {
        None
    } else {
        saved_device(accounts, &input.username).await?
    };
    tokio::time::timeout(std::time::Duration::from_secs(300), async {
        let client = ProtocolClient::new(proxy, location.timezone, javascript, saved).await?;
        let result = login(&client, input).await;
        let device = client.device.clone();
        client.close().await;
        result.map(|tokens| PreparedProtocolLogin {
            tokens,
            device,
            credentials,
        })
    })
    .await
    .map_err(|_| {
        failure(
            "协议登录超时，授权结果未知，请先核对账号状态",
            ProviderAdminErrorKind::Ambiguous,
        )
    })?
}

pub(crate) fn validate_credentials(
    credentials: &super::types::CodexProtocolLoginCredentials,
) -> Result<(), ProviderAdminError> {
    if credentials.username.trim().is_empty()
        || credentials.username.len() > 320
        || credentials.username.chars().any(char::is_control)
        || credentials.password.is_empty()
        || credentials.password.len() > 1024
        || credentials.password.contains(['\r', '\n'])
        || credentials.totp_secret.len() > 1024
    {
        return Err(invalid("协议登录的用户名或密码格式不合法"));
    }
    totp::generate(&credentials.totp_secret, 0)?;
    Ok(())
}

async fn saved_device(
    accounts: &dyn ProviderAccountStore,
    username: &str,
) -> Result<Option<Value>, ProviderAdminError> {
    let provider = gateway_core::identity::ProviderKind::new("openai")
        .map_err(|_| invalid("协议登录平台不合法"))?;
    let mut matches = accounts
        .list_for_provider(&provider)
        .await
        .map_err(|error| {
            failure("无法读取账号登录设备", ProviderAdminErrorKind::Unavailable).with_source(error)
        })?;
    matches.retain(|account| {
        account.authentication_kind() == super::CODEX_AUTHENTICATION_KIND_OAUTH
            && account
                .email()
                .is_some_and(|email| email.eq_ignore_ascii_case(username.trim()))
    });
    matches.sort_by(|left, right| left.id().as_str().cmp(right.id().as_str()));
    let mut latest: Option<Value> = None;
    for account in matches {
        let loaded = accounts
            .load_current_credential(account.id())
            .await
            .map_err(|error| {
                failure("无法读取账号登录设备", ProviderAdminErrorKind::Unavailable)
                    .with_source(error)
            })?;
        let data = super::CodexCredentialCodec::decode_complete(&loaded.credential)
            .map_err(|_| invalid("账号登录设备数据不合法"))?;
        if let Some(device) = data
            .oauth()
            .and_then(|data| data.protocol_login_device.as_deref().cloned())
        {
            // 同一邮箱的多个工作区共享最近一次成功导入的设备
            if latest.as_ref().is_none_or(|saved| {
                saved
                    .get("createdAt")
                    .and_then(Value::as_i64)
                    .unwrap_or_default()
                    < device
                        .get("createdAt")
                        .and_then(Value::as_i64)
                        .unwrap_or_default()
            }) {
                latest = Some(device);
            }
        }
    }
    Ok(latest)
}

async fn login(client: &ProtocolClient, input: LoginInput) -> Result<Value, ProviderAdminError> {
    client.follow("https://chatgpt.com/").await?;
    client
        .get_json("https://chatgpt.com/api/auth/providers")
        .await?;
    let csrf = client.get_json("https://chatgpt.com/api/auth/csrf").await?;
    let mut signin = Url::parse("https://chatgpt.com/api/auth/signin/openai")
        .map_err(|_| invalid("登录地址不合法"))?;
    signin
        .query_pairs_mut()
        .append_pair("prompt", "login")
        .append_pair("ext-oai-did", &client.device_id)
        .append_pair("auth_session_logging_id", &Uuid::new_v4().to_string())
        .append_pair("screen_hint", "login_or_signup")
        .append_pair("login_hint", &input.username);
    let signin = client
        .form(
            signin.as_str(),
            &[
                ("callbackUrl", "https://chatgpt.com/"),
                ("csrfToken", text(&csrf, "csrfToken")?),
                ("json", "true"),
            ],
        )
        .await?;
    let (mut landing, _) = client.follow(text(&signin, "url")?).await?;
    if landing.path() == "/email-verification" {
        let session = client
            .get_json("https://auth.openai.com/api/accounts/client_auth_session_dump")
            .await?;
        let session = &session["client_auth_session"];
        // 邮箱页也可能只是默认的免密入口；额外身份验证不能通过切换登录方式跳过
        let password_allowed = session["email_verification_mode"].as_str()
            == Some("passwordless_login")
            && session["reauth"].as_bool() != Some(true)
            && session.get("login_methods").is_none_or(|methods| {
                methods.as_array().is_some_and(|methods| {
                    methods
                        .iter()
                        .any(|method| method["type"].as_str() == Some("password"))
                })
            });
        if password_allowed {
            let username = session
                .get("email")
                .or_else(|| session.pointer("/username/value"))
                .and_then(Value::as_str);
            if !username
                .is_some_and(|username| username.eq_ignore_ascii_case(input.username.trim()))
            {
                return Err(invalid("上游登录会话与导入用户名不一致"));
            }
            (landing, _) = client
                .follow("https://auth.openai.com/log-in/password")
                .await?;
        }
    }
    if landing.path() != "/log-in/password" {
        return Err(failure(
            if landing.path() == "/email-verification" {
                "上游要求邮箱验证码，无法仅用密码和 2FA 完成协议登录"
            } else {
                "上游未返回密码登录页，协议登录未继续执行"
            },
            ProviderAdminErrorKind::BadGateway,
        )
        .with_message(format!("GET {}", landing.path())));
    }
    let mut payload = client
        .auth(
            "/api/accounts/password/verify",
            &json!({
                "password": input.password.expose_secret(),
            }),
            true,
            landing.as_str(),
        )
        .await?;
    if payload.pointer("/page/type").and_then(Value::as_str) == Some("mfa_challenge")
        || payload
            .get("continue_url")
            .and_then(Value::as_str)
            .and_then(|value| Url::parse(value).ok())
            .is_some_and(|url| url.path().starts_with("/mfa-challenge/"))
    {
        let session = payload
            .get("oai-client-auth-session")
            .ok_or_else(|| invalid("登录响应缺少 2FA 验证方式"))?;
        let factor = session
            .get("mfa_challenge_factors")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .chain(
                session
                    .get("mfa_factors")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten(),
            )
            .find(|factor| {
                factor
                    .get("type")
                    .or_else(|| factor.get("factor_type"))
                    .and_then(Value::as_str)
                    == Some("totp")
            })
            .ok_or_else(|| invalid("账号未提供 TOTP 验证方式"))?;
        let id = text(factor, "id")?.to_owned();
        client
            .auth(
                "/api/accounts/mfa/issue_challenge",
                &json!({
                    "type": "totp", "id": id, "force_fresh_challenge": false,
                }),
                false,
                landing.as_str(),
            )
            .await?;
        let challenge_url = payload
            .get("continue_url")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("https://auth.openai.com/mfa-challenge/{id}"));
        // 安全令牌请求可能等待代理，完成后再生成当前周期的验证码
        let verification = client
            .auth_request("/api/accounts/mfa/verify", true, &challenge_url)
            .await?;
        payload = client.request_json(verification.json(&json!({
            "type": "totp", "id": id,
            "code": totp::generate(input.totp_secret.expose_secret(), chrono::Utc::now().timestamp())?,
        }))).await?;
    }
    payload = select_workspace(client, payload, None).await?;
    client.follow(continue_url(&payload)?).await?;
    let session = client
        .get_json("https://chatgpt.com/api/auth/session")
        .await?;
    let access_token = text(&session, "accessToken")?;
    let metadata = super::types::parse_chatgpt_jwt_claims(access_token)
        .map_err(|_| invalid("登录返回的账号身份不完整"))?;
    if metadata
        .email
        .as_ref()
        .is_some_and(|email| !email.eq_ignore_ascii_case(input.username.trim()))
    {
        return Err(invalid("登录返回的账号与导入用户名不一致"));
    }
    let account_id = metadata
        .chatgpt_account_id
        .as_deref()
        .ok_or_else(|| invalid("登录返回的工作区身份不完整"))?;
    client
        .revoke_other_sessions(access_token, account_id)
        .await?;
    let tokens = authorize(client, account_id).await?;
    let authorized = super::types::parse_chatgpt_jwt_claims(text(&tokens, "idToken")?)
        .map_err(|_| invalid("Codex 授权返回的账号身份不完整"))?;
    if authorized.chatgpt_account_id.as_deref() != Some(account_id)
        || authorized
            .email
            .as_ref()
            .is_some_and(|email| !email.eq_ignore_ascii_case(input.username.trim()))
    {
        return Err(invalid("Codex 授权返回的账号与本次登录不一致"));
    }
    Ok(tokens)
}

async fn select_workspace(
    client: &ProtocolClient,
    payload: Value,
    account_id: Option<&str>,
) -> Result<Value, ProviderAdminError> {
    let workspaces = payload
        .pointer("/oai-client-auth-session/workspaces")
        .and_then(Value::as_array);
    let workspace = if let Some(account_id) = account_id {
        workspaces.and_then(|workspaces| {
            workspaces
                .iter()
                .find(|workspace| workspace.get("id").and_then(Value::as_str) == Some(account_id))
        })
    } else {
        workspaces.and_then(|workspaces| workspaces.first())
    };
    if workspaces.is_some_and(|workspaces| !workspaces.is_empty()) && workspace.is_none() {
        return Err(invalid("Codex 工作区与本次登录不一致"));
    }
    if let Some(workspace) = workspace {
        return client
            .auth(
                "/api/accounts/workspace/select",
                &json!({
                    "workspace_id": text(workspace, "id")?,
                }),
                false,
                payload
                    .get("continue_url")
                    .and_then(Value::as_str)
                    .unwrap_or("https://auth.openai.com/workspace"),
            )
            .await;
    }
    Ok(payload)
}

async fn authorize(client: &ProtocolClient, account_id: &str) -> Result<Value, ProviderAdminError> {
    let verifier = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let state = Uuid::new_v4().to_string();
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut authorization = Url::parse("https://auth.openai.com/oauth/authorize")
        .map_err(|_| invalid("Codex 授权地址不合法"))?;
    authorization
        .query_pairs_mut()
        .append_pair("client_id", OFFICIAL_CODEX_OAUTH_CLIENT_ID)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", OFFICIAL_CODEX_REDIRECT_URI)
        .append_pair(
            "scope",
            "openid profile email offline_access api.connectors.read api.connectors.invoke",
        )
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state)
        .append_pair("id_token_add_organizations", "true")
        .append_pair("codex_cli_simplified_flow", "true");
    let (mut target, html) = client.follow(authorization.as_str()).await?;
    if !client::is_callback(&target) {
        let session_id = account_session_id(&html)?;
        let selected = client
            .auth(
                "/api/accounts/session/select",
                &json!({
                    "session_id": session_id,
                }),
                false,
                target.as_str(),
            )
            .await?;
        let selected = select_workspace(client, selected, Some(account_id)).await?;
        (target, _) = client.follow(continue_url(&selected)?).await?;
    }
    // 回调只在本地提取授权码，绝不向代理节点的 localhost 发出请求
    if !client::is_callback(&target) {
        return Err(invalid("Codex 授权未返回预期回调"));
    }
    let parameters: Vec<_> = target.query_pairs().collect();
    let code: Vec<_> = parameters
        .iter()
        .filter(|(name, _)| name == "code")
        .collect();
    let callback_state: Vec<_> = parameters
        .iter()
        .filter(|(name, _)| name == "state")
        .collect();
    if code.len() != 1
        || callback_state.len() != 1
        || code[0].1.is_empty()
        || !bool::from(state.as_bytes().ct_eq(callback_state[0].1.as_bytes()))
    {
        return Err(invalid("Codex 授权回调校验失败"));
    }
    let tokens = client
        .form(
            "https://auth.openai.com/oauth/token",
            &[
                ("grant_type", "authorization_code"),
                ("client_id", OFFICIAL_CODEX_OAUTH_CLIENT_ID),
                ("redirect_uri", OFFICIAL_CODEX_REDIRECT_URI),
                ("code_verifier", &verifier),
                ("code", code[0].1.as_ref()),
            ],
        )
        .await?;
    Ok(json!({
        "accessToken": text(&tokens, "access_token")?,
        "refreshToken": text(&tokens, "refresh_token")?,
        "idToken": text(&tokens, "id_token")?,
    }))
}

fn account_session_id(html: &str) -> Result<String, ProviderAdminError> {
    // 流式页面会重复引用会话，只接受去重后的唯一候选，避免选中其他账号
    let matcher =
        regex::Regex::new(r"\bus_[a-zA-Z0-9_-]+").map_err(|_| invalid("Codex 会话解析失败"))?;
    let sessions: std::collections::BTreeSet<_> =
        matcher.find_iter(html).map(|item| item.as_str()).collect();
    if let Some(session) = sessions.first().filter(|_| sessions.len() == 1) {
        return Ok((*session).to_owned());
    }
    Err(failure(
        "Codex 授权无法唯一确定登录会话，需要人工确认",
        ProviderAdminErrorKind::BadGateway,
    )
    .with_message(format!(
        "Codex account selection: {} sessions",
        sessions.len()
    )))
}

fn continue_url(payload: &Value) -> Result<&str, ProviderAdminError> {
    payload
        .get("continue_url")
        .or_else(|| payload.pointer("/page/payload/url"))
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| {
            failure(
                "登录流程缺少后续地址，需要人工确认",
                ProviderAdminErrorKind::BadGateway,
            )
        })
}

fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str, ProviderAdminError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| {
            failure(
                "上游登录响应缺少必要字段",
                ProviderAdminErrorKind::BadGateway,
            )
        })
}

fn invalid(message: &'static str) -> ProviderAdminError {
    failure(message, ProviderAdminErrorKind::Invalid)
}

fn failure(message: &'static str, kind: ProviderAdminErrorKind) -> ProviderAdminError {
    ProviderAdminError::new(kind).with_public_message(message)
}
