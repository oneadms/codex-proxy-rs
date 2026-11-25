//! 使用隔离 TLS 代理验证登录、会话清理、授权回调及拒绝路径

mod browser;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_admin::{
    model::provider_credentials::{PrepareCredentialImport, ProviderDocument},
    ports::provider::ProviderAdminErrorKind,
};
use gateway_core::account::{
    OpaqueProviderData, OutboundProxy, ProviderAccountStore, RequestLocation,
};
use provider_openai::credential::{
    CodexCredentialAdmin, CodexCredentialCodec, ImportCodexOAuthCredential,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use url::Url;

const CHILD_CASE: &str = "CPR_PROTOCOL_LOGIN_SYNTHETIC_CASE";

#[test]
fn fixed_proxy_login_should_revoke_only_other_sessions_before_authorizing() {
    if let Ok(case) = std::env::var(CHILD_CASE) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_case(&case));
        return;
    }
    for case in [
        "success",
        "account-form",
        "account-form-reversed",
        "account-entities",
        "account-stream",
        "multiple-account-sessions",
        "json-continuation",
        "email-password",
        "email-password-without-methods",
        "email-otp-required",
        "email-reauth",
        "email-password-unavailable",
        "email-wrong-account",
        "email-password-redirect",
        "revoke-failed",
        "wrong-state",
        "browser-challenge",
        "account-deactivated",
        "untrusted-sdk-request",
        "unexpected-login-page",
        "http-rejected",
        "missing-proxy",
        "unidentified-current",
        "proxy-failed",
        "reuse-device",
        "change-device",
        "failed-change-device",
        "auto-relogin",
        "auto-relogin-id-fallback",
        "auto-relogin-failed",
        "auto-relogin-wrong-user",
        "auto-relogin-no-credentials",
        "auto-relogin-busy",
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "admin::protocol_login::fixed_proxy_login_should_revoke_only_other_sessions_before_authorizing", "--nocapture"])
            .env(CHILD_CASE, case)
            .env("CODEX_CA_CERTIFICATE", std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/admin/protocol_login/ca.pem"))
            .output().unwrap();
        assert!(
            output.status.success(),
            "{case}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[derive(Default)]
struct State {
    paths: Vec<String>,
    revoked: Vec<String>,
    oauth_state: String,
}

async fn run_case(case: &str) {
    drop(provider_openai::transport::build_reqwest_client().unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy =
        OutboundProxy::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(include_bytes!("cert.der").to_vec())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(include_bytes!("key.der").to_vec())),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server));
    let state = Arc::new(Mutex::new(State::default()));
    let observed = state.clone();
    let scenario = if case == "failed-change-device" {
        "revoke-failed"
    } else {
        case
    }
    .to_owned();
    let worker = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (head, _) = read_request(&mut stream).await;
            let target = head
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
                .to_owned();
            assert!(
                [
                    "chatgpt.com:443",
                    "auth.openai.com:443",
                    "sentinel.openai.com:443"
                ]
                .contains(&target.as_str())
            );
            if scenario == "proxy-failed" {
                stream.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                continue;
            }
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let (head, body) = read_request(&mut stream).await;
            let path = head
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap();
            let url = Url::parse(&format!("https://{target}{path}")).unwrap();
            let response = respond(&scenario, &observed, &url, &head, &body);
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let config = super::valid_config();
    let store = Arc::new(crate::support::MemoryAccountStore::default());
    let original_device = json!({"id":"00000000-0000-4000-8000-000000000003", "config":{"navigator.platform":"Linux x86_64", "canvas:seed":98765}});
    let existing = if matches!(
        case,
        "reuse-device" | "change-device" | "failed-change-device"
    ) {
        let mut profile = crate::support::profile("workspace_fixture");
        profile.email = Some("fixture@example.invalid".to_owned());
        profile.chatgpt_user_id = "user_fixture".to_owned();
        let mut account = CodexCredentialAdmin
            .prepare_import(ImportCodexOAuthCredential {
                account_id: "acct_saved_device".to_owned(),
                name: "saved device".to_owned(),
                secret: crate::support::secret("synthetic-original"),
                verified_account: profile,
                next_refresh_at: None,
                enabled: true,
            })
            .unwrap();
        let mut data = CodexCredentialCodec::decode_complete(&account.credential).unwrap();
        data.oauth_mut().unwrap().protocol_login_device = Some(Box::new(original_device.clone()));
        account.credential = CodexCredentialCodec::encode_complete(data).unwrap();
        let credential = account.credential.clone();
        store.create_account(account).await.unwrap();
        Some(credential)
    } else {
        None
    };
    let browser = Arc::new(browser::FixtureBrowser::default());
    let ports =
        super::provider_ports_with(store.clone(), Arc::new(super::TestOAuthPending::default()));
    let ports = if case.starts_with("auto-relogin") {
        gateway_core::provider_ports::ProviderStorePorts::new(
            ports.accounts(),
            Arc::new(browser::RecoveryLeases {
                busy: case == "auto-relogin-busy",
                ..Default::default()
            }),
            ports.session_affinity(),
            ports.session_exclusions(),
            ports.catalog_cache(),
            ports.artifact_profiles(),
            ports.credential_state(),
            ports.cooldowns(),
            ports.runtime_policy(),
            ports.oauth_pending(),
            ports.diagnostics(),
        )
    } else {
        ports
    };
    let mut bundle = provider_openai::initialize(
        config.config.clone(),
        ports,
        Arc::new(gateway_core::account::UpstreamInfoReader::default()),
        browser.clone(),
    )
    .await
    .unwrap();
    if case.starts_with("auto-relogin") {
        let mut profile = crate::support::profile("workspace_fixture");
        profile.email = Some("fixture@example.invalid".to_owned());
        profile.chatgpt_user_id = if case == "auto-relogin-wrong-user" {
            "other_user"
        } else {
            "user_fixture"
        }
        .to_owned();
        let mut prepared = CodexCredentialAdmin
            .prepare_import(ImportCodexOAuthCredential {
                account_id: "acct_auto_relogin".to_owned(),
                name: "automatic recovery".to_owned(),
                secret: crate::support::secret("synthetic-expired"),
                verified_account: profile,
                next_refresh_at: None,
                enabled: true,
            })
            .unwrap();
        prepared.account = prepared
            .account
            .with_outbound_proxy(Some(proxy.clone()))
            .with_request_location(Some(RequestLocation::default()));
        let mut data = CodexCredentialCodec::decode_complete(&prepared.credential).unwrap();
        let oauth = data.oauth_mut().unwrap();
        oauth.protocol_login_device = Some(Box::new(original_device.clone()));
        if case != "auto-relogin-no-credentials" {
            oauth.protocol_login = Some(Box::new(
                provider_openai::credential::CodexProtocolLoginCredentials {
                    username: "fixture@example.invalid".to_owned(),
                    password: "synthetic-password".to_owned(),
                    totp_secret: "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ".to_owned(),
                },
            ));
        }
        prepared.credential = CodexCredentialCodec::encode_complete(data).unwrap();
        let original = prepared.credential.clone();
        store.create_account(prepared).await.unwrap();
        let before = store.account("acct_auto_relogin").unwrap();
        store
            .apply_state_change(gateway_core::account::AccountStateChange {
                account_id: before.id().clone(),
                expected_revision: before.revision(),
                credential_state: gateway_core::account::CredentialState::Expired,
                observed_at: std::time::SystemTime::now(),
                error_reason: Some(gateway_core::account::AccountErrorReason::AccessTokenExpired),
                message: None,
            })
            .await
            .unwrap();
        let registration = bundle
            .take_worker_contributions()
            .into_iter()
            .find_map(|item| match item {
                gateway_core::task::WorkerContribution::Registration(registration)
                    if registration.id.owner() == "openai-protocol-login" =>
                {
                    Some(registration)
                }
                _ => None,
            })
            .unwrap();
        let gateway_core::task::WorkerRunnable::Scheduled { task, .. } = registration.runnable
        else {
            panic!("scheduled recovery")
        };
        let context = gateway_core::task::WorkerCycleContext::new(
            registration.id,
            None,
            gateway_core::lifecycle::CancellationToken::new(),
        );
        let (first, second) = tokio::join!(
            task.run_cycle(context.clone()),
            task.run_cycle(context.clone())
        );
        first.unwrap();
        second.unwrap();
        task.run_cycle(context).await.unwrap();
        let current = store.load_current_credential(before.id()).await.unwrap();
        if matches!(case, "auto-relogin" | "auto-relogin-id-fallback") {
            assert_eq!(
                current.account.credential_state(),
                gateway_core::account::CredentialState::Ready
            );
            assert!(current.account.revision() > before.revision());
            let data = CodexCredentialCodec::decode_complete(&current.credential).unwrap();
            let oauth = data.oauth().unwrap();
            assert_eq!(
                oauth.refresh_token.as_deref(),
                Some("synthetic-refresh-token")
            );
            assert_eq!(
                oauth.protocol_login.as_ref().unwrap().password,
                "synthetic-password"
            );
            assert_eq!(
                oauth.protocol_login_device.as_deref(),
                Some(&original_device)
            );
            let exported = bundle
                .admin_provider()
                .export_credentials(vec![
                    gateway_admin::model::provider_credentials::ProviderExportCredentialInput {
                        account: super::account_record(&current.account),
                        provider_material: ProviderDocument::new(OpaqueProviderData::new(
                            current.credential.expose_to_provider().clone(),
                        )),
                    },
                ])
                .await
                .unwrap();
            assert!(!format!("{exported:?}").contains("synthetic-password"));
            let document = Value::Object(exported.document.into_provider_data().into_inner());
            assert_eq!(
                document["accounts"][0]["protocolLogin"]["password"],
                "synthetic-password"
            );
            let imported = bundle
                .admin_provider()
                .prepare_import(PrepareCredentialImport {
                    default_outbound_proxy: None,
                    default_request_location: None,
                    document: ProviderDocument::new(OpaqueProviderData::new(
                        document.as_object().unwrap().clone(),
                    )),
                })
                .await
                .unwrap();
            assert_eq!(
                imported.credentials[0]
                    .provider_material
                    .expose_to_provider()
                    .expose_to_provider()["protocol_login"]["password"],
                "synthetic-password"
            );
        } else {
            assert_eq!(current.account.revision(), before.revision());
            assert_eq!(current.credential, original);
            assert_eq!(
                current.account.credential_state(),
                gateway_core::account::CredentialState::Expired
            );
            let expected = if matches!(case, "auto-relogin-failed" | "auto-relogin-wrong-user") {
                gateway_core::account::AccountErrorReason::CredentialExpired
            } else {
                gateway_core::account::AccountErrorReason::AccessTokenExpired
            };
            assert_eq!(current.account.last_error_reason(), Some(expected));
        }
        let expected_sessions = usize::from(!matches!(
            case,
            "auto-relogin-no-credentials" | "auto-relogin-busy"
        ));
        assert_eq!(browser.devices.lock().unwrap().len(), expected_sessions);
        assert_eq!(*browser.closed.lock().unwrap(), expected_sessions);
        let state = state.lock().unwrap();
        assert!(state.revoked.is_empty());
        assert!(
            !state
                .paths
                .iter()
                .any(|path| path.starts_with("/backend-api/accounts/sessions"))
        );
        if case == "auto-relogin-failed" {
            assert_eq!(
                state.paths.last().map(String::as_str),
                Some("/oauth/authorize")
            );
            assert_eq!(
                state
                    .paths
                    .iter()
                    .filter(|path| path.as_str() == "/oauth/authorize")
                    .count(),
                1
            );
        }
        worker.abort();
        return;
    }
    let document = json!({
        "authentication_kind": "protocol_login", "username": "fixture@example.invalid",
        "password": "synthetic-password", "totpSecret": "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
        "changeDevice": matches!(case, "change-device" | "failed-change-device"),
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        bundle
            .admin_provider()
            .prepare_import(PrepareCredentialImport {
                default_outbound_proxy: (case != "missing-proxy").then_some(proxy.clone()),
                default_request_location: Some(RequestLocation::default()),
                document: ProviderDocument::new(OpaqueProviderData::new(
                    document.as_object().unwrap().clone(),
                )),
            }),
    )
    .await
    .unwrap();
    worker.abort();
    if let Some(existing) = &existing {
        let current = store
            .load_current_credential(
                &gateway_core::account::ProviderAccountId::new("acct_saved_device").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            current.credential.expose_to_provider(),
            existing.expose_to_provider()
        );
    }
    let state = state.lock().unwrap();
    if matches!(
        case,
        "success"
            | "account-form"
            | "account-form-reversed"
            | "account-entities"
            | "account-stream"
            | "json-continuation"
            | "email-password"
            | "email-password-without-methods"
            | "reuse-device"
            | "change-device"
    ) {
        let prepared = result.unwrap();
        assert_eq!(prepared.credentials.len(), 1);
        assert_eq!(
            prepared.credentials[0].outbound_proxy.as_ref(),
            Some(&proxy)
        );
        assert_eq!(state.revoked, ["us_other_1", "us_other_2"]);
        let revoke_at = state
            .paths
            .iter()
            .rposition(|path| path == "/backend-api/accounts/sessions/revoke")
            .unwrap();
        let authorize_at = state
            .paths
            .iter()
            .position(|path| path == "/oauth/authorize")
            .unwrap();
        assert!(revoke_at < authorize_at);
        assert_eq!(state.paths.last().unwrap(), "/oauth/token");
        let material = prepared.credentials[0]
            .provider_material
            .expose_to_provider();
        let value = material.expose_to_provider();
        assert_eq!(
            value["protocol_login_device"],
            browser.devices.lock().unwrap()[0]
        );
        assert_eq!(
            value["protocol_login"]["username"],
            "fixture@example.invalid"
        );
        assert_eq!(value["protocol_login"]["password"], "synthetic-password");
        assert_eq!(
            value["protocol_login"]["totpSecret"],
            "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"
        );
        if case == "reuse-device" {
            assert_eq!(value["protocol_login_device"], original_device);
        }
        if case == "change-device" {
            assert_ne!(value["protocol_login_device"]["id"], original_device["id"]);
        }
        if let Some(existing) = &existing {
            assert_eq!(
                value["installation_id"],
                existing.expose_to_provider()["installation_id"]
            );
        }
    } else {
        assert!(result.is_err());
        assert!(!state.paths.iter().any(|path| path == "/oauth/token"));
        if case.starts_with("email-") {
            let error = result.as_ref().unwrap_err();
            assert!(!state.paths.iter().any(|path| {
                matches!(
                    path.as_str(),
                    "/api/accounts/password/verify"
                        | "/backend-api/accounts/sessions/revoke"
                        | "/oauth/authorize"
                )
            }));
            if case == "email-wrong-account" {
                assert_eq!(
                    error.public_message(),
                    Some("上游登录会话与导入用户名不一致")
                );
            } else {
                assert_eq!(error.message(), Some("GET /email-verification"));
                assert_eq!(
                    error.public_message(),
                    Some("上游要求邮箱验证码，无法仅用密码和 2FA 完成协议登录")
                );
            }
            assert_eq!(
                state
                    .paths
                    .iter()
                    .filter(|path| path.as_str() == "/log-in/password")
                    .count(),
                usize::from(case == "email-password-redirect")
            );
        }
        if matches!(
            case,
            "revoke-failed"
                | "browser-challenge"
                | "account-deactivated"
                | "untrusted-sdk-request"
                | "unexpected-login-page"
                | "http-rejected"
                | "missing-proxy"
                | "unidentified-current"
                | "proxy-failed"
        ) {
            assert!(!state.paths.iter().any(|path| path == "/oauth/authorize"));
        }
        if case == "missing-proxy" {
            assert_eq!(result.unwrap_err().kind(), ProviderAdminErrorKind::Invalid);
            assert!(state.paths.is_empty());
        } else if case == "unexpected-login-page" {
            assert_eq!(result.unwrap_err().message(), Some("GET /log-in"));
            assert!(
                !state
                    .paths
                    .iter()
                    .any(|path| path == "/api/accounts/password/verify")
            );
        } else if case == "http-rejected" {
            let error = result.unwrap_err();
            assert_eq!(
                error.message(),
                Some("POST /api/auth/signin/openai: HTTP 403 Forbidden")
            );
            assert_eq!(state.paths.last().unwrap(), "/api/auth/signin/openai");
        } else if case == "account-deactivated" {
            let error = result.unwrap_err();
            assert_eq!(
                error.public_message(),
                Some("OpenAI 账号已停用，无法完成协议登录")
            );
            assert!(
                error
                    .message()
                    .unwrap()
                    .contains("code=account_deactivated")
            );
            assert!(state.revoked.is_empty());
        } else if case == "untrusted-sdk-request" {
            assert!(
                !state
                    .paths
                    .iter()
                    .any(|path| path == "/backend-api/sentinel/req")
            );
            assert!(
                !state
                    .paths
                    .iter()
                    .any(|path| path == "/api/accounts/password/verify")
            );
        }
    }
    assert!(!state.revoked.iter().any(|id| id == "us_current"));
    assert_eq!(
        *browser.closed.lock().unwrap(),
        usize::from(case != "missing-proxy")
    );
}

async fn read_request(stream: &mut (impl AsyncRead + Unpin)) -> (String, String) {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(stream.read_u8().await.unwrap());
        assert!(bytes.len() < 64 * 1024);
    }
    let head = String::from_utf8(bytes).unwrap();
    let length = head
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or_default();
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (head, String::from_utf8(body).unwrap())
}

fn respond(case: &str, observed: &Mutex<State>, url: &Url, head: &str, body: &str) -> String {
    let mut state = observed.lock().unwrap();
    state.paths.push(url.path().to_owned());
    let asset = match url.path() {
        "/backend-api/sentinel/sdk.js" => Some(include_str!("sentinel-loader.js")),
        "/sentinel/synthetic/sdk.js" => Some(include_str!("sentinel-sdk.js")),
        _ => None,
    };
    if let Some(body) = asset {
        let body = if case == "untrusted-sdk-request" {
            body.replace(
                "https://sentinel.openai.com/backend-api/sentinel/req",
                "https://example.invalid/blocked",
            )
        } else {
            body.to_owned()
        };
        return format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/javascript\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
    if case == "account-deactivated" && url.path() == "/api/accounts/mfa/verify" {
        let body = json!({"error": {"code": "account_deactivated", "message": "synthetic-sensitive-body"}}).to_string();
        return format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
    if case == "http-rejected" && url.path() == "/api/auth/signin/openai" {
        let body = "synthetic-sensitive-body";
        return format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
    if case == "email-password-redirect" && url.path() == "/log-in/password" {
        return "HTTP/1.1 302 Found\r\nLocation: /email-verification\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned();
    }
    let token = || {
        let claims = json!({"exp": chrono::Utc::now().timestamp() + 3600, "email": "fixture@example.invalid", "https://api.openai.com/auth": {
            "chatgpt_account_id": "workspace_fixture", "chatgpt_user_id": "user_fixture", "chatgpt_plan_type": "plus",
        }});
        format!(
            "e30.{}.synthetic",
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    };
    let response = match url.path() {
        "/" | "/log-in" | "/log-in/password" | "/email-verification" | "/web-callback" => {
            Value::Null
        }
        "/api/auth/providers" => json!({}),
        "/api/auth/csrf" => json!({"csrfToken": "synthetic-csrf"}),
        "/api/auth/signin/openai" => json!({"url": match case {
            "unexpected-login-page" => "https://auth.openai.com/log-in",
            "json-continuation" => "https://auth.openai.com/start-login",
            case if case.starts_with("email-") => "https://auth.openai.com/email-verification",
            _ => "https://auth.openai.com/log-in/password",
        }}),
        "/api/accounts/client_auth_session_dump" => {
            let mut session = json!({
                "email": if case == "email-wrong-account" { "other@example.invalid" } else { "fixture@example.invalid" },
                "email_verification_mode": if case == "email-otp-required" { "post_signup" } else { "passwordless_login" },
                "reauth": case == "email-reauth",
            });
            if case != "email-password-without-methods" {
                session["login_methods"] = json!([{"type": if case == "email-password-unavailable" { "email_otp" } else { "password" }}]);
            } else {
                session.as_object_mut().unwrap().remove("email");
                session["username"] = json!({"kind": "email", "value": "fixture@example.invalid"});
            }
            json!({"client_auth_session": session})
        }
        "/start-login" => {
            json!({"method": "GET", "continue_url": "https://auth.openai.com/log-in/password"})
        }
        "/backend-api/sentinel/req" => {
            json!({"token": "synthetic-sentinel", "turnstile": {"required": case == "browser-challenge"}, "so": {"required": case == "success"}})
        }
        "/api/accounts/password/verify" => {
            if case == "success" {
                assert!(head.to_lowercase().contains("openai-sentinel-so-token:"));
            }
            assert_eq!(
                serde_json::from_str::<Value>(body).unwrap()["password"],
                "synthetic-password"
            );
            json!({"page": {"type": "mfa_challenge"}, "oai-client-auth-session": {"mfa_challenge_factors": [{"id": "totp_fixture", "type": "totp"}]}})
        }
        "/api/accounts/mfa/issue_challenge" => json!({}),
        "/api/accounts/mfa/verify" => {
            let value: Value = serde_json::from_str(body).unwrap();
            assert_eq!(value["code"].as_str().unwrap().len(), 6);
            json!({"continue_url": "https://chatgpt.com/web-callback"})
        }
        "/api/auth/session" => json!({"accessToken": token()}),
        "/backend-api/accounts/sessions" => {
            assert!(
                head.to_lowercase()
                    .contains("chatgpt-account-id: workspace_fixture")
            );
            let mut devices = vec![
                json!({"session_id": "us_current", "is_current_device": case != "unidentified-current"}),
            ];
            for id in ["us_other_1", "us_other_2"] {
                if case == "revoke-failed" || !state.revoked.iter().any(|revoked| revoked == id) {
                    devices.push(json!({"session_id": id, "is_current_device": false}));
                }
            }
            json!({"devices": devices})
        }
        "/backend-api/accounts/sessions/revoke" => {
            let value: Value = serde_json::from_str(body).unwrap();
            let id = value["session_id"].as_str().unwrap();
            assert_ne!(id, "us_current");
            state.revoked.push(id.to_owned());
            json!({"revoked_unified_sessions": [id], "revoked_app_sessions": []})
        }
        "/oauth/authorize" => {
            assert_eq!(
                state.revoked.len(),
                if case.starts_with("auto-relogin") {
                    0
                } else {
                    2
                }
            );
            if case == "auto-relogin-failed" {
                return "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_owned();
            }
            let query: BTreeMap<_, _> = url.query_pairs().collect();
            state.oauth_state = query["state"].to_string();
            if matches!(
                case,
                "success"
                    | "account-form"
                    | "account-form-reversed"
                    | "account-entities"
                    | "account-stream"
                    | "multiple-account-sessions"
            ) {
                let body = match case {
                    "account-form" => r#"<form><input type="hidden" name="session_id" value="us_current"/></form>"#.to_owned(),
                    "account-form-reversed" => r#"<form><input value='us_current' type='hidden' name='session_id'/></form>"#.to_owned(),
                    "account-entities" => r#"{&quot;session_id&quot;:&quot;us_current&quot;}"#.to_owned(),
                    "account-stream" => r#"<script>["session_id",17,"us_current-",17]</script><form><button value="us_current-">Continue</button></form>"#.to_owned(),
                    "multiple-account-sessions" => r#"["us_current","us_other"]"#.to_owned(),
                    _ => json!({"session_id": "us_current"}).to_string(),
                };
                return format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
            let callback = format!(
                "http://127.0.0.1:1455/auth/callback?code=synthetic-code&state={}",
                if case == "wrong-state" {
                    "wrong-state"
                } else {
                    &state.oauth_state
                }
            );
            return format!(
                "HTTP/1.1 302 Found\r\nLocation: {callback}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
        }
        "/api/accounts/session/select" => {
            let value: Value = serde_json::from_str(body).unwrap();
            assert_eq!(
                value["session_id"],
                if case == "account-stream" {
                    "us_current-"
                } else {
                    "us_current"
                }
            );
            json!({"continue_url": "https://auth.openai.com/consent", "oai-client-auth-session": {"workspaces": [{"id": "workspace_fixture"}]}})
        }
        "/api/accounts/workspace/select" => {
            let value: Value = serde_json::from_str(body).unwrap();
            assert_eq!(value["workspace_id"], "workspace_fixture");
            json!({"continue_url": format!("http://127.0.0.1:1455/auth/callback?code=synthetic-code&state={}", state.oauth_state)})
        }
        "/oauth/token" => {
            let fields: BTreeMap<_, _> = url::form_urlencoded::parse(body.as_bytes()).collect();
            assert_eq!(fields["code"], "synthetic-code");
            assert!(fields["code_verifier"].len() >= 43);
            let mut id_token = token();
            if case == "auto-relogin-id-fallback" {
                let payload = URL_SAFE_NO_PAD
                    .decode(id_token.split('.').nth(1).unwrap())
                    .unwrap();
                let mut claims: Value = serde_json::from_slice(&payload).unwrap();
                claims["https://api.openai.com/auth"]
                    .as_object_mut()
                    .unwrap()
                    .remove("chatgpt_user_id");
                id_token = format!(
                    "e30.{}.synthetic",
                    URL_SAFE_NO_PAD.encode(claims.to_string())
                );
            }
            json!({"access_token": token(), "refresh_token": "synthetic-refresh-token", "id_token": id_token})
        }
        path => panic!("unexpected protocol path: {path}"),
    };
    let body = response.to_string();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
