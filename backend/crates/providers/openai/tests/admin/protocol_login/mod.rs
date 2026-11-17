//! 使用隔离 TLS 代理验证登录、会话清理、授权回调及拒绝路径

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_admin::{
    model::provider_credentials::{PrepareCredentialImport, ProviderDocument},
    ports::provider::ProviderAdminErrorKind,
};
use gateway_core::account::{OpaqueProviderData, OutboundProxy, RequestLocation};
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
    let scenario = case.to_owned();
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
    let bundle = provider_openai::initialize(
        config.config.clone(),
        super::provider_ports(),
        Arc::new(gateway_core::account::UpstreamInfoReader::default()),
        crate::support::javascript_runtime(),
    )
    .await
    .unwrap();
    let document = json!({
        "authentication_kind": "protocol_login", "username": "fixture@example.invalid",
        "password": "synthetic-password", "totpSecret": "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ",
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
    let state = state.lock().unwrap();
    if matches!(
        case,
        "success"
            | "account-form"
            | "account-form-reversed"
            | "account-entities"
            | "account-stream"
            | "json-continuation"
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
    } else {
        assert!(result.is_err());
        assert!(!state.paths.iter().any(|path| path == "/oauth/token"));
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
        "/" | "/log-in" | "/log-in/password" | "/web-callback" => Value::Null,
        "/api/auth/providers" => json!({}),
        "/api/auth/csrf" => json!({"csrfToken": "synthetic-csrf"}),
        "/api/auth/signin/openai" => json!({"url": match case {
            "unexpected-login-page" => "https://auth.openai.com/log-in",
            "json-continuation" => "https://auth.openai.com/start-login",
            _ => "https://auth.openai.com/log-in/password",
        }}),
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
            assert_eq!(state.revoked.len(), 2);
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
            json!({"access_token": token(), "refresh_token": "synthetic-refresh-token", "id_token": token()})
        }
        path => panic!("unexpected protocol path: {path}"),
    };
    let body = response.to_string();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}
