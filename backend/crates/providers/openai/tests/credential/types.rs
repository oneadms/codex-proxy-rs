//! 验证 OpenAI 凭据类型的脱敏、Cookie 往返与字段约束

use chrono::Utc;
use gateway_core::account::PlaintextCredential;
use provider_openai::credential::{
    CodexAccountProfile, CodexCookie, CodexCredentialCodec, CodexCredentialData,
    CodexCredentialPrincipal, CodexOAuthCredentialData, CodexOAuthSecret,
};
use secrecy::SecretString;

#[test]
fn oauth_secret_debug_redacts_every_token() {
    let secret = CodexOAuthSecret {
        access_token: SecretString::from("access-private"),
        refresh_token: Some(SecretString::from("refresh-private")),
        id_token: Some(SecretString::from("id-private")),
    };
    let debug = format!("{secret:?}");
    for value in ["access-private", "refresh-private", "id-private"] {
        assert!(!debug.contains(value));
    }
}

#[test]
fn account_profile_debug_redacts_identity_fields() {
    let profile = CodexAccountProfile {
        email: Some("private@example.com".to_owned()),
        oauth_subject: "subject-private".to_owned(),
        poid: Some("poid-private".to_owned()),
        chatgpt_account_id: "chatgpt-private".to_owned(),
        chatgpt_user_id: "user-private".to_owned(),
        plan_type: Some("pro".to_owned()),
        access_token_expires_at: Some(Utc::now()),
    };
    let debug = format!("{profile:?}");
    assert!(!debug.contains("private@example.com"));
    assert!(!debug.contains("chatgpt-private"));
    assert!(!debug.contains("user-private"));
    assert!(debug.contains("pro"));
}

#[test]
fn plaintext_provider_schema_round_trips_login_device_and_cookie_data() {
    let device = serde_json::from_str::<serde_json::Value>(
        r#"{"id":"00000000-0000-4000-8000-000000000002","config":{"webGl2:parameters":{"37137":18446744073709552000}},"webgl2":true}"#,
    )
    .expect("device with a WebGL2 integer larger than u64");
    let data = CodexCredentialData::OAuth(CodexOAuthCredentialData {
        transport: provider_openai::credential::ResponsesTransport::PreferWebsocket,
        schema_version: 1,
        principal: Some(CodexCredentialPrincipal {
            oauth_subject: "subject-private".to_owned(),
            poid: Some("poid-private".to_owned()),
        }),
        installation_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        protocol_login_device: Some(Box::new(device.clone())),
        protocol_login: Some(Box::new(
            provider_openai::credential::CodexProtocolLoginCredentials {
                username: "fixture@example.invalid".to_owned(),
                password: "password-private".to_owned(),
                totp_secret: "totp-private".to_owned(),
            },
        )),
        access_token: "at".to_owned(),
        refresh_token: Some("rt".to_owned()),
        id_token: None,
        oauth_client_id: Some("client".to_owned()),
        oauth_scope: Some("openid profile".to_owned()),
        cookies: vec![CodexCookie {
            name: "oai-did".to_owned(),
            value: "cookie-private".to_owned(),
            domain: "chatgpt.com".to_owned(),
            path: "/".to_owned(),
            host_only: false,
            secure: true,
            expires_at: None,
        }],
    });
    let encoded = serde_json::to_value(&data).expect("serialize provider JSON");
    let decoded: CodexCredentialData =
        serde_json::from_value(encoded).expect("deserialize provider JSON");
    assert_eq!(
        decoded.oauth().unwrap().protocol_login_device.as_deref(),
        Some(&device)
    );
    assert_eq!(decoded.oauth().expect("OAuth data").schema_version, 1);
    assert_eq!(decoded.cookies()[0].name, "oai-did");
    assert!(!format!("{decoded:?}").contains("cookie-private"));
    let login = decoded.oauth().unwrap().protocol_login.as_ref().unwrap();
    assert_eq!(login.password, "password-private");
    for value in [
        "fixture@example.invalid",
        "password-private",
        "totp-private",
    ] {
        assert!(!format!("{decoded:?}").contains(value));
    }
    let credential = CodexCredentialCodec::encode_complete(decoded).expect("encode credential");
    let persisted =
        serde_json::to_vec(credential.expose_to_provider()).expect("store credential JSON");
    let credential = PlaintextCredential::new(
        serde_json::from_slice(&persisted).expect("read stored credential JSON"),
    );
    let decoded = CodexCredentialCodec::decode_complete(&credential).expect("decode credential");
    assert_eq!(
        decoded.oauth().unwrap().protocol_login_device.as_deref(),
        Some(&device)
    );
    let runtime = CodexCredentialCodec::decode(&credential).expect("decode runtime credential");
    assert_eq!(
        runtime.installation_id,
        "00000000-0000-4000-8000-000000000001"
    );
}

#[test]
fn provider_schema_requires_installation_id_and_rejects_unknown_fields() {
    let valid = serde_json::json!({
        "schema_version": 1,
        "principal": {"oauth_subject": "subject-private", "poid": null},
        "installation_id": "00000000-0000-4000-8000-000000000001",
        "access_token": "at",
        "cookies": []
    });
    serde_json::from_value::<CodexCredentialData>(valid.clone())
        .expect("valid credential baseline");

    let mut unknown = valid.clone();
    unknown["unknown_field"] = serde_json::json!(9);
    assert!(serde_json::from_value::<CodexCredentialData>(unknown).is_err());

    let mut missing_installation = valid;
    missing_installation
        .as_object_mut()
        .unwrap()
        .remove("installation_id");
    assert!(serde_json::from_value::<CodexCredentialData>(missing_installation).is_err());
}
