//! 验证账号设备身份在 HTTP、WebSocket、协议元数据与请求头中保持一致

use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::{
    account::{CredentialRevision, ProviderAccount, ProviderAccountId},
    operation::{GenerateRequest, ProtocolPayload},
    routing::ProviderKind,
};
use provider_openai::transport::encode_generate_request;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

fn account() -> ProviderAccount {
    ProviderAccount::new(
        ProviderAccountId::new("acct_device_identity").unwrap(),
        ProviderKind::new("openai").unwrap(),
        "device account".to_owned(),
        Some("upstream-account".to_owned()),
        "oauth".to_owned(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
}

fn request(websocket: bool) -> CodexResponsesRequest {
    let turn = json!({"deviceId":"client-device", "machine_id":"client-machine", "oai-did":"client-device", "turn_id":"client-turn"});
    let payload = ProtocolPayload::json_object("openai", json!({
        "model":"gpt-test", "device_id": {"untrusted":"client-device"},
        "machineId":"client-machine", "oai-did":"client-device",
        "input":[{"role":"user", "content":"business-device", "metadata":{"device_id":"business-device"}}],
        "client_metadata":{
            "device_id":"client-device", "ws_request_header_oa-device-id":"client-device",
            "oai-did":"client-device", "ws_request_header_oai-did":"client-device",
            "telemetry":json!({"device_id":"client-device", "oai-did":"client-device"}).to_string(),
            "x-codex-turn-metadata":turn.to_string()
        },
        "turnMetadata":turn
    }).as_object().unwrap().clone()).unwrap().with_context(Map::from_iter([
        ("use_websocket".to_owned(), json!(websocket)),
        ("turn_metadata".to_owned(), json!(turn.to_string())),
        ("opaque_request_headers".to_owned(), json!([
            ["X-Device-Id", STANDARD.encode("client-device")],
            ["oa-device-id", STANDARD.encode("client-device")],
            ["oai-did", STANDARD.encode("client-device")],
            ["machine-id", STANDARD.encode("client-machine")],
            ["x-codex-turn-metadata", STANDARD.encode(turn.to_string())]
        ])),
    ]));
    encode_generate_request(
        &GenerateRequest::from_protocol_payload(payload),
        "gpt-test",
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn account_devices_scope_identity_containers_over_http_and_websocket() {
    let mut ids = Vec::new();
    for websocket in [false, true] {
        let (url, capture): (
            String,
            futures::future::BoxFuture<'static, (reqwest::header::HeaderMap, Value)>,
        ) = if websocket {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut headers = None;
                let mut socket = accept_codex_test_websocket_with(socket, |request, _| {
                    headers = Some(request.headers().clone());
                })
                .await;
                let frame = socket.next().await.unwrap().unwrap();
                let body = serde_json::from_str(frame.to_text().unwrap()).unwrap();
                socket
                    .send(Message::Text(
                        completed_websocket_response("resp_device", 1, 1).into(),
                    ))
                    .await
                    .unwrap();
                (headers.unwrap(), body)
            });
            (url, Box::pin(async move { task.await.unwrap() }))
        } else {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string(format!(
                            "event: response.completed\ndata: {}\n\n",
                            completed_websocket_response("resp_device", 1, 1)
                        )),
                )
                .mount(&server)
                .await;
            (
                server.uri(),
                Box::pin(async move {
                    let requests = server.received_requests().await.unwrap();
                    let request = &requests[0];
                    let bytes =
                        zstd::stream::decode_all(std::io::Cursor::new(&request.body)).unwrap();
                    (
                        request.headers.clone(),
                        serde_json::from_slice(&bytes).unwrap(),
                    )
                }),
            )
        };
        let client = CodexBackendClient::new(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            url,
            test_wire_profile(),
        )
        .for_account(&account())
        .unwrap();
        client
            .create_response(
                &request(websocket),
                request_context("device-request", Some("upstream-account")),
            )
            .await
            .unwrap();
        let (headers, body) = capture.await;
        let id = headers["x-device-id"].to_str().unwrap();
        assert!(uuid::Uuid::parse_str(id).is_ok());
        assert_ne!(id, "client-device");
        assert_eq!(headers["oa-device-id"], id);
        assert_eq!(headers["oai-did"], id);
        assert_eq!(headers["machine-id"], id);
        assert_eq!(body["device_id"], id);
        assert_eq!(body["machineId"], id);
        assert_eq!(body["oai-did"], id);
        let metadata = &body["client_metadata"];
        assert_eq!(metadata["device_id"], id);
        assert_eq!(metadata["ws_request_header_oa-device-id"], id);
        assert_eq!(metadata["oai-did"], id);
        assert_eq!(metadata["ws_request_header_oai-did"], id);
        let telemetry: Value =
            serde_json::from_str(metadata["telemetry"].as_str().unwrap()).unwrap();
        assert_eq!(telemetry["device_id"], id);
        assert_eq!(telemetry["oai-did"], id);
        let header_turn: Value =
            serde_json::from_str(headers["x-codex-turn-metadata"].to_str().unwrap()).unwrap();
        let body_turn: Value =
            serde_json::from_str(metadata["x-codex-turn-metadata"].as_str().unwrap()).unwrap();
        for turn in [&header_turn, &body_turn, &body["turnMetadata"]] {
            assert_eq!(turn["deviceId"], id);
            assert_eq!(turn["machine_id"], id);
            assert_eq!(turn["oai-did"], id);
            assert_eq!(turn["turn_id"], "client-turn");
        }
        assert_eq!(body["input"], request(websocket).body()["input"]);
        assert!(!body.to_string().contains("client-device"));
        assert!(!body.to_string().contains("client-machine"));
        ids.push(id.to_owned());
    }
    assert_eq!(ids[0], ids[1]);
}
