//! 缺失会话身份的出站补齐、轮次稳定性及 HTTP/WS 传输一致性回归

use super::*;

type CapturedRequestFuture = BoxFuture<'static, (reqwest::header::HeaderMap, Value)>;

pub(super) fn assert_completed_metadata(actual: Value, mut expected: Value) {
    // 只允许增加默认身份字段，其余字段仍整体比对，防止换号清理和扩展保留的断言退化
    for field in [
        "installation_id",
        "session_id",
        "thread_id",
        "window_id",
        "turn_id",
    ] {
        let value = actual[field]
            .as_str()
            .filter(|value| !value.is_empty())
            .expect("completed identity");
        expected
            .as_object_mut()
            .unwrap()
            .entry(field)
            .or_insert_with(|| json!(value));
    }
    assert_eq!(actual, expected);
}

async fn capture(
    store: &Arc<MemoryAccountStore>,
    mut body: Value,
    protocol_context: Map<String, Value>,
    websocket: bool,
    request_id: &str,
    client_key: &str,
) -> (reqwest::header::HeaderMap, Value) {
    let (base_url, capture): (String, CapturedRequestFuture) = if websocket {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut headers = None;
            let mut socket =
                crate::transport::accept_codex_test_websocket_with(socket, |request, _| {
                    headers = Some(request.headers().clone());
                })
                .await;
            let frame = socket.next().await.unwrap().unwrap();
            let body = serde_json::from_str(frame.to_text().unwrap()).unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"response.completed", "response":{
                        "id":"resp_completed", "model":"gpt-5.4", "status":"completed", "output":[]
                    }})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            (headers.unwrap(), body)
        });
        (url, Box::pin(async { task.await.unwrap() }))
    } else {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(CAPTURE_COMPLETED_SSE),
            )
            .expect(1)
            .mount(&server)
            .await;
        (
            server.uri(),
            Box::pin(async move {
                let requests = server.received_requests().await.unwrap();
                (
                    requests[0].headers.clone(),
                    captured_request_body(&requests[0]),
                )
            }),
        )
    };
    body["model"] = json!("gpt-5.4");
    let mut protocol_context = protocol_context;
    protocol_context.insert("use_websocket".to_owned(), json!(websocket));
    let needs_pool_owner = protocol_context.contains_key("downstream_websocket_connection_id");
    let mut generate = GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().unwrap().clone())
            .unwrap()
            .with_context(protocol_context),
    );
    if needs_pool_owner {
        // 直接构造的 Provider 夹具未加载宿主的锚点服务，显式提供合法的本地池归属
        generate = generate.with_provider_session_state(
            ProviderSessionState::new(
                "openai",
                Map::from_iter([
                    ("account_id".to_owned(), json!("acct_provider_contract")),
                    (
                        "conversation_id".to_owned(),
                        json!("fixture-local-conversation"),
                    ),
                    ("continuation_scope".to_owned(), json!("persisted")),
                ]),
            )
            .unwrap(),
        );
    }
    let operation = Operation::Generate(generate);
    let context = context_for_request(
        RequestAttemptContext::new(
            ModelRequestId::new(request_id).unwrap(),
            ClientApiKeyId::new(client_key).unwrap(),
        ),
        CancellationToken::new(),
    );
    let mut stream = provider_with_base_url(store, base_url)
        .execute(planned_request("openai", operation), context)
        .await
        .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    capture.await
}

#[tokio::test]
async fn missing_identity_is_stable_across_transports_and_advances_only_the_turn() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let first = json!({"input":[{"role":"user", "content":[{"type":"input_text", "text":"first question"}]}]});
    let later = json!({"input":[
        {"role":"user", "content":[{"type":"input_text", "text":"first question"}]},
        {"role":"assistant", "content":[{"type":"output_text", "text":"first answer"}]},
        {"role":"user", "content":[{"type":"input_text", "text":"next question"}]}
    ]});
    let mut results = Vec::new();
    for (body, websocket, request_id) in [
        (first.clone(), false, "req_initial"),
        (later, true, "req_next"),
        (first, false, "req_initial"),
    ] {
        let (headers, body) = capture(
            &store,
            body,
            Map::new(),
            websocket,
            request_id,
            "key_openai_contract",
        )
        .await;
        let metadata: Value =
            serde_json::from_str(headers["x-codex-turn-metadata"].to_str().unwrap()).unwrap();
        let body_metadata: Value = serde_json::from_str(
            body["client_metadata"]["x-codex-turn-metadata"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        for field in [
            "session_id",
            "thread_id",
            "window_id",
            "turn_id",
            "installation_id",
        ] {
            assert_eq!(metadata[field], body_metadata[field]);
        }
        for (flat, nested) in [
            ("x-codex-installation-id", "installation_id"),
            ("session_id", "session_id"),
            ("thread_id", "thread_id"),
            ("turn_id", "turn_id"),
            ("x-codex-window-id", "window_id"),
        ] {
            assert_eq!(body["client_metadata"][flat], metadata[nested]);
        }
        assert_eq!(
            metadata["session_id"],
            headers["session-id"].to_str().unwrap()
        );
        assert_eq!(
            metadata["thread_id"],
            headers["thread-id"].to_str().unwrap()
        );
        assert_eq!(body["prompt_cache_key"], metadata["session_id"]);
        assert_eq!(
            metadata["window_id"],
            format!("{}:0", metadata["thread_id"].as_str().unwrap())
        );
        assert!(uuid::Uuid::parse_str(metadata["session_id"].as_str().unwrap()).is_ok());
        assert!(uuid::Uuid::parse_str(metadata["turn_id"].as_str().unwrap()).is_ok());
        results.push(metadata);
    }
    assert_eq!(results[0]["session_id"], results[1]["session_id"]);
    assert_eq!(results[0]["thread_id"], results[1]["thread_id"]);
    assert_ne!(results[0]["turn_id"], results[1]["turn_id"]);
    assert_eq!(results[0]["turn_id"], results[2]["turn_id"]);
}

#[tokio::test]
async fn generated_sessions_are_isolated_by_client_key_and_account() {
    let first = Arc::new(MemoryAccountStore::default());
    let other = Arc::new(MemoryAccountStore::default());
    create_account(&first, "acct_provider_contract").await;
    create_account(&other, "acct_scope_new").await;
    let body = json!({"input":[{"role":"user", "content":[{"type":"input_text", "text":"same question"}]}]});
    let mut sessions = Vec::new();
    for (store, key) in [
        (&first, "key_openai_contract"),
        (&first, "other_key"),
        (&other, "key_openai_contract"),
    ] {
        let (headers, _) = capture(store, body.clone(), Map::new(), false, "req_same", key).await;
        sessions.push(headers["session-id"].clone());
    }
    assert_ne!(sessions[0], sessions[1]);
    assert_ne!(sessions[0], sessions[2]);
}

#[tokio::test]
async fn partial_metadata_keeps_client_turn_and_extensions_while_filling_missing_fields() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let original = json!({"turn_id":"client-turn", "window_id":"client-window", "future":{"keep":true}, "workspace":"工作区"});
    let body = json!({"input":[{"role":"user", "content":[{"type":"input_text", "text":"question"}]}],
        "prompt_cache_key":"client-cache", "client_metadata":{"x-codex-turn-metadata":original.to_string()}});
    for websocket in [false, true] {
        let (headers, body) = capture(
            &store,
            body.clone(),
            Map::new(),
            websocket,
            "req_partial",
            "key_openai_contract",
        )
        .await;
        let raw = headers["x-codex-turn-metadata"].to_str().unwrap();
        assert!(raw.is_ascii());
        let metadata: Value = serde_json::from_str(raw).unwrap();
        assert_eq!(metadata["turn_id"], "client-turn");
        assert_eq!(metadata["future"], original["future"]);
        assert_eq!(metadata["workspace"], original["workspace"]);
        assert_eq!(
            metadata["window_id"],
            headers["x-codex-window-id"].to_str().unwrap()
        );
        assert_scoped_id(metadata["window_id"].as_str().unwrap(), "client-window");
        assert_eq!(
            metadata["session_id"],
            headers["session-id"].to_str().unwrap()
        );
        let body_metadata: Value = serde_json::from_str(
            body["client_metadata"]["x-codex-turn-metadata"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata["turn_id"], body_metadata["turn_id"]);
        assert_eq!(metadata["future"], body_metadata["future"]);
        assert_eq!(body["client_metadata"]["turn_id"], "client-turn");
        assert_eq!(
            body["client_metadata"]["x-codex-window-id"],
            metadata["window_id"]
        );
    }
}

#[tokio::test]
async fn client_metadata_projects_logical_identity_without_replacing_routing_headers() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let original = json!({
        "session_id":"logical-session", "thread_id":"logical-thread", "turn_id":"client-turn",
        "window_id":"logical-window", "future":{"keep":true}
    });
    for websocket in [false, true] {
        let (headers, body) = capture(
            &store,
            json!({"input":"question", "client_metadata":{
                "x-codex-turn-metadata": original.to_string(), "future":"keep"
            }}),
            Map::from_iter([
                ("session_id".to_owned(), json!("routing-session")),
                ("thread_id".to_owned(), json!("routing-thread")),
            ]),
            websocket,
            "req_logical_metadata",
            "key_openai_contract",
        )
        .await;
        let metadata = &body["client_metadata"];
        let nested: Value =
            serde_json::from_str(metadata["x-codex-turn-metadata"].as_str().unwrap()).unwrap();
        for (field, original) in [
            ("session_id", "logical-session"),
            ("thread_id", "logical-thread"),
        ] {
            assert_scoped_id(metadata[field].as_str().unwrap(), original);
            assert_eq!(metadata[field], nested[field]);
        }
        assert_scoped_id(headers["session-id"].to_str().unwrap(), "routing-session");
        assert_scoped_id(headers["thread-id"].to_str().unwrap(), "routing-thread");
        assert_ne!(
            metadata["session_id"],
            headers["session-id"].to_str().unwrap()
        );
        assert_ne!(
            metadata["thread_id"],
            headers["thread-id"].to_str().unwrap()
        );
        assert_eq!(metadata["turn_id"], "client-turn");
        assert_eq!(metadata["x-codex-window-id"], nested["window_id"]);
        assert_eq!(metadata["future"], "keep");
        assert_eq!(nested["future"], original["future"]);
    }
}

#[tokio::test]
async fn unidentified_websocket_requests_share_identity_within_the_inbound_connection() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let mut sessions = Vec::new();
    for (index, connection) in ["connection-a", "connection-a", "connection-b"]
        .into_iter()
        .enumerate()
    {
        let body = json!({"input":[{"role":"user", "content":[{
            "type":"input_text", "text":format!("only the newest message {index}")
        }]}]});
        let (headers, _) = capture(
            &store,
            body,
            Map::from_iter([(
                "downstream_websocket_connection_id".to_owned(),
                json!(connection),
            )]),
            true,
            &format!("req_connection_{index}"),
            "key_openai_contract",
        )
        .await;
        sessions.push(headers["session-id"].clone());
    }
    assert_eq!(sessions[0], sessions[1]);
    assert_ne!(sessions[0], sessions[2]);
}

#[tokio::test]
async fn explicit_cache_key_preserves_generated_identity_across_inbound_websocket_connections() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let mut sessions = Vec::new();
    for connection in ["connection-a", "connection-b"] {
        let (headers, _) = capture(
            &store,
            json!({"input":"question", "prompt_cache_key":"stable-client-conversation"}),
            Map::from_iter([(
                "downstream_websocket_connection_id".to_owned(),
                json!(connection),
            )]),
            true,
            "req_explicit_cache",
            "key_openai_contract",
        )
        .await;
        sessions.push(headers["session-id"].clone());
    }
    assert_eq!(sessions[0], sessions[1]);
}

#[tokio::test]
async fn responses_api_key_requests_do_not_gain_codex_session_metadata() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    store
        .seed_api_key(
            "acct_provider_contract",
            server.uri(),
            provider_openai::credential::ResponsesTransport::Http,
        )
        .await;
    Mock::given(method("POST"))
        .and(path("/responses"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(CAPTURE_COMPLETED_SSE, "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let mut stream = provider_with_base_url(&store, server.uri())
        .execute(
            planned_request("openai", generate_operation()),
            context("req_api_identity", CancellationToken::new()),
        )
        .await
        .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    let body = captured_request_body(&requests[0]);
    assert!(!requests[0].headers.contains_key("session-id"));
    assert!(!requests[0].headers.contains_key("thread-id"));
    assert!(!requests[0].headers.contains_key("x-codex-turn-metadata"));
    assert!(body.get("prompt_cache_key").is_none());
    assert!(
        body["client_metadata"]
            .get("x-codex-turn-metadata")
            .is_none()
    );
    for field in ["session_id", "thread_id", "turn_id", "x-codex-window-id"] {
        assert!(body["client_metadata"].get(field).is_none());
    }
}
