//! 验证真实托管工具事件、出站身份、账号出口、队列和 analytics 故障隔离

use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::operation::{GenerateRequest, ProtocolPayload};

const ANALYTICS_PATH: &str = "/codex/analytics-events/events";
const TURN_METADATA: &str = r#"{"thread_id":"019a8c90-38ff-7000-8000-000000000001","session_id":"019a8c90-38ff-7000-8000-000000000002","turn_id":"019a8c90-38ff-7000-8000-000000000003"}"#;

pub(super) fn analytics_settings() -> CodexTelemetrySettings {
    CodexTelemetrySettings {
        metrics_exporter: CodexMetricsExporter::None,
        ..CodexTelemetrySettings::default()
    }
}

pub(super) fn context(account: &str) -> CodexRequestContext<'_> {
    let mut context = request_context("analytics-request", Some(account));
    context.installation_id = Some("selected-account-installation");
    context.turn_metadata = Some(TURN_METADATA);
    context
}

pub(super) fn events_for(items: &[Value]) -> Vec<Value> {
    let mut events = vec![json!({"type":"response.created","response":{"id":"resp_hosted"}})];
    for item in items {
        let mut started = item.clone();
        started["status"] = json!("in_progress");
        events.push(json!({"type":"response.output_item.added","item":started}));
        events.push(json!({"type":"response.output_item.done","item":item}));
    }
    events.push(serde_json::from_str(&completed_websocket_response("resp_hosted", 2, 1)).unwrap());
    events
}

fn search(id: &str) -> Value {
    json!({"type":"web_search_call","id":id,"status":"completed", "action":{
        "type":"search", "queries":["private-search-one", "private-search-two"]
    }})
}

pub(super) fn sse(events: &[Value]) -> String {
    events
        .iter()
        .map(|event| {
            format!(
                "event: {}\ndata: {event}\n\n",
                event["type"].as_str().unwrap()
            )
        })
        .collect()
}

pub(super) async fn mount_analytics(server: &MockServer, status: u16) {
    Mock::given(method("POST"))
        .and(path(ANALYTICS_PATH))
        .respond_with(ResponseTemplate::new(status))
        .mount(server)
        .await;
}

pub(super) async fn payloads(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.url.path() == ANALYTICS_PATH)
        .map(|request| {
            assert_eq!(request.headers["content-type"], "application/json");
            assert!(!request.headers.contains_key("cookie"));
            assert!(!request.headers.contains_key("x-api-key"));
            assert!(!request.headers.contains_key("x-private-header"));
            let payload: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(payload.as_object().unwrap().len(), 1);
            assert_eq!(payload["events"].as_array().unwrap().len(), 1);
            payload["events"][0].clone()
        })
        .collect()
}

fn http_request(headers: &[(&str, &str)]) -> CodexResponsesRequest {
    let payload = ProtocolPayload::json_object(
        "openai",
        codex_request("gpt-test", "private-model-prompt", Vec::new())
            .body()
            .clone(),
    )
    .unwrap()
    .with_context(Map::from_iter([
        ("use_websocket".into(), json!(false)),
        (
            "opaque_request_headers".into(),
            json!(
                headers
                    .iter()
                    .map(|(name, value)| [name.to_string(), STANDARD.encode(value)])
                    .collect::<Vec<_>>()
            ),
        ),
    ]));
    provider_openai::encode_generate_request(
        &GenerateRequest::from_protocol_payload(payload),
        "gpt-test",
        None,
    )
    .unwrap()
}

pub(super) async fn run(client: &CodexBackendClient, context: CodexRequestContext<'_>) -> String {
    let request = http_request(&[("x-private-header", "private-header-value")]);
    let mut response = client
        .create_response_stream(&request, context)
        .await
        .unwrap();
    let mut raw = Vec::new();
    while let Some(chunk) = response.body.next().await {
        raw.extend_from_slice(&chunk.unwrap());
    }
    String::from_utf8(raw).unwrap()
}

#[tokio::test]
async fn http_hosted_events_use_official_envelope_scoped_identity_and_selected_auth() {
    let body = sse(&events_for(&[
        search("ws_1"),
        json!({"type":"image_generation_call","id":"ig_1","status":"failed", "revised_prompt":"private-revised-prompt", "result":"private-image-base64"}),
    ]));
    let upstream = http_response(&body).await;
    mount_analytics(&upstream, 200).await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    assert_eq!(run(&client, context("selected-account")).await, body);
    flush(task.as_ref()).await.unwrap();
    let events = payloads(&upstream).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["event_type"], "codex_web_search_event");
    let params = &events[0]["event_params"];
    assert_eq!(params["query_count"], 2);
    assert_eq!(params["query_present"], true);
    assert_eq!(params["web_search_action"], "search");
    assert_eq!(params["originating_response_id"], "resp_hosted");
    assert_eq!(params["tool_event_type"], "model_tool_call");
    assert_eq!(params["terminal_status"], "completed");
    assert_eq!(params["app_server_client"]["rpc_transport"], "in_process");
    assert_eq!(params["runtime"]["codex_rs_version"], "1.2.3");
    assert!(
        params["completed_at_ms"].as_u64().unwrap() >= params["started_at_ms"].as_u64().unwrap()
    );
    let requests = upstream.received_requests().await.unwrap();
    let model = requests
        .iter()
        .find(|request| request.url.path() == "/codex/responses")
        .unwrap();
    let wire_metadata: Value =
        serde_json::from_str(model.headers["x-codex-turn-metadata"].to_str().unwrap()).unwrap();
    for key in ["thread_id", "session_id", "turn_id"] {
        assert_eq!(params[key], wire_metadata[key]);
    }
    let original: Value = serde_json::from_str(TURN_METADATA).unwrap();
    assert_ne!(params["thread_id"], original["thread_id"]);
    assert_eq!(params["turn_id"], original["turn_id"]);
    for request in requests
        .iter()
        .filter(|request| request.url.path() == ANALYTICS_PATH)
    {
        assert_eq!(request.headers["authorization"], "Bearer access-token");
        assert_eq!(request.headers["chatgpt-account-id"], "selected-account");
        assert_eq!(request.headers["originator"], model.headers["originator"]);
        assert_eq!(request.headers["user-agent"], model.headers["user-agent"]);
        let body = String::from_utf8_lossy(&request.body);
        assert!(!body.contains("private-"));
        assert!(!body.contains("access-token"));
        assert!(!body.contains("selected-account"));
    }
    assert_eq!(events[1]["event_type"], "codex_image_generation_event");
    assert_eq!(events[1]["event_params"]["terminal_status"], "failed");
    assert_eq!(events[1]["event_params"]["failure_kind"], "tool_error");
    assert_eq!(events[1]["event_params"]["revised_prompt_present"], true);
    assert_eq!(
        events[1]["event_params"]["transparent_background"],
        Value::Null
    );
}

#[tokio::test]
async fn duplicate_history_missing_starts_and_incomplete_tools_do_not_create_extra_events() {
    let mut events = events_for(&[search("ws_once")]);
    let terminal = events.pop().unwrap();
    events.push(json!({"type":"response.output_item.done", "item":search("ws_once")}));
    events.push(json!({"type":"response.output_item.done", "item":search("ws_no_start")}));
    events.push(json!({"type":"response.output_item.added", "item":search("ws_never_done")}));
    events.push(
        json!({"type":"response.output_item.added", "item":{"id":"fc_1","type":"function_call"}}),
    );
    events.push(
        json!({"type":"response.output_item.done", "item":{"id":"fc_1","type":"function_call"}}),
    );
    events.push(terminal);
    events.push(json!({"type":"response.output_item.done", "item":search("ws_never_done")}));
    let body = sse(&events);
    let upstream = http_response(&body).await;
    mount_analytics(&upstream, 200).await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    for _ in 0..2 {
        assert_eq!(run(&client, context("account-one")).await, body);
    }
    let mut other = context("account-two");
    other.installation_id = Some("other-account-installation");
    run(&client, other).await;
    flush(task.as_ref()).await.unwrap();
    let events = payloads(&upstream).await;
    assert_eq!(events.len(), 2);
    assert_ne!(
        events[0]["event_params"]["thread_id"],
        events[1]["event_params"]["thread_id"]
    );
    assert!(
        events
            .iter()
            .all(|event| event["event_params"]["item_id"] == "ws_once")
    );
}

#[tokio::test]
async fn analytics_honors_its_own_opt_out_and_requires_correlated_identity() {
    let upstream = http_response(&sse(&events_for(&[search("ws_disabled")]))).await;
    for settings in [
        CodexTelemetrySettings {
            enabled: false,
            ..analytics_settings()
        },
        CodexTelemetrySettings {
            analytics_enabled: false,
            ..analytics_settings()
        },
        analytics_settings(),
    ] {
        let missing_identity = settings.enabled && settings.analytics_enabled;
        let (client, task) = client(&upstream.uri(), settings);
        run(
            &client,
            if missing_identity {
                request_context("missing", Some("account"))
            } else {
                context("account")
            },
        )
        .await;
        flush(task.as_ref()).await.unwrap();
    }
    assert!(payloads(&upstream).await.is_empty());
    let metrics = collector(200).await;
    let (client, task) = client(
        &upstream.uri(),
        CodexTelemetrySettings {
            analytics_enabled: false,
            ..settings(&metrics)
        },
    );
    run(&client, context("metrics-only-account")).await;
    flush(task.as_ref()).await.unwrap();
    assert_eq!(collected(&metrics).await.len(), 1);
    assert!(payloads(&upstream).await.is_empty());
}

#[tokio::test]
async fn analytics_queue_is_bounded_without_blocking_the_model_stream() {
    let items: Vec<_> = (0..300)
        .map(|index| search(&format!("ws_{index}")))
        .collect();
    let body = sse(&events_for(&items));
    let upstream = http_response(&body).await;
    mount_analytics(&upstream, 200).await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    assert_eq!(run(&client, context("account")).await, body);
    assert!(payloads(&upstream).await.is_empty());
    flush(task.as_ref()).await.unwrap();
    assert_eq!(payloads(&upstream).await.len(), 256);
}

#[tokio::test]
async fn live_worker_delivers_before_shutdown_and_does_not_replay_failed_posts() {
    let body = sse(&events_for(&[search("ws_failure")]));
    let upstream = http_response(&body).await;
    mount_analytics(&upstream, 503).await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    let cancellation = CancellationToken::new();
    let worker_context = WorkerDaemonContext::new(cancellation.clone());
    let worker = tokio::spawn(async move { task.run(worker_context).await });
    assert_eq!(run(&client, context("account")).await, body);
    timeout(Duration::from_secs(2), async {
        while payloads(&upstream).await.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(run(&client, context("account")).await, body);
    assert_eq!(payloads(&upstream).await.len(), 1);
    cancellation.cancel();
    timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn analytics_uses_the_selected_proxy_and_preserves_product_attribution() {
    use gateway_core::{
        account::{CredentialRevision, OutboundProxy, ProviderAccount, ProviderAccountId},
        routing::ProviderKind,
    };
    let proxy = http_response(&sse(&events_for(&[search("ws_proxy")]))).await;
    mount_analytics(&proxy, 200).await;
    let (base, task) = client("http://upstream.invalid/backend-api", analytics_settings());
    // 账号出口接收 absolute-form 请求，模型和 analytics 必须使用同一代理
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(sse(&events_for(&[search("ws_proxy")])), "text/event-stream"),
        )
        .mount(&proxy)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/analytics-events/events"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&proxy)
        .await;
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_analytics_proxy").unwrap(),
        ProviderKind::new("openai").unwrap(),
        "proxy".into(),
        None,
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_outbound_proxy(Some(OutboundProxy::parse(&proxy.uri()).unwrap()));
    let client = base.for_account(&account).unwrap();
    let request = http_request(&[("x-openai-product-sku", "codex-product-a")]);
    let mut response = client
        .create_response_stream(&request, context("selected-proxy-account"))
        .await
        .unwrap();
    while let Some(chunk) = response.body.next().await {
        chunk.unwrap();
    }
    flush(task.as_ref()).await.unwrap();
    let requests = proxy.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let exported = requests
        .iter()
        .find(|request| request.url.path().ends_with(ANALYTICS_PATH))
        .unwrap();
    assert_eq!(exported.url.host_str(), Some("upstream.invalid"));
    assert_eq!(exported.headers["authorization"], "Bearer access-token");
    assert_eq!(
        exported.headers["chatgpt-account-id"],
        "selected-proxy-account"
    );
    assert_eq!(exported.headers["x-openai-product-sku"], "codex-product-a");
}

#[tokio::test]
async fn pooled_websocket_keeps_analytics_bound_to_each_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_codex_test_websocket(stream).await;
        let mut frames = Vec::new();
        for index in 0..2 {
            let frame = websocket.next().await.unwrap().unwrap();
            frames.push(serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap());
            for event in events_for(&[search(&format!("ws_pool_{index}"))]) {
                websocket
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
        }
        let _ = websocket.next().await;
        drop(websocket);
        let mut delivered = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = crate::transport::read_http_request_with_body(&mut stream).await;
            let split = request
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
                .unwrap();
            let headers = String::from_utf8_lossy(&request[..split]);
            assert!(headers.starts_with("POST /codex/analytics-events/events "));
            assert!(headers.contains("authorization: Bearer access-token"));
            delivered.push(serde_json::from_slice::<Value>(&request[split + 4..]).unwrap());
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
        }
        (delivered, frames)
    });
    let (base, task) = client(&base_url, analytics_settings());
    let pool = Arc::new(CodexWebSocketPool::new(Duration::from_secs(60)));
    let client = base.with_websocket_pool(Arc::clone(&pool));
    let mut request = pooled_websocket_request("analytics-conversation");
    request.use_websocket = true;
    let handshake_metadata = json!({
        "session_id":"handshake-session", "thread_id":"handshake-thread", "turn_id":"handshake-turn",
        "thread_source":"handshake-source", "subagent_source":"handshake-agent", "root_turn_id":"handshake-root"
    }).to_string();
    for index in 0..2 {
        request.set_client_metadata(Some(json!({
            "turnId":format!("client-turn-{index}"),
            "threadSource":if index == 0 { json!({"feature":"automation"}) } else { Value::Null },
            "subagentSource":if index == 0 { json!("thread_spawn") } else { Value::Null },
            "rootTurnId":if index == 0 { json!("client-root") } else { Value::Null }
        })));
        let mut request_context = context("ws-account");
        request_context.turn_metadata = Some(&handshake_metadata);
        let mut response = client
            .create_response_stream(&request, request_context)
            .await
            .unwrap();
        while let Some(chunk) = response.body.next().await {
            chunk.unwrap();
        }
    }
    pool.shutdown().await;
    flush(task.as_ref()).await.unwrap();
    let (delivered, frames) = timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.len(), 2);
    for (index, payload) in delivered.iter().enumerate() {
        assert_eq!(payload["events"][0]["event_type"], "codex_web_search_event");
        assert_eq!(
            payload["events"][0]["event_params"]["item_id"],
            format!("ws_pool_{index}")
        );
        for (field, alias) in [
            ("turn_id", "turnId"),
            ("thread_source", "threadSource"),
            ("subagent_source", "subagentSource"),
            ("root_turn_id", "rootTurnId"),
        ] {
            assert_eq!(
                payload["events"][0]["event_params"][field],
                frames[index]["client_metadata"][alias]
            );
        }
    }
}
