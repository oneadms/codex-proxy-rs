//! 验证 Bundle 装配后的 analytics 生命周期与 OAuth、API Key 资格边界

use super::*;
use gateway_core::task::WorkerDaemonContext;
use provider_openai::{config::CodexMetricsExporter, credential::ResponsesTransport};

#[tokio::test]
async fn initialized_analytics_only_reports_hosted_tools_with_codex_oauth() {
    for api_key in [false, true] {
        let server = MockServer::start().await;
        let store = Arc::new(MemoryAccountStore::default());
        let account_id = "acct_initialized_analytics";
        if api_key {
            store
                .seed_api_key(account_id, server.uri(), ResponsesTransport::Http)
                .await;
        } else {
            store
                .seed_oauth_credential(ImportCodexOAuthCredential {
                    account_id: account_id.into(),
                    name: "analytics".into(),
                    secret: secret("at-selected-analytics"),
                    verified_account: profile("chatgpt-selected-analytics"),
                    next_refresh_at: Some(Utc::now() + chrono::Duration::minutes(30)),
                    enabled: true,
                })
                .await;
        }
        let mut body = String::new();
        for kind in ["added", "done"] {
            let event = json!({"type":format!("response.output_item.{kind}"), "item":{
                "type":"web_search_call", "id":"ws_initialized", "status":"completed",
                "action":{"type":"search", "query":"private-search"}
            }});
            body.push_str(&format!(
                "event: {}\ndata: {event}\n\n",
                event["type"].as_str().unwrap()
            ));
        }
        body.push_str(COMPLETED_SESSION_SSE);
        Mock::given(method("POST"))
            .and(path(if api_key {
                "/responses"
            } else {
                "/codex/responses"
            }))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/codex/analytics-events/events"))
            .respond_with(ResponseTemplate::new(200))
            .expect(if api_key { 0 } else { 1 })
            .mount(&server)
            .await;
        let mut config = valid_config();
        config.config.api.base_url = server.uri();
        config.config.telemetry.metrics_exporter = CodexMetricsExporter::None;
        let mut bundle = provider_openai::initialize(
            config.config.clone(),
            provider_ports_with(store, Arc::new(TestOAuthPending::default())),
            Arc::new(gateway_core::account::UpstreamInfoReader::default()),
            crate::support::javascript_runtime(),
        )
        .await
        .unwrap();
        let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object(
                "openai",
                json!({
                    "model":"gpt-5.4", "input":"private-input", "session_id":"analytics-session",
                    "turnId":"client-turn", "client_metadata":{
                        "thread_source":{"feature":"automation"}, "subagent_source":"thread_spawn",
                        "root_turn_id":"client-root-turn", "experimental_api_enabled":false
                    }
                })
                .as_object()
                .unwrap()
                .clone(),
            )
            .unwrap()
            .with_context(Map::from_iter([("use_websocket".into(), json!(false))])),
        ));
        let mut stream = bundle
            .core_provider()
            .execute(
                initialized_provider_request(operation, account_id),
                initialized_attempt_context("req_analytics", account_id),
            )
            .await
            .unwrap();
        while let Some(event) = stream.next().await {
            event.unwrap();
        }
        let worker = bundle
            .take_worker_contributions()
            .into_iter()
            .find_map(|contribution| match contribution {
                WorkerContribution::Registration(registration)
                    if registration.id.owner() == "openai-telemetry" =>
                {
                    match registration.runnable {
                        WorkerRunnable::Daemon { task, .. } => Some(task),
                        WorkerRunnable::Scheduled { .. } => None,
                    }
                }
                _ => None,
            })
            .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        tokio::time::timeout(
            Duration::from_secs(3),
            worker.run(WorkerDaemonContext::new(cancellation)),
        )
        .await
        .unwrap()
        .unwrap();
        let requests = server.received_requests().await.unwrap();
        let analytics: Vec<_> = requests
            .iter()
            .filter(|request| request.url.path() == "/codex/analytics-events/events")
            .collect();
        assert_eq!(analytics.len(), usize::from(!api_key));
        if let Some(request) = analytics.first() {
            assert_eq!(
                request.headers["authorization"],
                "Bearer at-selected-analytics"
            );
            assert_eq!(
                request.headers["chatgpt-account-id"],
                "chatgpt-selected-analytics"
            );
            let payload: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(payload["events"][0]["event_type"], "codex_web_search_event");
            let model = requests
                .iter()
                .find(|request| request.url.path() == "/codex/responses")
                .unwrap();
            let body = zstd::stream::decode_all(std::io::Cursor::new(&model.body)).unwrap();
            let outbound: Value = serde_json::from_slice(&body).unwrap();
            let params = &payload["events"][0]["event_params"];
            assert_eq!(params["turn_id"], "client-turn");
            assert_eq!(params["turn_id"], outbound["turnId"]);
            assert_eq!(params["turn_id"], outbound["client_metadata"]["turn_id"]);
            for field in ["thread_source", "subagent_source", "root_turn_id"] {
                assert_eq!(params[field], outbound["client_metadata"][field]);
            }
            assert_eq!(
                params["app_server_client"]["experimental_api_enabled"],
                false
            );
            assert!(!payload.to_string().contains("private-"));
        }
    }
}
