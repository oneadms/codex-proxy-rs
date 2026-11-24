//! 验证 Bundle 装配后的 analytics 生命周期与 OAuth、API Key 资格边界

use super::*;
use gateway_core::{
    provider_ports::ProviderWarmupPolicy,
    task::{WorkerCycleContext, WorkerDaemonContext},
};
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
        Mock::given(method("POST"))
            .and(path("/v1/metrics"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        config.config.telemetry.metrics_exporter = CodexMetricsExporter::OtlpHttp {
            endpoint: format!("{}/v1/metrics", server.uri()),
            headers: Default::default(),
        };
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
        let metrics = requests
            .iter()
            .find(|request| request.url.path() == "/v1/metrics")
            .unwrap();
        let payload: Value = serde_json::from_slice(&metrics.body).unwrap();
        let auth_modes: Vec<_> = payload["resourceMetrics"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|resource| resource["scopeMetrics"].as_array().unwrap())
            .flat_map(|scope| scope["metrics"].as_array().unwrap())
            .filter(|metric| metric["name"] == "codex.api_request")
            .flat_map(|metric| metric["sum"]["dataPoints"].as_array().unwrap())
            .flat_map(|point| point["attributes"].as_array().unwrap())
            .filter(|attribute| attribute["key"] == "auth_mode")
            .map(|attribute| attribute["value"]["stringValue"].as_str().unwrap())
            .collect();
        assert_eq!(auth_modes, [if api_key { "ApiKey" } else { "Chatgpt" }]);
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

#[tokio::test]
async fn initialized_warmup_and_model_requests_share_the_provider_telemetry_worker() {
    for (enabled, runtime_enabled) in [(false, true), (true, false), (true, true)] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/codex/responses"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(COMPLETED_SESSION_SSE, "text/event-stream"),
            )
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/metrics"))
            .respond_with(ResponseTemplate::new(200))
            .expect(u64::from(enabled))
            .mount(&server)
            .await;
        let store = Arc::new(MemoryAccountStore::default());
        let account_id = "acct_initialized_warmup_telemetry";
        store
            .seed_oauth_credential(ImportCodexOAuthCredential {
                account_id: account_id.into(),
                name: "warmup telemetry".into(),
                secret: secret("at-private-warmup"),
                verified_account: profile("chatgpt-private-warmup"),
                next_refresh_at: Some(Utc::now() + chrono::Duration::minutes(30)),
                enabled: true,
            })
            .await;
        let ports = provider_ports_with(store, Arc::new(TestOAuthPending::default()));
        let ports = ProviderStorePorts::new(
            ports.accounts(),
            ports.leases(),
            ports.session_affinity(),
            ports.session_exclusions(),
            ports.catalog_cache(),
            ports.artifact_profiles(),
            ports.credential_state(),
            ports.cooldowns(),
            Arc::new(WarmupRuntimePolicy(runtime_enabled)),
            ports.oauth_pending(),
            ports.diagnostics(),
        );
        let mut config = valid_config();
        config.config.api.base_url = server.uri();
        config.config.telemetry.enabled = enabled;
        config.config.telemetry.analytics_enabled = false;
        config.config.telemetry.metrics_exporter = CodexMetricsExporter::OtlpHttp {
            endpoint: format!("{}/v1/metrics", server.uri()),
            headers: Default::default(),
        };
        let mut bundle = provider_openai::initialize(
            config.config,
            ports,
            Arc::new(gateway_core::account::UpstreamInfoReader::default()),
            crate::support::javascript_runtime(),
        )
        .await
        .unwrap();
        let mut warmup = None;
        let mut telemetry = None;
        for contribution in bundle.take_worker_contributions() {
            if let WorkerContribution::Registration(registration) = contribution {
                match (registration.id.owner(), registration.runnable) {
                    ("openai-account-warmup", WorkerRunnable::Scheduled { task, .. }) => {
                        warmup = Some((registration.id, task));
                    }
                    ("openai-telemetry", WorkerRunnable::Daemon { task, .. }) => {
                        assert!(telemetry.replace(task).is_none());
                    }
                    _ => {}
                }
            }
        }
        let (id, warmup) = warmup.unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            warmup.run_cycle(WorkerCycleContext::new(id, None, CancellationToken::new())),
        )
        .await
        .unwrap()
        .unwrap();
        let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object(
                "openai",
                json!({"model":"gpt-5.4", "input":"private-model-input"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .unwrap()
            .with_context(Map::from_iter([("use_websocket".into(), json!(false))])),
        ));
        let mut response = bundle
            .core_provider()
            .execute(
                initialized_provider_request(operation, account_id),
                initialized_attempt_context("req_shared_telemetry", account_id),
            )
            .await
            .unwrap();
        while let Some(event) = response.next().await {
            event.unwrap();
        }
        // 请求只采样，全部指标由同一个受监督任务导出
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() == "/codex/responses")
        );
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        tokio::time::timeout(
            Duration::from_secs(3),
            telemetry
                .unwrap()
                .run(WorkerDaemonContext::new(cancellation)),
        )
        .await
        .unwrap()
        .unwrap();
        let requests = server.received_requests().await.unwrap();
        let exports: Vec<_> = requests
            .iter()
            .filter(|request| request.url.path() == "/v1/metrics")
            .collect();
        assert_eq!(exports.len(), usize::from(enabled));
        if let Some(export) = exports.first() {
            assert!(!export.headers.contains_key("authorization"));
            let payload: Value = serde_json::from_slice(&export.body).unwrap();
            let api = payload["resourceMetrics"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|resource| resource["scopeMetrics"].as_array().unwrap())
                .flat_map(|scope| scope["metrics"].as_array().unwrap())
                .find(|metric| metric["name"] == "codex.api_request")
                .unwrap();
            let requests: i64 = api["sum"]["dataPoints"]
                .as_array()
                .unwrap()
                .iter()
                .map(|point| point["asInt"].as_i64().unwrap())
                .sum();
            assert_eq!(requests, 1 + i64::from(runtime_enabled));
            assert!(!payload.to_string().contains("private-"));
        }
    }
}

struct WarmupRuntimePolicy(bool);

impl ProviderRuntimePolicyPort for WarmupRuntimePolicy {
    fn load_openai_telemetry_enabled(&self) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(self.0) })
    }

    fn load_refresh_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderRefreshPolicy, ProviderStoreError>> {
        TestRuntimePolicy.load_refresh_policy()
    }

    fn load_warmup_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderWarmupPolicy, ProviderStoreError>> {
        Box::pin(async { ProviderWarmupPolicy::try_new(true, "* * * * *", Some("gpt-5.4".into())) })
    }

    fn claim_warmup_slot<'a>(
        &'a self,
        _timezone: gateway_core::time::DeploymentTimeZone,
        _slot: chrono::NaiveDateTime,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(true) })
    }
}
