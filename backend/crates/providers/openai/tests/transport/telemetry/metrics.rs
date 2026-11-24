//! 验证 OTLP JSON、Delta 聚合、账号出口与指标故障隔离

use super::*;

#[tokio::test]
async fn http_metrics_export_delta_histograms_without_request_content() {
    let collector = collector(200).await;
    let body = completed_sse();
    let upstream = http_response(&body).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    assert_eq!(run_http(&client).await.body, body);
    flush(task.as_ref()).await.unwrap();
    let first = collected(&collector).await;
    assert_eq!(first.len(), 1);
    let resource = attributes(&first[0]["resourceMetrics"][0]["resource"]);
    assert_eq!(resource["service.name"], "codex_cli_rs");
    assert_eq!(resource["service.version"], "1.2.3");
    assert_eq!(resource["os"], "linux");
    assert_eq!(resource["os_version"], "6.8");
    assert_eq!(
        first[0]["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"],
        "codex"
    );
    let api = metric(&first[0], "codex.api_request");
    assert_eq!(api["sum"]["aggregationTemporality"], 1);
    assert_eq!(api["sum"]["dataPoints"][0]["asInt"], 1);
    let tags = attributes(&api["sum"]["dataPoints"][0]);
    assert_eq!(tags["model"], "gpt-test");
    assert_eq!(tags["auth_mode"], "chatgpt");
    assert_eq!(tags["app.version"], "1.2.3");
    assert_eq!(tags["originator"], "codex_cli_rs");
    assert_eq!(tags["status"], "200");
    assert_eq!(tags["success"], "true");
    let duration = metric(&first[0], "codex.api_request.duration_ms");
    assert_eq!(duration["unit"], "ms");
    assert_eq!(duration["histogram"]["aggregationTemporality"], 1);
    let point = &duration["histogram"]["dataPoints"][0];
    assert_eq!(point["count"], 1);
    assert_eq!(point["explicitBounds"][0], 0.0);
    assert_eq!(
        point["explicitBounds"].as_array().unwrap().last().unwrap(),
        &json!(120000.0)
    );
    let json = first[0].to_string();
    for private in [
        "prompt-private-marker",
        "account-private-marker",
        "access-token",
        "test-collector-token",
    ] {
        assert!(!json.contains(private));
    }

    assert_eq!(run_http(&client).await.body, body);
    flush(task.as_ref()).await.unwrap();
    let second = collected(&collector).await;
    assert_eq!(second.len(), 2);
    assert_eq!(
        metric(&second[1], "codex.api_request")["sum"]["dataPoints"][0]["asInt"],
        1
    );
}

#[tokio::test]
async fn telemetry_export_failure_does_not_fail_generation_or_following_requests() {
    let collector = collector(503).await;
    let body = completed_sse();
    let upstream = http_response(&body).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    assert_eq!(run_http(&client).await.body, body);
    assert!(flush(task.as_ref()).await.is_err());
    assert_eq!(run_http(&client).await.body, body);
}

#[tokio::test]
async fn samples_recorded_during_export_survive_idle_aggregator_retirement() {
    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/metrics"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
        .mount(&collector)
        .await;
    let upstream = http_response(&completed_sse()).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    run_http(&client).await;
    let later_request = async {
        timeout(Duration::from_secs(2), async {
            while collector.received_requests().await.unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        run_http(&client).await;
    };
    let (export, ()) = tokio::join!(flush(task.as_ref()), later_request);
    export.unwrap();
    flush(task.as_ref()).await.unwrap();
    let payloads = collected(&collector).await;
    assert_eq!(payloads.len(), 2);
    for payload in payloads {
        assert_eq!(
            metric(&payload, "codex.api_request")["sum"]["dataPoints"][0]["asInt"],
            1
        );
    }
}

#[tokio::test]
async fn disabled_metrics_keep_model_requests_and_send_no_telemetry() {
    let collector = collector(200).await;
    let upstream = http_response(&completed_sse()).await;
    for disabled in [
        CodexTelemetrySettings {
            enabled: false,
            ..settings(&collector)
        },
        CodexTelemetrySettings {
            metrics_exporter: CodexMetricsExporter::None,
            ..settings(&collector)
        },
    ] {
        let (client, task) = client(&upstream.uri(), disabled);
        run_http(&client).await;
        flush(task.as_ref()).await.unwrap();
    }
    assert!(collector.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn telemetry_follows_the_selected_account_proxy() {
    use gateway_core::{
        account::{CredentialRevision, OutboundProxy, ProviderAccount, ProviderAccountId},
        routing::ProviderKind,
    };

    let proxy = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(completed_sse(), "text/event-stream"))
        .mount(&proxy)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/metrics"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&proxy)
        .await;
    let mut settings = settings(&proxy);
    settings.metrics_exporter = CodexMetricsExporter::OtlpHttp {
        endpoint: "http://collector.invalid/v1/metrics".into(),
        headers: Default::default(),
    };
    let (base, task) = client("http://upstream.invalid", settings);
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_telemetry_proxy").unwrap(),
        ProviderKind::new("openai").unwrap(),
        "proxy".into(),
        None,
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_outbound_proxy(Some(OutboundProxy::parse(&proxy.uri()).unwrap()));
    let client = base.for_account(&account).unwrap();
    run_http(&client).await;
    flush(task.as_ref()).await.unwrap();
    let requests = proxy.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let exported = requests
        .iter()
        .find(|request| request.url.path() == "/v1/metrics")
        .unwrap();
    assert_eq!(exported.url.host_str(), Some("collector.invalid"));
    assert!(!exported.headers.contains_key("authorization"));
}
