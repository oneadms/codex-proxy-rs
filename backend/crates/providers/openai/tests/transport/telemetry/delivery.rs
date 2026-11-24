//! 验证 analytics 混批边界、发送并发与请求级运行开关

use super::{analytics::*, *};
use gateway_core::account::OpaqueProviderData;
use provider_openai::transport::profile::identity::RequestProfileSelection;

#[tokio::test]
async fn analytics_batches_only_matching_accounts_credentials_profiles_and_products() {
    let body = sse(&events_for(&[json!({
        "type":"web_search_call", "id":"ws_batch", "status":"completed",
        "action":{"type":"search", "query":"private-query"}
    })]));
    let upstream = http_response(&body).await;
    mount_analytics(&upstream, 200).await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    for (index, account, token, ua, sku) in [
        (0, "one", "Bearer first", "codex-tui/1.2.3", "product-a"),
        (1, "one", "Bearer first", "codex-tui/1.2.3", "product-a"),
        (2, "two", "Bearer first", "codex-tui/1.2.3", "product-a"),
        (3, "one", "Bearer second", "codex-tui/1.2.3", "product-a"),
        (4, "one", "Bearer first", "codex-tui/2.0.0", "product-a"),
        (5, "one", "Bearer first", "codex-tui/1.2.3", "product-b"),
    ] {
        let profile = RequestProfileSelection::parse(&OpaqueProviderData::new(
            json!({"mode":"custom", "userAgent":ua})
                .as_object()
                .unwrap()
                .clone(),
        ))
        .unwrap()
        .resolve(&test_wire_profile())
        .unwrap();
        let client = client.clone().with_request_profile(profile);
        let mut request = http_request(&[("x-openai-product-sku", sku)]);
        request.set_client_metadata(Some(json!({"turnId":format!("turn-{index}")})));
        let mut context = context(account);
        context.authorization = token;
        let mut response = client
            .create_response_stream(&request, context)
            .await
            .unwrap();
        while let Some(chunk) = response.body.next().await {
            chunk.unwrap();
        }
    }
    flush(task.as_ref()).await.unwrap();
    let requests = upstream.received_requests().await.unwrap();
    let batches: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path() == "/codex/analytics-events/events")
        .collect();
    assert_eq!(batches.len(), 5);
    assert_eq!(payloads(&upstream).await.len(), 6);
    for batch in batches {
        let payload: Value = serde_json::from_slice(&batch.body).unwrap();
        let turns: Vec<_> = payload["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["event_params"]["turn_id"].as_str().unwrap())
            .collect();
        let first = batch.headers["authorization"] == "Bearer first"
            && batch.headers["chatgpt-account-id"] == "one"
            && batch.headers["user-agent"] == "codex-tui/1.2.3"
            && batch.headers["x-openai-product-sku"] == "product-a";
        if first {
            assert_eq!(turns, ["turn-0", "turn-1"]);
        } else {
            assert_eq!(turns.len(), 1);
        }
    }
}

#[tokio::test]
async fn analytics_sends_four_batches_concurrently_and_drains_the_remainder_on_shutdown() {
    let upstream = http_response(&sse(&events_for(&[json!({
        "type":"web_search_call", "id":"ws_concurrent", "status":"completed"
    })])))
    .await;
    Mock::given(method("POST"))
        .and(path("/codex/analytics-events/events"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(600)))
        .mount(&upstream)
        .await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    for account in ["one", "two", "three", "four", "five", "six"] {
        run(&client, context(account)).await;
    }
    let cancellation = CancellationToken::new();
    let worker_context = WorkerDaemonContext::new(cancellation.clone());
    let worker = tokio::spawn(async move { task.run(worker_context).await });
    timeout(Duration::from_secs(2), async {
        while payloads(&upstream).await.len() < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // 第一组尚未返回前，第五个批次必须仍在等待发送槽位
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(payloads(&upstream).await.len(), 4);
    cancellation.cancel();
    timeout(Duration::from_secs(3), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(payloads(&upstream).await.len(), 6);
}

#[tokio::test]
async fn runtime_opt_out_stops_new_samples_and_preserves_an_inflight_request() {
    let body = sse(&events_for(&[json!({
        "type":"web_search_call", "id":"ws_gate", "status":"completed"
    })]));
    let upstream = http_response(&body).await;
    mount_analytics(&upstream, 200).await;
    let collector = collector(200).await;
    let (base, task) = client(&upstream.uri(), settings(&collector));
    let request = http_request(&[]);
    let mut inflight = base
        .create_response_stream(&request, context("inflight"))
        .await
        .unwrap();
    let disabled = base.clone().with_telemetry_enabled(false);
    assert_eq!(run(&disabled, context("disabled")).await, body);
    while let Some(chunk) = inflight.body.next().await {
        chunk.unwrap();
    }
    flush(task.as_ref()).await.unwrap();
    assert_eq!(payloads(&upstream).await.len(), 1);
    let exports = collected(&collector).await;
    assert_eq!(exports.len(), 1);
    assert_eq!(
        metric(&exports[0], "codex.api_request")["sum"]["dataPoints"][0]["asInt"],
        1
    );
}
