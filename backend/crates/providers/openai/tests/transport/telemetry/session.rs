//! 验证 SSE 与 WebSocket 实际事件、连接复用和官方 timing 字段

use super::*;

#[tokio::test]
async fn sse_comments_done_and_failed_events_preserve_bytes_and_classification() {
    let collector = collector(200).await;
    let body = concat!(
        ": keepalive\r\n\r\n",
        "event: response.output_text.delta\r\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\r\n\r\n",
        "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"test failure\"}}}\n\n",
        "data: [DONE]\n\n"
    );
    let upstream = http_response(body).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    let mut request = codex_request("gpt-test", "", Vec::new());
    request.force_http_sse = true;
    let mut response = client
        .create_response_stream(&request, request_context("sse", None))
        .await
        .unwrap();
    let mut raw = Vec::new();
    while let Some(chunk) = response.body.next().await {
        raw.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(raw, body.as_bytes());
    flush(task.as_ref()).await.unwrap();
    let exported = collected(&collector).await;
    let points = metric(&exported[0], "codex.sse_event")["sum"]["dataPoints"]
        .as_array()
        .unwrap();
    assert_eq!(points.len(), 3);
    let tags: Vec<_> = points.iter().map(attributes).collect();
    assert!(
        tags.iter()
            .any(|tags| tags["kind"] == "response.failed" && tags["success"] == "false")
    );
    assert!(
        tags.iter()
            .any(|tags| tags["kind"] == "message" && tags["success"] == "true")
    );
    assert!(tags.iter().all(|tags| tags["kind"] != "keepalive"));
}

#[tokio::test]
async fn websocket_metrics_count_physical_connects_and_unfiltered_timing_events() {
    let collector = collector(200).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_codex_test_websocket(stream).await;
        for index in 0..2 {
            assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
            websocket
                .send(Message::Text(
                    json!({
                        "type": "responsesapi.websocket_timing",
                        "timing_metrics": {
                            "responses_duration_excl_engine_and_client_tool_time_ms": 4.6,
                            "engine_service_total_ms": 20,
                            "engine_iapi_tbt_across_engine_calls_ms": 1.25,
                            "engine_service_tbt_across_engine_calls_ms": -1
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            websocket
                .send(Message::Text(
                    completed_websocket_response(&format!("resp_metrics_{index}"), 2, 1).into(),
                ))
                .await
                .unwrap();
        }
        let _ = websocket.next().await;
    });
    let (client, task) = client(&base_url, settings(&collector));
    let pool = Arc::new(CodexWebSocketPool::new(Duration::from_secs(60)));
    let client = client.with_websocket_pool(Arc::clone(&pool));
    let mut request = pooled_websocket_request("telemetry-conversation");
    request.use_websocket = true;
    request.set_client_metadata(Some(json!({"telemetry":json!({"websocket_continuation":{"after_prewarm":true},"websocket_connect":{"reauth":true}}).to_string()})));
    for _ in 0..2 {
        let response = client
            .create_response(&request, request_context("ws", Some("ws-account")))
            .await
            .unwrap();
        assert_eq!(response.transport, CodexBackendTransport::WebSocket);
    }
    flush(task.as_ref()).await.unwrap();
    let exported = collected(&collector).await;
    let payload = &exported[0];
    assert_eq!(
        metric(payload, "codex.websocket.connect")["sum"]["dataPoints"][0]["asInt"],
        1
    );
    assert_eq!(
        metric(payload, "codex.websocket.request")["sum"]["dataPoints"][0]["asInt"],
        2
    );
    let continuation = &metric(payload, "codex.websocket.continuation")["sum"]["dataPoints"][0];
    assert_eq!(continuation["asInt"], 2);
    let tags = attributes(continuation);
    assert_eq!(tags["mode"], "incremental");
    assert_eq!(tags["reason"], "incremental");
    assert_eq!(tags["phase"], "generation");
    assert_eq!(tags["after_prewarm"], "true");
    assert_eq!(
        attributes(&metric(payload, "codex.websocket.connect")["sum"]["dataPoints"][0])["reauth"],
        "true"
    );
    let events = metric(payload, "codex.websocket.event")["sum"]["dataPoints"]
        .as_array()
        .unwrap();
    let timing = events
        .iter()
        .find(|point| attributes(point)["kind"] == "responsesapi.websocket_timing")
        .unwrap();
    assert_eq!(timing["asInt"], 2);
    let overhead =
        &metric(payload, "codex.responses_api_overhead.duration_ms")["histogram"]["dataPoints"][0];
    assert_eq!(overhead["sum"], 10.0);
    assert_eq!(overhead["count"], 2);
    let tbt = &metric(payload, "codex.responses_api_engine_iapi_tbt.duration_ms")["histogram"]["dataPoints"]
        [0];
    assert_eq!(tbt["sum"], 2.5);
    pool.shutdown().await;
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
