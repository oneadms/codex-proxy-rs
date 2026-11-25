//! 验证跨采样轮次聚合、工具回传和目录增量

use super::*;

fn request_with_metadata(input: Vec<Value>, telemetry: Value) -> CodexResponsesRequest {
    let mut request = codex_request("gpt-test", "prompt-private-marker", input);
    request.force_http_sse = true;
    request.set_client_metadata(Some(json!({"session_id":"client-session", "thread_id":"client-thread", "turn_id":"client-turn", "process_id":"client-process", "service_name":"client-service", "telemetry":telemetry.to_string()})));
    request
}

#[tokio::test]
async fn one_turn_aggregates_actual_models_and_completed_local_tools_across_sampling_requests() {
    let collector = collector(200).await;
    let upstream = MockServer::start().await;
    let index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("POST")).and(path("/codex/responses"))
        .respond_with(move |_: &wiremock::Request| {
            let step = index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let response = if step == 0 {
                json!({"type":"response.completed", "response":{"id":"response-a", "model":"gpt-5.1", "output":[
                    {"type":"function_call","call_id":"call-exec","name":"exec_command","arguments":"{\"tty\":true}"},
                    {"type":"function_call","call_id":"call-goal","name":"create_goal","arguments":"{}"}
                ], "usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}})
            } else {
                json!({"type":"response.completed", "response":{"id":"response-b", "model":"gpt-5.2", "output":[{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"done-private-marker"}]}], "usage":{"input_tokens":20,"output_tokens":10,"total_tokens":30,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":2}}}})
            };
            ResponseTemplate::new(200).set_body_raw(format!("data: {response}\n\n"), "text/event-stream")
        }).mount(&upstream).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    client
        .create_response(
            &request_with_metadata(Vec::new(), json!({})),
            request_context("sample-a", None),
        )
        .await
        .unwrap();
    let input = vec![
        json!({"type":"function_call_output","call_id":"call-exec","output":"{\"is_error\":false,\"duration_ms\":7}"}),
        json!({"type":"function_call_output","call_id":"call-goal","output":"{\"is_error\":false,\"goal\":{\"status\":\"active\"}}"}),
    ];
    let facts =
        json!({"turn":{"e2e_duration_ms":200,"ttft_ms":40,"ttfm_ms":150,"cost_microusd":12345}});
    client
        .create_response(
            &request_with_metadata(input, facts),
            request_context("sample-b", None),
        )
        .await
        .unwrap();
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    let e2e = &metric(payload, "codex.turn.e2e_duration_ms")["histogram"]["dataPoints"][0];
    assert_eq!(e2e["count"], 1);
    assert_eq!(e2e["sum"], 200.);
    assert_eq!(
        metric(payload, "codex.turn.ttft.duration_ms")["histogram"]["dataPoints"][0]["sum"],
        40.
    );
    assert_eq!(
        metric(payload, "codex.turn.ttfm.duration_ms")["histogram"]["dataPoints"][0]["sum"],
        150.
    );
    assert_eq!(
        metric(payload, "codex.turn.tool.call")["histogram"]["dataPoints"][0]["sum"],
        2.
    );
    let usage = metric(payload, "codex.turn.token_usage")["histogram"]["dataPoints"]
        .as_array()
        .unwrap();
    assert_eq!(usage.len(), 12);
    assert!(
        usage
            .iter()
            .all(|point| attributes(point)["tmp_mem_enabled"] == "false")
    );
    for (model, total, cached, reasoning) in [("gpt-5.1", 15., 0., 0.), ("gpt-5.2", 30., 3., 2.)] {
        for (kind, expected) in [
            ("total", total),
            ("cached_input", cached),
            ("reasoning_output", reasoning),
        ] {
            let point = usage
                .iter()
                .find(|point| {
                    let tags = attributes(point);
                    tags["model"] == model && tags["token_type"] == kind
                })
                .unwrap();
            assert_eq!(point["count"], 1);
            assert_eq!(point["sum"], expected);
        }
    }
    let cost = &metric(payload, "codex.turn.cost_microusd")["sum"]["dataPoints"][0];
    assert_eq!(cost["asInt"], 12345);
    assert_eq!(attributes(cost)["turn.id"], "client-turn");
    assert_eq!(attributes(cost)["service_name"], "client-service");
    assert_eq!(
        metric(payload, "codex.exec_command.duration_total_ms")["sum"]["dataPoints"][0]["asInt"],
        7
    );
    assert_eq!(
        metric(payload, "codex.goal.created")["sum"]["dataPoints"][0]["asInt"],
        1
    );
    let registrations = metric(payload, "codex.tools.registered")["histogram"]["dataPoints"]
        .as_array()
        .unwrap();
    assert_eq!(registrations.len(), 6);
    assert!(registrations.iter().all(|point| point["count"] == 2));
    for private in ["prompt-private-marker", "done-private-marker", "call-exec"] {
        assert!(!payload.to_string().contains(private));
    }
}

#[tokio::test]
async fn catalog_excludes_hosted_tools_and_records_replacements_and_removals_only_after_snapshot() {
    let collector = collector(200).await;
    let upstream = http_response(&completed_sse()).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    for (index, tools) in [
        json!([{"type":"function","name":"foo","description":"old"},{"type":"web_search"}]),
        json!([{"type":"function","name":"foo","description":"中文"},{"type":"function","name":"bar","description":"new"}]),
        json!([{"type":"function","name":"bar","description":"new"}]),
    ].into_iter().enumerate() {
        let mut request = request_with_metadata(Vec::new(), json!({"tools":{"incremental_tools":true,"tool_mode":"code_mode","fragment_bytes":{"before":88,"after":44}}}));
        let mut body = request.body().clone();
        body.insert("tools".to_owned(), tools);
        request = CodexResponsesRequest::from_body(body);
        request.force_http_sse = true;
        client.create_response(&request, request_context(&format!("catalog-{index}"),None)).await.unwrap();
    }
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    let updates = metric(payload, "codex.tools.incremental_updates")["sum"]["dataPoints"]
        .as_array()
        .unwrap();
    assert_eq!(updates.len(), 3);
    for (action, tool) in [
        ("added", "functions.bar"),
        ("schema_changed", "functions.foo"),
        ("removed", "functions.foo"),
    ] {
        let point = updates
            .iter()
            .find(|point| {
                let tags = attributes(point);
                tags["action"] == action && tags["tool"] == tool
            })
            .unwrap();
        assert_eq!(point["asInt"], 1);
    }
    let description =
        &metric(payload, "codex.tools.spec_description_bytes")["histogram"]["dataPoints"][0];
    assert_eq!(description["sum"], 6.);
    assert_eq!(description["count"], 1);
    let direct = metric(payload, "codex.tools.registered")["histogram"]["dataPoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|point| attributes(point)["exposure"] == "direct")
        .unwrap();
    assert_eq!(direct["sum"], 4.);
    assert_eq!(direct["count"], 3);
}

#[tokio::test]
async fn interleaved_turn_ids_keep_pending_tools_and_usage_in_their_original_turn() {
    let collector = collector(200).await;
    let upstream = MockServer::start().await;
    let index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("POST")).and(path("/codex/responses"))
        .respond_with(move |_: &wiremock::Request| {
            let step=index.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
            let output=if step==0 {json!([{"type":"function_call","name":"exec_command","call_id":"interleaved-call","arguments":"{}"}])} else {json!([{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"done"}]}])};
            let tokens=(step+1)*10;
            let response=json!({"type":"response.completed","response":{"id":format!("interleaved-{step}"),"model":"gpt-test","output":output,"usage":{"input_tokens":tokens,"output_tokens":0,"total_tokens":tokens}}});
            ResponseTemplate::new(200).set_body_raw(format!("data: {response}\n\n"),"text/event-stream")
        }).mount(&upstream).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    for (step, turn, input, cost) in [
        (0, "turn-a", vec![], 100),
        (1, "turn-b", vec![], 200),
        (
            2,
            "turn-a",
            vec![
                json!({"type":"function_call_output","call_id":"interleaved-call","output":"{\"duration_ms\":4}"}),
            ],
            100,
        ),
    ] {
        let mut request = request_with_metadata(input, json!({"turn":{"cost_microusd":cost}}));
        request.set_client_metadata(Some(json!({"session_id":"interleaved-session","thread_id":"interleaved-thread","turn_id":turn,"telemetry":{"turn":{"cost_microusd":cost}}})));
        client
            .create_response(
                &request,
                request_context(&format!("interleaved-sample-{step}"), None),
            )
            .await
            .unwrap();
    }
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    assert_eq!(
        metric(payload, "codex.turn.e2e_duration_ms")["histogram"]["dataPoints"][0]["count"],
        2
    );
    let usage = metric(payload, "codex.turn.token_usage")["histogram"]["dataPoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|point| attributes(point)["token_type"] == "total")
        .unwrap();
    assert_eq!(usage["count"], 2);
    assert_eq!(usage["sum"], 60.);
    let tools = &metric(payload, "codex.turn.tool.call")["histogram"]["dataPoints"][0];
    assert_eq!(tools["sum"], 1.);
    assert_eq!(tools["count"], 2);
    let costs = metric(payload, "codex.turn.cost_microusd")["sum"]["dataPoints"]
        .as_array()
        .unwrap();
    assert_eq!(costs.len(), 2);
    for (turn, amount) in [("turn-a", 100), ("turn-b", 200)] {
        let point = costs
            .iter()
            .find(|point| attributes(point)["turn.id"] == turn)
            .unwrap();
        assert_eq!(point["asInt"], amount);
        assert_eq!(attributes(point)["turn.interrupted"], "false");
    }
}

#[tokio::test]
async fn warmup_is_excluded_from_turns_and_cancelled_streams_close_at_shutdown() {
    let collector = collector(200).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_codex_test_websocket(stream).await;
        assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
        websocket
            .send(Message::Text(
                completed_websocket_response("prewarm-response", 0, 0).into(),
            ))
            .await
            .unwrap();
        assert!(matches!(websocket.next().await, Some(Ok(Message::Text(_)))));
        websocket
            .send(Message::Text(
                json!({"type":"response.created","response":{"id":"cancelled-response"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let _ = websocket.next().await;
    });
    let (client, task) = client(&base_url, settings(&collector));
    let pool = Arc::new(CodexWebSocketPool::new(Duration::from_secs(60)));
    let client = client.with_websocket_pool(Arc::clone(&pool));
    let mut generation = request_with_metadata(Vec::new(), json!({}));
    generation.force_http_sse = false;
    generation.use_websocket = true;
    generation.local_conversation_id = Some("warmup-conversation".to_owned());
    let mut warmup_body = generation.body().clone();
    warmup_body.insert("generate".to_owned(), json!(false));
    let mut warmup = CodexResponsesRequest::from_body(warmup_body);
    warmup.use_websocket = true;
    warmup.local_conversation_id = Some("warmup-conversation".to_owned());
    client
        .create_response(&warmup, request_context("warmup", Some("warmup-account")))
        .await
        .unwrap();
    let response = client
        .create_response_stream(
            &generation,
            request_context("cancelled", Some("warmup-account")),
        )
        .await
        .unwrap();
    drop(response);
    timeout(Duration::from_secs(3), pool.shutdown())
        .await
        .unwrap();
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    assert_eq!(
        metric(payload, "codex.turn.e2e_duration_ms")["histogram"]["dataPoints"][0]["count"],
        1
    );
    assert_eq!(
        metric(payload, "codex.startup_prewarm.duration_ms")["histogram"]["dataPoints"][0]["count"],
        1
    );
    assert_eq!(
        metric(payload, "codex.startup_prewarm.age_at_first_turn_ms")["histogram"]["dataPoints"][0]
            ["count"],
        1
    );
    assert!(!payload.to_string().contains("codex.turn.ttft.duration_ms"));
    assert!(!payload.to_string().contains("codex.turn.cost_microusd"));
    let continuation = metric(payload, "codex.websocket.continuation")["sum"]["dataPoints"]
        .as_array()
        .unwrap()
        .iter()
        .find(|point| attributes(point)["phase"] == "generation")
        .unwrap();
    assert_eq!(attributes(continuation)["after_prewarm"], "true");
}

#[tokio::test]
async fn estimated_cost_rounds_once_per_turn_and_explicit_null_preserves_hidden_cost() {
    for (supplied, expected) in [
        (None, Some(3)),
        (Some(Value::Null), None),
        (Some(json!(0)), Some(0)),
    ] {
        let collector = collector(200).await;
        let upstream = MockServer::start().await;
        let index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        Mock::given(method("POST")).and(path("/codex/responses"))
            .respond_with(move |_: &wiremock::Request| {
                let step=index.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                let output=if step==0 {json!([{"type":"function_call","name":"foo","call_id":"priced-call","arguments":"{}"}])} else {json!([])};
                let response=json!({"type":"response.completed","response":{"id":format!("priced-{step}"),"model":"gpt-5.1","output":output,"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}});
                ResponseTemplate::new(200).set_body_raw(format!("data: {response}\n\n"),"text/event-stream")
            }).mount(&upstream).await;
        let (client, task) = client(&upstream.uri(), settings(&collector));
        let facts = supplied
            .map(|value| json!({"turn":{"cost_microusd":value}}))
            .unwrap_or_else(|| json!({}));
        for (step, input) in [
            vec![],
            vec![json!({"type":"function_call_output","call_id":"priced-call","output":"ok"})],
        ]
        .into_iter()
        .enumerate()
        {
            client
                .create_response(
                    &request_with_metadata(input, facts.clone()),
                    request_context(&format!("priced-sample-{step}"), None),
                )
                .await
                .unwrap();
        }
        flush(task.as_ref()).await.unwrap();
        let exports = collected(&collector).await;
        if let Some(expected) = expected {
            assert_eq!(
                metric(&exports[0], "codex.turn.cost_microusd")["sum"]["dataPoints"][0]["asInt"],
                expected
            );
        } else {
            assert!(!exports[0].to_string().contains("codex.turn.cost_microusd"));
        }
    }
}
