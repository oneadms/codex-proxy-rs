//! 验证真实工具回传驱动 Goal 状态与多代理结果指标

use super::*;

#[tokio::test]
async fn tool_results_drive_goal_transitions_and_distinguish_spawn_success_from_failure() {
    let collector = collector(200).await;
    let upstream = MockServer::start().await;
    let results = vec![
        (
            "create_goal",
            json!({"is_error":false,"goal":{"status":"active"}}),
        ),
        (
            "update_goal",
            json!({"is_error":false,"goal":{"status":"blocked","tokensUsed":11,"timeUsedSeconds":3}}),
        ),
        (
            "update_goal",
            json!({"is_error":false,"goal":{"status":"active"}}),
        ),
        (
            "update_goal",
            json!({"is_error":false,"goal":{"status":"budget_limited","tokensUsed":22,"timeUsedSeconds":6}}),
        ),
        (
            "update_goal",
            json!({"is_error":false,"goal":{"status":"usage_limited","tokensUsed":23,"timeUsedSeconds":7}}),
        ),
        (
            "update_goal",
            json!({"is_error":false,"goal":{"status":"complete","tokensUsed":24,"timeUsedSeconds":8}}),
        ),
        (
            "spawn_agent",
            json!({"success":true,"agent_id":"child-thread","duration_ms":12}),
        ),
        (
            "spawn_agent",
            json!({"error":{"message":"private-spawn-error"}}),
        ),
        ("close_agent", json!({"success":true,"status":"closed"})),
        ("spawn_agent", json!({"agent_id":"unverified-child"})),
        ("close_agent", json!({"status":"closed"})),
        ("create_goal", json!({"goal":{"status":"active"}})),
        ("update_goal", json!({"goal":{"status":"blocked"}})),
    ];
    let names: Vec<_> = results.iter().map(|(name, _)| *name).collect();
    let step = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("POST")).and(path("/codex/responses"))
        .respond_with(move |_: &wiremock::Request| {
            let index=step.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
            let output=names.get(index).map(|name|json!([{"type":"function_call","call_id":format!("local-call-{index}"),"name":name,"arguments":"{}"}])).unwrap_or_else(||json!([]));
            let response=json!({"type":"response.completed","response":{"id":format!("local-response-{index}"),"model":"gpt-test","output":output,"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}});
            ResponseTemplate::new(200).set_body_raw(format!("data: {response}\n\n"),"text/event-stream")
        }).mount(&upstream).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    let mut input = Vec::new();
    for index in 0..=results.len() {
        let mut request = codex_request("gpt-test", "", input);
        request.force_http_sse = true;
        request.set_client_metadata(Some(
            json!({"session_id":"local-session","thread_id":"local-thread","turn_id":"local-turn"}),
        ));
        client
            .create_response(
                &request,
                request_context(&format!("local-sample-{index}"), None),
            )
            .await
            .unwrap();
        input=results.get(index).map(|(_,result)|vec![json!({"type":"function_call_output","call_id":format!("local-call-{index}"),"output":result.to_string()})]).unwrap_or_default();
    }
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    for name in [
        "codex.goal.created",
        "codex.goal.blocked",
        "codex.goal.resumed",
        "codex.goal.budget_limited",
        "codex.goal.usage_limited",
        "codex.goal.completed",
        "codex.multi_agent.spawned",
        "codex.multi_agent.spawn.failure",
        "codex.multi_agent.stop_accepted",
    ] {
        assert_eq!(
            metric(payload, name)["sum"]["dataPoints"][0]["asInt"],
            1,
            "{name}"
        );
    }
    let tokens = metric(payload, "codex.goal.token_count")["histogram"]["dataPoints"]
        .as_array()
        .unwrap();
    assert_eq!(tokens.len(), 4);
    for (status, value) in [
        ("blocked", 11.),
        ("budget_limited", 22.),
        ("usage_limited", 23.),
        ("complete", 24.),
    ] {
        assert_eq!(
            tokens
                .iter()
                .find(|point| attributes(point)["status"] == status)
                .unwrap()["sum"],
            value
        );
    }
    let phase =
        &metric(payload, "codex.multi_agent.spawn.phase.duration_ms")["histogram"]["dataPoints"][0];
    assert_eq!(phase["count"], 1);
    assert_eq!(phase["sum"], 12.);
    assert_eq!(attributes(phase)["fork_mode"], "none");
    let spawned = &metric(payload, "codex.multi_agent.spawned")["sum"]["dataPoints"][0];
    assert!(!attributes(spawned).contains_key("model"));
    assert_eq!(attributes(spawned)["product_sku"], "unknown");
    assert!(!payload.to_string().contains("private-spawn-error"));
}

#[tokio::test]
async fn tool_outcomes_require_explicit_status_while_unknown_results_complete_the_turn() {
    let collector = collector(200).await;
    let upstream = MockServer::start().await;
    let results = [
        (json!("Error: tool execution failed"), None),
        (
            json!([{"type":"input_text","text":"Error: tool execution failed"}]),
            None,
        ),
        (json!("ok"), None),
        (json!("{\"duration_ms\":7}"), None),
        (json!("{\"error\":null}"), None),
        (json!("{\"is_error\":false}"), Some(true)),
        (json!("{\"success\":true}"), Some(true)),
        (json!("{\"is_error\":true}"), Some(false)),
        (json!("{\"success\":false}"), Some(false)),
        (
            json!("{\"error\":{\"message\":\"private-error\"}}"),
            Some(false),
        ),
        (json!("{\"success\":true,\"is_error\":true}"), Some(false)),
    ];
    let calls: Vec<_> = (0..results.len())
        .map(|index| json!({"type":"function_call","call_id":format!("outcome-{index}"),"name":"exec_command","arguments":"{}"}))
        .collect();
    let step = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(move |_: &wiremock::Request| {
            let index = step.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let output = if index == 0 { json!(calls) } else { json!([]) };
            let response = json!({"type":"response.completed","response":{"id":format!("outcomes-{index}"),"model":"gpt-test","output":output,"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}});
            ResponseTemplate::new(200).set_body_raw(format!("data: {response}\n\n"), "text/event-stream")
        })
        .mount(&upstream)
        .await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    let input: Vec<_> = results.iter().enumerate()
        .map(|(index, (output, _))| json!({"type":"function_call_output","call_id":format!("outcome-{index}"),"output":output}))
        .collect();
    for (index, input) in [vec![], input.clone(), input].into_iter().enumerate() {
        let mut request = codex_request("gpt-test", "", input);
        request.force_http_sse = true;
        request.set_client_metadata(Some(json!({"session_id":"outcome-session","thread_id":"outcome-thread","turn_id":"outcome-turn","telemetry":{"turn":{"cost_microusd":1}}})));
        client
            .create_response(
                &request,
                request_context(&format!("outcome-sample-{index}"), None),
            )
            .await
            .unwrap();
    }
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    for name in ["codex.tool.call", "codex.exec_command.calls"] {
        let points = metric(payload, name)["sum"]["dataPoints"]
            .as_array()
            .unwrap();
        assert_eq!(points.len(), 2);
        for success in [false, true] {
            let point = points
                .iter()
                .find(|point| attributes(point)["success"] == success.to_string())
                .unwrap();
            assert_eq!(
                point["asInt"],
                results
                    .iter()
                    .filter(|(_, outcome)| *outcome == Some(success))
                    .count()
            );
        }
    }
    let tools = &metric(payload, "codex.turn.tool.call")["histogram"]["dataPoints"][0];
    assert_eq!(tools["count"], 1);
    assert_eq!(tools["sum"], results.len() as f64);
    let cost = &metric(payload, "codex.turn.cost_microusd")["sum"]["dataPoints"][0];
    assert_eq!(attributes(cost)["turn.interrupted"], "false");
    assert!(!payload.to_string().contains("private-error"));
}
