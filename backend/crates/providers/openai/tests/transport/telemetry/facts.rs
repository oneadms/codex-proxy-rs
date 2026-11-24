//! 验证客户端事实优先、事件去重、单位和标签白名单

use super::*;

#[tokio::test]
async fn explicit_client_facts_preserve_amounts_override_simulation_and_deduplicate_ids() {
    let collector = collector(200).await;
    let upstream = http_response(&completed_sse()).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    let mut request = codex_request("gpt-test", "prompt-private-marker", Vec::new());
    request.force_http_sse = true;
    request.set_client_metadata(Some(json!({
        "session_id":"facts-session", "thread_id":"facts-thread", "turn_id":"facts-turn",
        "telemetry":json!({"turn":{"critical_path":{"status":"disabled"}},"events":[
            {"id":"process-1","name":"codex.process.start","value":3,"tags":{"originator":"codex_cli_rs","prompt":"private-fact-marker"}},
            {"id":"exec-1","name":"codex.exec_command.duration_total_ms","value":42,"tags":{"success":true}},
            {"id":"goal-1","name":"codex.goal.duration_s","value":9,"tags":{"status":"complete"}},
            {"id":"goal-1","name":"codex.goal.token_count","value":700,"tags":{"status":"complete"}},
            {"id":"invalid","name":"codex.goal.resumed","value":-1},
            {"id":"unknown","name":"codex.private.prompt","value":1}
        ]}).to_string()
    })));
    for sample in ["facts-a", "facts-b"] {
        client
            .create_response(&request, request_context(sample, None))
            .await
            .unwrap();
    }
    let requests = upstream.received_requests().await.unwrap();
    let wire = zstd::stream::decode_all(requests[0].body.as_slice()).unwrap();
    let wire: Value = serde_json::from_slice(&wire).unwrap();
    assert!(
        wire["client_metadata"]
            .as_object()
            .unwrap()
            .values()
            .all(Value::is_string)
    );
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    assert_eq!(
        metric(payload, "codex.process.start")["sum"]["dataPoints"][0]["asInt"],
        3
    );
    let exec = &metric(payload, "codex.exec_command.duration_total_ms")["sum"]["dataPoints"][0];
    assert_eq!(exec["asInt"], 42);
    assert_eq!(attributes(exec)["success"], "true");
    let duration = metric(payload, "codex.goal.duration_s");
    // 官方 Goal 使用普通整数直方图，名称虽以 _s 结尾也不附加毫秒单位
    assert!(duration.get("unit").is_none_or(|unit| unit == ""));
    assert_eq!(duration["histogram"]["dataPoints"][0]["sum"], 9.);
    assert_eq!(duration["histogram"]["dataPoints"][0]["count"], 1);
    assert_eq!(
        attributes(&metric(payload, "codex.turn.critical_path.capture")["sum"]["dataPoints"][0])["capture_status"],
        "disabled"
    );
    assert!(
        !payload
            .to_string()
            .contains("codex.turn.critical_path.duration_ms")
    );
    assert_eq!(
        metric(payload, "codex.goal.token_count")["histogram"]["dataPoints"][0]["sum"],
        700.
    );
    for private in [
        "private-fact-marker",
        "codex.private.prompt",
        "codex.goal.resumed",
        "prompt-private-marker",
    ] {
        assert!(!payload.to_string().contains(private));
    }
}

#[tokio::test]
async fn guardian_hooks_artifacts_and_plugin_sync_use_supplied_facts_with_official_instruments() {
    let collector = collector(200).await;
    let mut completed: Value =
        serde_json::from_str(&completed_websocket_response("guardian-completed", 20, 5)).unwrap();
    completed["response"]["output"] = json!([{"type":"message","role":"assistant","content":[{"type":"output_text","text":"private-review-marker"}]}]);
    let body = format!(
        "data: {{\"type\":\"response.output_text.delta\",\"delta\":\"x\"}}\n\ndata: {completed}\n\n"
    );
    let upstream = http_response(&body).await;
    let (client, task) = client(&upstream.uri(), settings(&collector));
    let mut request = codex_request("gpt-test", "", Vec::new());
    request.force_http_sse = true;
    request.set_client_metadata(Some(json!({"session_id":"guardian-session","thread_id":"guardian-thread","turn_id":"guardian-turn","subagent_kind":"guardian","telemetry":{
        "guardian":{"decision":"denied","terminal_status":"denied","duration_ms":25,"ttft_ms":8,"truncations":[{"component":"reviewed_action","original_bytes":100,"retained_bytes":40}]},
        "hooks":[{"hook_name":"PreToolUse","source":"plugin","status":"completed","handler_type":"command","execution_mode":"sync","duration_ms":7}],
        "artifact_operation":{"skill":"slides","artifact_type":"presentation","operation_kind":"create","output_format":"pptx","expected_output_count":2},
        "plugin_sync":{"transport":"http","status":"success"},
        "skills":{"enabled_total":5,"kept_total":3,"description_truncated_chars":12}
    }})));
    client
        .create_response(&request, request_context("guardian-request", None))
        .await
        .unwrap();
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    let payload = &exports[0];
    let review = &metric(payload, "codex.guardian.review")["sum"]["dataPoints"][0];
    assert_eq!(review["asInt"], 1);
    assert_eq!(attributes(review)["decision"], "denied");
    assert_eq!(
        metric(payload, "codex.guardian.review.duration_ms")["histogram"]["dataPoints"][0]["sum"],
        25.
    );
    assert_eq!(
        metric(payload, "codex.guardian.review.ttft.duration_ms")["histogram"]["dataPoints"][0]["sum"],
        8.
    );
    let bytes =
        metric(payload, "codex.guardian.review.truncation.bytes")["histogram"]["dataPoints"]
            .as_array()
            .unwrap();
    for (measurement, value) in [("original", 100.), ("retained", 40.), ("omitted", 60.)] {
        let point = bytes
            .iter()
            .find(|point| attributes(point)["measurement"] == measurement)
            .unwrap();
        assert_eq!(point["sum"], value);
        assert_eq!(
            point["explicitBounds"].as_array().unwrap().last().unwrap(),
            &json!(16777216.)
        );
    }
    assert_eq!(
        metric(payload, "codex.guardian.review.token_usage")["histogram"]["dataPoints"]
            .as_array()
            .unwrap()
            .len(),
        7
    );
    assert_eq!(
        metric(payload, "codex.hooks.run.duration_ms")["histogram"]["dataPoints"][0]["sum"],
        7.
    );
    assert_eq!(
        metric(payload, "codex.artifact.operation.expected_output_count")["histogram"]["dataPoints"]
            [0]["sum"],
        2.
    );
    assert_eq!(
        metric(payload, "codex.plugins.startup_sync.final")["sum"]["dataPoints"][0]["asInt"],
        1
    );
    assert_eq!(
        metric(payload, "codex.thread.skills.truncated")["histogram"]["dataPoints"][0]["sum"],
        1.
    );
    assert!(!payload.to_string().contains("private-review-marker"));
}

#[tokio::test]
async fn client_event_ids_are_isolated_by_selected_account() {
    use gateway_core::{
        account::{CredentialRevision, ProviderAccount, ProviderAccountId},
        routing::ProviderKind,
    };
    let collector = collector(200).await;
    let upstream = http_response(&completed_sse()).await;
    let (base, task) = client(&upstream.uri(), settings(&collector));
    let mut request = codex_request("gpt-test", "", Vec::new());
    request.force_http_sse = true;
    request.set_client_metadata(Some(json!({"session_id":"same-session","thread_id":"same-thread","turn_id":"same-turn","telemetry":{"events":[{"id":"same-event","name":"codex.goal.created","value":1,"tags":{"reasoning_effort":"high"}}]}})));
    for id in ["acct_telemetry_one", "acct_telemetry_two"] {
        let account = ProviderAccount::new(
            ProviderAccountId::new(id).unwrap(),
            ProviderKind::new("openai").unwrap(),
            "test".to_owned(),
            None,
            "oauth".to_owned(),
            CredentialRevision::new(1).unwrap(),
            None,
        );
        let client = base.for_account(&account).unwrap();
        client
            .create_response(&request, request_context("same-request", None))
            .await
            .unwrap();
    }
    flush(task.as_ref()).await.unwrap();
    let exports = collected(&collector).await;
    assert_eq!(exports.len(), 2);
    for payload in exports {
        assert_eq!(
            metric(&payload, "codex.goal.created")["sum"]["dataPoints"][0]["asInt"],
            1
        );
    }
}
