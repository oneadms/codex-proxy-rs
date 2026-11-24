//! 验证托管工具事件的请求身份、来源元数据、查询摘要与图片生成结果

use super::{analytics::*, *};
use gateway_core::account::OpaqueProviderData;
use gateway_core::operation::{GenerateRequest, ProtocolPayload};
use provider_openai::transport::profile::identity::RequestProfileSelection;

#[tokio::test]
async fn analytics_client_identity_follows_each_requests_effective_user_agent() {
    let cases = [
        (
            "codex-tui/0.172.0 (Linux 6.8.0; x86_64) unknown (codex-tui; 0.172.0)",
            Some("codex-tui"),
            Some("0.172.0"),
        ),
        (
            "codex_exec/0.173.0 (Linux 6.8.0; x86_64) unknown",
            Some("codex_exec"),
            Some("0.173.0"),
        ),
        (
            "Codex Desktop/0.174.0-alpha.3 (Mac OS 15.7.1; arm64) unknown (Codex Desktop; 26.1124.12345)",
            Some("Codex Desktop"),
            Some("26.1124.12345"),
        ),
        (
            "team-entry/0.172.0 (Linux 6.8.0; x86_64) unknown (codex-tui; 0.172.1)",
            Some("codex-tui"),
            Some("0.172.1"),
        ),
        (
            "codex-tui/0.172.0 (Linux 6.8.0; x86_64)",
            Some("codex-tui"),
            Some("0.172.0"),
        ),
        ("custom-network-client", None, None),
    ];
    for (user_agent, name, version) in cases {
        let body = sse(&events_for(&[json!({
            "type":"web_search_call", "id":"ws_client_identity", "status":"completed",
            "action":{"type":"search", "query":"private-query"}
        })]));
        let upstream = http_response(&body).await;
        mount_analytics(&upstream, 200).await;
        let (client, task) = client(&upstream.uri(), analytics_settings());
        let mut configuration = json!({"mode":"custom", "userAgent":user_agent});
        if user_agent.starts_with("team-entry/") || name.is_none() {
            configuration["originator"] = json!("team-entry");
            configuration["codexVersion"] = json!("0.172.0");
        }
        let profile = RequestProfileSelection::parse(&OpaqueProviderData::new(
            configuration.as_object().unwrap().clone(),
        ))
        .unwrap()
        .resolve(&test_wire_profile())
        .unwrap();
        let client = client.with_request_profile(profile);
        run(&client, context("account-client-identity")).await;
        flush(task.as_ref()).await.unwrap();
        let events = payloads(&upstream).await;
        assert_eq!(events.len(), 1);
        let identity = &events[0]["event_params"]["app_server_client"];
        assert_eq!(identity["client_name"], json!(name), "{user_agent}");
        assert_eq!(identity["client_version"], json!(version), "{user_agent}");
        assert_eq!(
            identity["experimental_api_enabled"],
            json!(
                name.filter(|name| matches!(*name, "codex-tui" | "codex_exec"))
                    .map(|_| true)
            ),
            "{user_agent}"
        );
        for request in upstream.received_requests().await.unwrap() {
            assert_eq!(request.headers["user-agent"], user_agent);
        }
    }
}

#[tokio::test]
async fn analytics_sources_preserve_request_values_aliases_and_explicit_null() {
    let supplied = json!({
        "thread_source":{"feature":"automation"},
        "subagent_source":"thread_spawn",
        "root_turn_id":"client-root-turn"
    });
    let aliases = json!({
        "threadSource":supplied["thread_source"],
        "subagentSource":supplied["subagent_source"],
        "rootTurnId":supplied["root_turn_id"]
    });
    let empty = json!({"thread_source":null,"subagent_source":null,"root_turn_id":null});
    let handshake = json!({
        "session_id":"logical-session", "thread_id":"logical-thread", "turn_id":"header-turn",
        "thread_source":"handshake-source", "subagent_source":"handshake-agent",
        "root_turn_id":"handshake-root-turn", "experimental_api_enabled":true
    });
    for (fields, header_sources, expected, experimental) in [
        (
            supplied.clone(),
            handshake.clone(),
            supplied.clone(),
            json!(true),
        ),
        (
            aliases.clone(),
            handshake.clone(),
            supplied.clone(),
            json!(true),
        ),
        (
            json!({"client_metadata":supplied}),
            handshake.clone(),
            supplied.clone(),
            json!(true),
        ),
        (
            json!({"client_metadata":aliases}),
            handshake.clone(),
            supplied.clone(),
            json!(true),
        ),
        (
            json!({"turn_metadata":supplied.to_string()}),
            handshake.clone(),
            supplied.clone(),
            json!(true),
        ),
        (
            json!({"client_metadata":{"x-codex-turn-metadata":aliases.to_string()}}),
            handshake.clone(),
            supplied.clone(),
            json!(true),
        ),
        (json!({}), handshake.clone(), handshake.clone(), json!(true)),
        (
            json!({"client_metadata":empty}),
            handshake.clone(),
            empty.clone(),
            json!(true),
        ),
        (empty.clone(), handshake.clone(), empty.clone(), json!(true)),
        (
            json!({"experimentalApiEnabled":false}),
            handshake.clone(),
            handshake.clone(),
            json!(false),
        ),
        (
            json!({"client_metadata":{"capabilities":{"experimentalApi":false}}}),
            handshake.clone(),
            handshake.clone(),
            json!(false),
        ),
        (
            json!({"capabilities":{"experimentalApi":null}}),
            handshake,
            json!({
                "thread_source":"handshake-source", "subagent_source":"handshake-agent",
                "root_turn_id":"handshake-root-turn"
            }),
            Value::Null,
        ),
        (json!({}), json!({}), empty, Value::Null),
    ] {
        let upstream = http_response(&sse(&events_for(&[json!({
            "type":"web_search_call", "id":"ws_request_sources", "status":"completed",
            "action":{"type":"search", "query":"private-query"}
        })])))
        .await;
        mount_analytics(&upstream, 200).await;
        let (client, task) = client(&upstream.uri(), analytics_settings());
        let mut body = codex_request("gpt-test", "private-prompt", Vec::new())
            .body()
            .clone();
        body.extend(fields.as_object().unwrap().clone());
        let request = provider_openai::encode_generate_request(
            &GenerateRequest::from_protocol_payload(
                ProtocolPayload::json_object("openai", body)
                    .unwrap()
                    .with_context(Map::from_iter([("use_websocket".into(), json!(false))])),
            ),
            "gpt-test",
            None,
        )
        .unwrap();
        let mut metadata = json!({"session_id":"logical-session", "thread_id":"logical-thread", "turn_id":"header-turn"});
        metadata
            .as_object_mut()
            .unwrap()
            .extend(header_sources.as_object().unwrap().clone());
        let metadata = metadata.to_string();
        let mut context = context("account-request-sources");
        context.turn_metadata = Some(&metadata);
        let mut response = client
            .create_response_stream(&request, context)
            .await
            .unwrap();
        while let Some(chunk) = response.body.next().await {
            chunk.unwrap();
        }
        flush(task.as_ref()).await.unwrap();
        let events = payloads(&upstream).await;
        assert_eq!(events.len(), 1, "{fields}");
        let params = &events[0]["event_params"];
        for field in ["thread_source", "subagent_source", "root_turn_id"] {
            assert_eq!(params[field], expected[field], "{field}: {fields}");
        }
        assert_eq!(
            params["app_server_client"]["experimental_api_enabled"], experimental,
            "{fields}"
        );
    }
}

#[tokio::test]
async fn hosted_tool_summaries_follow_official_action_and_outcome_rules() {
    let cases = [
        (
            json!({"type":"search", "queries":[]}),
            "search",
            false,
            json!(0),
        ),
        (
            json!({"type":"search", "query":" ", "queries":["private-query"]}),
            "search",
            false,
            json!(1),
        ),
        (
            json!({"type":"search", "queries":[" ", "private-query"]}),
            "search",
            true,
            json!(2),
        ),
        (
            json!({"type":"search", "query":"private-query"}),
            "search",
            true,
            json!(1),
        ),
        (
            json!({"type":"open_page", "url":"https://private-url.test"}),
            "open_page",
            true,
            Value::Null,
        ),
        (
            json!({"type":"find_in_page", "pattern":""}),
            "find_in_page",
            true,
            Value::Null,
        ),
        (json!({"type":"unrecognized"}), "other", false, Value::Null),
        (Value::Null, "other", false, Value::Null),
    ];
    let mut items: Vec<_> = cases.iter().enumerate().map(|(index, (action, ..))| {
        json!({"type":"web_search_call", "id":format!("ws_action_{index}"), "status":"completed", "action":action})
    }).collect();
    items.push(json!({"type":"image_generation_call", "id":"ig_completed", "status":"completed", "result":"private-image"}));
    let upstream = http_response(&sse(&events_for(&items))).await;
    mount_analytics(&upstream, 200).await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    run(&client, context("account-actions")).await;
    flush(task.as_ref()).await.unwrap();
    let events = payloads(&upstream).await;
    assert_eq!(events.len(), cases.len() + 1);
    for (event, (_, action, present, count)) in events.iter().zip(cases.iter()) {
        let params = &event["event_params"];
        assert_eq!(params["web_search_action"], *action);
        assert_eq!(params["query_present"], *present);
        assert_eq!(params["query_count"], *count);
        assert!(!event.to_string().contains("private-"));
    }
    let image = &events.last().unwrap()["event_params"];
    assert_eq!(image["terminal_status"], "completed");
    assert_eq!(image["failure_kind"], Value::Null);
    assert_eq!(image["revised_prompt_present"], false);
    assert_eq!(image["saved_path_present"], false);
}
