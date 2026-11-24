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
            json!({"experimental_api_enabled":"true"}),
            handshake.clone(),
            handshake.clone(),
            Value::Null,
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
            Some("search"),
            false,
            json!(0),
        ),
        (
            json!({"type":"search", "query":" ", "queries":["private-query"]}),
            Some("search"),
            false,
            json!(1),
        ),
        (
            json!({"type":"search", "queries":[" ", "private-query"]}),
            Some("search"),
            true,
            json!(2),
        ),
        (
            json!({"type":"search", "query":"private-query"}),
            Some("search"),
            true,
            json!(1),
        ),
        (
            json!({"type":"open_page", "url":"https://private-url.test"}),
            Some("open_page"),
            true,
            Value::Null,
        ),
        (
            json!({"type":"find_in_page", "pattern":""}),
            Some("find_in_page"),
            true,
            Value::Null,
        ),
        (
            json!({"type":"unrecognized"}),
            Some("other"),
            false,
            Value::Null,
        ),
        (Value::Null, None, false, Value::Null),
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
        assert_eq!(params["web_search_action"], json!(action));
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

#[tokio::test]
async fn hosted_tool_metadata_uses_request_sources_without_replacing_explicit_values() {
    let supplied = json!({
        "turn_trigger":"scheduled", "cell_id":"cell-request", "parent_call_id":"call-parent",
        "parent_thread_id":"thread-parent", "subsequent_response_id":"resp-next",
        "execution_duration_ms":23, "review_count":3, "guardian_review_count":1,
        "user_review_count":2, "final_approval_outcome":"user_approved",
        "requested_additional_permissions":true, "requested_network_access":true
    });
    let aliases = json!({
        "turnTrigger":"scheduled", "cellId":"cell-request", "parentCallId":"call-parent",
        "parentThreadId":"thread-parent", "subsequentResponseId":"resp-next",
        "executionDurationMs":23, "reviewCount":3, "guardianReviewCount":1,
        "userReviewCount":2, "finalApprovalOutcome":"user_approved",
        "requestedAdditionalPermissions":true, "requestedNetworkAccess":true
    });
    let explicit = json!({
        "turnTrigger":null, "cellId":null, "parentCallId":null, "parentThreadId":null,
        "subsequentResponseId":null, "executionDurationMs":null, "reviewCount":0,
        "guardianReviewCount":0, "userReviewCount":0, "finalApprovalOutcome":"not_needed",
        "requestedAdditionalPermissions":false, "requestedNetworkAccess":false
    });
    let explicit_expected = json!({
        "cell_id":null, "parent_call_id":null, "parent_thread_id":null,
        "subsequent_response_id":null, "execution_duration_ms":null, "review_count":0,
        "guardian_review_count":0, "user_review_count":0, "final_approval_outcome":"not_needed",
        "requested_additional_permissions":false, "requested_network_access":false
    });
    let mut prefixed = aliases.clone();
    prefixed.as_object_mut().unwrap().remove("parentThreadId");
    prefixed["x-codex-parent-thread-id"] = json!("thread-parent");
    for (fields, expected) in [
        (supplied.clone(), supplied.clone()),
        (
            json!({"client_metadata":aliases, "cell_id":"lower-priority"}),
            supplied.clone(),
        ),
        (
            json!({"client_metadata":prefixed, "parent_thread_id":"lower-priority"}),
            supplied.clone(),
        ),
        (
            json!({"turn_metadata":aliases.to_string()}),
            supplied.clone(),
        ),
        (json!({}), supplied.clone()),
        (json!({"client_metadata":explicit}), explicit_expected),
    ] {
        let upstream = http_response(&sse(&events_for(&[json!({
            "id":"ws_metadata", "type":"web_search_call", "status":"completed",
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
        let mut header_metadata: Value = serde_json::from_str(TURN_METADATA).unwrap();
        header_metadata
            .as_object_mut()
            .unwrap()
            .extend(supplied.as_object().unwrap().clone());
        let header_metadata = header_metadata.to_string();
        let mut context = context("account-metadata");
        context.installation_id = None;
        context.turn_metadata = Some(&header_metadata);
        let mut response = client
            .create_response_stream(&request, context)
            .await
            .unwrap();
        while let Some(chunk) = response.body.next().await {
            chunk.unwrap();
        }
        flush(task.as_ref()).await.unwrap();
        let events = payloads(&upstream).await;
        assert_eq!(events.len(), 1);
        let params = &events[0]["event_params"];
        for (field, value) in expected.as_object().unwrap() {
            assert_eq!(&params[field], value, "{field}: {fields}");
        }
        if expected.get("turn_trigger").is_none() {
            assert!(params.get("turn_trigger").is_none());
        }
        assert!(params.get("imagegen_request_id").is_none());
        assert!(params.get("transparent_background").is_none());
    }
}

#[tokio::test]
async fn image_metadata_prefers_actual_item_values_and_retains_start_only_summaries() {
    let events = vec![
        json!({"type":"response.created", "response":{"id":"resp_actual"}}),
        json!({"type":"response.output_item.added", "item":{
            "id":"ig_metadata", "type":"image_generation_call", "status":"in_progress",
            "cellId":"cell-start", "imagegenRequestId":"image-start", "generationId":"generation-start",
            "executionDurationMs":12, "saved_path":"private-start-path"
        }}),
        json!({"type":"response.output_item.done", "item":{
            "id":"ig_metadata", "type":"image_generation_call", "status":"failed",
            "generation_id":null, "transparent_background":false, "reviewCount":0,
            "requestedNetworkAccess":false, "turnTrigger":null, "saved_path":null,
            "revised_prompt":"private-revised-prompt", "result":"private-image"
        }}),
        serde_json::from_str(&completed_websocket_response("resp_actual", 2, 1)).unwrap(),
    ];
    let upstream = http_response(&sse(&events)).await;
    mount_analytics(&upstream, 200).await;
    let (client, task) = client(&upstream.uri(), analytics_settings());
    let mut request = http_request(&[]);
    request.set_client_metadata(Some(json!({
        "turnTrigger":"requested", "cellId":"cell-request", "parentCallId":"call-request",
        "originatingResponseId":"resp-untrusted", "subsequentResponseId":"resp-next",
        "executionDurationMs":99, "reviewCount":2, "requestedNetworkAccess":true,
        "imagegenRequestId":"image-request", "generationId":"generation-request",
        "transparentBackground":true, "savedPathPresent":true
    })));
    let mut response = client
        .create_response_stream(&request, context("account-image-metadata"))
        .await
        .unwrap();
    while let Some(chunk) = response.body.next().await {
        chunk.unwrap();
    }
    flush(task.as_ref()).await.unwrap();
    let events = payloads(&upstream).await;
    assert_eq!(events.len(), 1);
    let params = &events[0]["event_params"];
    for (field, expected) in [
        ("cell_id", json!("cell-start")),
        ("parent_call_id", json!("call-request")),
        ("originating_response_id", json!("resp_actual")),
        ("subsequent_response_id", json!("resp-next")),
        ("execution_duration_ms", json!(12)),
        ("review_count", json!(0)),
        ("requested_network_access", json!(false)),
        ("imagegen_request_id", json!("image-start")),
        ("generation_id", Value::Null),
        ("transparent_background", json!(false)),
        ("saved_path_present", json!(false)),
        ("revised_prompt_present", json!(true)),
        ("terminal_status", json!("failed")),
        ("failure_kind", json!("tool_error")),
    ] {
        assert_eq!(params[field], expected, "{field}");
    }
    assert!(params.get("turn_trigger").is_none());
    assert!(!events[0].to_string().contains("private-"));
}

#[tokio::test]
async fn image_metadata_uses_request_values_and_ignores_values_outside_the_official_types() {
    for (metadata, header_fields, expected) in [
        (
            json!({
                "imagegenRequestId":"image-request", "generationId":"generation-request",
                "transparentBackground":false, "savedPath":"private-local-path",
                "revisedPromptPresent":true
            }),
            json!({}),
            json!({
                "imagegen_request_id":"image-request", "generation_id":"generation-request",
                "transparent_background":false, "saved_path_present":true, "revised_prompt_present":true
            }),
        ),
        (
            json!({
                "imagegenRequestId":null, "generationId":null, "transparentBackground":null,
                "savedPathPresent":false, "revisedPromptPresent":false
            }),
            json!({
                "imagegen_request_id":"image-handshake", "generation_id":"generation-handshake",
                "transparent_background":true, "saved_path":"private-handshake-path",
                "revised_prompt":"private-handshake-prompt"
            }),
            json!({
                "imagegen_request_id":null, "generation_id":null, "transparent_background":null,
                "saved_path_present":false, "revised_prompt_present":false
            }),
        ),
        (
            json!({
                "imagegen_request_id":123, "generation_id":{}, "transparent_background":"true",
                "saved_path_present":null, "revised_prompt_present":"true", "cell_id":[],
                "review_count":-1, "guardian_review_count":null, "user_review_count":0.5,
                "final_approval_outcome":"private-invalid-approval", "execution_duration_ms":-2,
                "requested_additional_permissions":1, "requested_network_access":"true", "turn_trigger":[]
            }),
            json!({}),
            json!({
                "imagegen_request_id":null, "generation_id":null, "transparent_background":null,
                "saved_path_present":false, "revised_prompt_present":false, "cell_id":null,
                "review_count":0, "guardian_review_count":0, "user_review_count":0,
                "final_approval_outcome":"unknown", "execution_duration_ms":null,
                "requested_additional_permissions":false, "requested_network_access":false
            }),
        ),
    ] {
        let upstream = http_response(&sse(&events_for(&[json!({
            "id":"ig_request", "type":"image_generation_call", "status":"completed", "result":"private-image"
        })]))).await;
        mount_analytics(&upstream, 200).await;
        let (client, task) = client(&upstream.uri(), analytics_settings());
        let mut request = http_request(&[]);
        request.set_client_metadata(Some(metadata));
        let mut header_metadata: Value = serde_json::from_str(TURN_METADATA).unwrap();
        header_metadata
            .as_object_mut()
            .unwrap()
            .extend(header_fields.as_object().unwrap().clone());
        let header_metadata = header_metadata.to_string();
        let mut context = context("account-image-request");
        context.turn_metadata = Some(&header_metadata);
        let mut response = client
            .create_response_stream(&request, context)
            .await
            .unwrap();
        while let Some(chunk) = response.body.next().await {
            chunk.unwrap();
        }
        flush(task.as_ref()).await.unwrap();
        let events = payloads(&upstream).await;
        assert_eq!(events.len(), 1);
        for (field, value) in expected.as_object().unwrap() {
            assert_eq!(&events[0]["event_params"][field], value, "{field}");
        }
        assert!(events[0]["event_params"].get("turn_trigger").is_none());
        assert!(!events[0].to_string().contains("private-"));
    }
}
