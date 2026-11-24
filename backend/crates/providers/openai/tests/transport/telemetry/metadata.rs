//! 验证官方请求元数据的子代理来源和每请求 analytics 同意状态

use super::{analytics::*, *};

#[tokio::test]
async fn subagent_source_reads_official_metadata_without_overwriting_frame_values() {
    for (metadata, header_fields, expected) in [
        (
            json!({"subagent_kind":"thread_spawn"}),
            json!({"subagent_source":"review"}),
            json!("thread_spawn"),
        ),
        (
            json!({"subagentKind":"compact"}),
            json!({}),
            json!("compact"),
        ),
        (
            json!({"subagent_source":null, "subagent_kind":"review"}),
            json!({"subagent_kind":"thread_spawn"}),
            Value::Null,
        ),
        (
            json!({"x-openai-subagent":"memory_consolidation"}),
            json!({}),
            json!("memory_consolidation"),
        ),
        (
            json!({"x-codex-turn-metadata":json!({"subagent_kind":"review"}).to_string()}),
            json!({"subagent_kind":"compact"}),
            json!("review"),
        ),
        (
            json!({}),
            json!({"subagent_kind":"compact"}),
            json!("compact"),
        ),
        (json!({}), json!({}), json!("thread_spawn")),
    ] {
        let upstream = http_response(&sse(&events_for(&[json!({
            "id":"ws_subagent", "type":"web_search_call", "status":"completed"
        })])))
        .await;
        mount_analytics(&upstream, 200).await;
        let (client, task) = client(&upstream.uri(), analytics_settings());
        let mut request = http_request(&[("x-openai-subagent", "thread_spawn")]);
        request.set_client_metadata(Some(metadata.clone()));
        let mut header: Value = serde_json::from_str(TURN_METADATA).unwrap();
        header
            .as_object_mut()
            .unwrap()
            .extend(header_fields.as_object().unwrap().clone());
        let header = header.to_string();
        let mut context = context("account-subagent-source");
        context.turn_metadata = Some(&header);
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
        assert_eq!(
            events[0]["event_params"]["subagent_source"], expected,
            "{metadata}"
        );
    }
}

#[tokio::test]
async fn request_consent_controls_analytics_but_keeps_explicit_otlp_metrics() {
    for globally_enabled in [false, true] {
        for (metadata, header_consent, allowed) in [
            (json!({"analytics_enabled":false}), json!(true), false),
            (json!({"analyticsEnabled":true}), json!(false), true),
            (
                json!({"x-codex-turn-metadata":json!({"analytics_enabled":false}).to_string()}),
                json!(true),
                false,
            ),
            (json!({}), json!(false), false),
            (json!({}), json!(true), true),
            (json!({"analytics_enabled":null}), json!(false), true),
            (json!({"analytics_enabled":"false"}), json!(true), true),
        ] {
            let upstream = http_response(&sse(&events_for(&[json!({
                "id":"ws_consent", "type":"web_search_call", "status":"completed"
            })])))
            .await;
            mount_analytics(&upstream, 200).await;
            let collector = collector(200).await;
            let (client, task) = client(
                &upstream.uri(),
                CodexTelemetrySettings {
                    analytics_enabled: globally_enabled,
                    ..settings(&collector)
                },
            );
            let mut request = http_request(&[]);
            request.set_client_metadata(Some(metadata.clone()));
            let mut header: Value = serde_json::from_str(TURN_METADATA).unwrap();
            header["analytics_enabled"] = header_consent;
            let header = header.to_string();
            let mut context = context("account-request-consent");
            context.turn_metadata = Some(&header);
            let mut response = client
                .create_response_stream(&request, context)
                .await
                .unwrap();
            while let Some(chunk) = response.body.next().await {
                chunk.unwrap();
            }
            flush(task.as_ref()).await.unwrap();
            assert_eq!(
                payloads(&upstream).await.len(),
                usize::from(globally_enabled && allowed),
                "{metadata}"
            );
            let exports = collected(&collector).await;
            assert_eq!(exports.len(), 1);
            assert_eq!(
                metric(&exports[0], "codex.api_request")["sum"]["dataPoints"][0]["asInt"],
                1
            );
        }
    }
}
