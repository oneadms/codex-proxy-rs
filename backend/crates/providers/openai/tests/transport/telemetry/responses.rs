//! 验证响应诊断的可用性、移除计数和零值桶

use super::*;

#[tokio::test]
async fn reasoning_diagnostics_preserve_unavailable_zero_and_bounded_removal_reasons() {
    for (transformations, report, counts) in [
        (None, "unavailable", None),
        (Some(json!({})), "unavailable", None),
        (
            Some(json!([])),
            "no_removals_reported",
            Some([0, 0, 0, 0, 0, 0]),
        ),
        (
            Some(json!([
                {"type":"reasoning","action":"removed","reason":"prefix_mismatch"},
                {"type":"reasoning","action":"removed","reason":"prefix_mismatch"},
                {"type":"reasoning","action":"removed","reason":"unknown-private-reason"},
                {"type":"message","action":"removed","reason":"model_mismatch"},
                {"type":"reasoning","action":"retained","reason":"end_user_mismatch"}
            ])),
            "prefix_mismatch_reported",
            Some([0, 0, 0, 2, 0, 1]),
        ),
    ] {
        let collector = collector(200).await;
        let mut completed: Value =
            serde_json::from_str(&completed_websocket_response("resp-reasoning", 2, 1)).unwrap();
        if let Some(transformations) = transformations {
            completed["response"]["input_transformations"] = transformations;
        }
        // 重复的终结帧仍需透明转发，诊断样本只记一次
        let body = format!("data: {completed}\n\ndata: {completed}\n\n");
        let upstream = http_response(&body).await;
        let (client, task) = client(&upstream.uri(), settings(&collector));
        assert_eq!(run_http(&client).await.body, body);
        flush(task.as_ref()).await.unwrap();
        let exports = collected(&collector).await;
        let payload = &exports[0];
        let replay = &metric(payload, "codex.responses.reasoning_replay")["sum"]["dataPoints"][0];
        assert_eq!(replay["asInt"], 1);
        assert_eq!(attributes(replay)["report"], report);
        if let Some(counts) = counts {
            let removed = metric(payload, "codex.responses.reasoning_items_removed");
            assert!(removed.get("unit").is_none_or(|unit| unit == ""));
            let points = removed["histogram"]["dataPoints"].as_array().unwrap();
            assert_eq!(points.len(), 6);
            for (reason, expected) in [
                "organization_mismatch",
                "end_user_mismatch",
                "model_mismatch",
                "prefix_mismatch",
                "turn_boundary",
                "other",
            ]
            .into_iter()
            .zip(counts)
            {
                let point = points
                    .iter()
                    .find(|point| attributes(point)["reason"] == reason)
                    .unwrap();
                assert_eq!(point["count"], 1);
                assert_eq!(point["sum"], expected as f64);
                assert_eq!(
                    point["explicitBounds"],
                    json!([0., 1., 2., 4., 8., 16., 32., 64., 128., 256.])
                );
            }
        } else {
            assert!(
                !payload
                    .to_string()
                    .contains("codex.responses.reasoning_items_removed")
            );
        }
        assert!(!payload.to_string().contains("unknown-private-reason"));
    }
}
