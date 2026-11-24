//! 验证 Codex 官方指标、analytics 协议、真实传输采样和导出任务生命周期

use super::*;
use gateway_core::{
    lifecycle::CancellationToken,
    task::{DaemonTask, WorkerContribution, WorkerDaemonContext, WorkerRunnable},
};
use provider_openai::config::{CodexMetricsExporter, CodexTelemetrySettings};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

mod analytics;
mod hosted_tools;
mod metrics;
mod session;

fn settings(collector: &MockServer) -> CodexTelemetrySettings {
    CodexTelemetrySettings {
        metrics_exporter: CodexMetricsExporter::OtlpHttp {
            endpoint: format!("{}/v1/metrics", collector.uri()),
            headers: std::collections::HashMap::from([(
                "x-collector-token".into(),
                "test-collector-token".into(),
            )]),
        },
        ..CodexTelemetrySettings::default()
    }
}

fn client(
    base_url: &str,
    settings: CodexTelemetrySettings,
) -> (CodexBackendClient, Box<dyn DaemonTask>) {
    let (client, worker) = CodexBackendClient::new(
        reqwest::Client::builder().no_proxy().build().unwrap(),
        base_url,
        test_wire_profile(),
    )
    .with_telemetry(settings)
    .unwrap();
    let WorkerContribution::Registration(worker) = worker else {
        panic!("telemetry worker");
    };
    let WorkerRunnable::Daemon { task, .. } = worker.runnable else {
        panic!("telemetry daemon");
    };
    (client, task)
}

async fn flush(task: &dyn DaemonTask) -> Result<(), gateway_core::task::WorkerTaskError> {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    timeout(
        Duration::from_secs(30),
        task.run(WorkerDaemonContext::new(cancellation)),
    )
    .await
    .expect("bounded telemetry drain")
}

async fn collector(status: u16) -> MockServer {
    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/metrics"))
        .respond_with(ResponseTemplate::new(status))
        .mount(&collector)
        .await;
    collector
}

async fn collected(collector: &MockServer) -> Vec<Value> {
    collector
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| {
            assert_eq!(request.headers["content-type"], "application/json");
            assert_eq!(request.headers["x-collector-token"], "test-collector-token");
            assert!(!request.headers.contains_key("authorization"));
            serde_json::from_slice(&request.body).expect("OTLP JSON")
        })
        .collect()
}

fn metric<'a>(payload: &'a Value, name: &str) -> &'a Value {
    payload["resourceMetrics"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|resource| resource["scopeMetrics"].as_array().unwrap())
        .flat_map(|scope| scope["metrics"].as_array().unwrap())
        .find(|metric| metric["name"] == name)
        .unwrap_or_else(|| panic!("missing {name}: {payload}"))
}

fn attributes(point: &Value) -> std::collections::HashMap<&str, &str> {
    point["attributes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|attribute| {
            (
                attribute["key"].as_str().unwrap(),
                attribute["value"]["stringValue"].as_str().unwrap(),
            )
        })
        .collect()
}

async fn http_response(body: &str) -> MockServer {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
        .mount(&upstream)
        .await;
    upstream
}

fn completed_sse() -> String {
    format!(
        "event: response.completed\ndata: {}\n\n",
        completed_websocket_response("resp_metrics", 2, 1)
    )
}

async fn run_http(client: &CodexBackendClient) -> CollectedBackendResponse {
    let mut request = codex_request("gpt-test", "prompt-private-marker", Vec::new());
    request.force_http_sse = true;
    client
        .create_response(
            &request,
            request_context("telemetry-request", Some("account-private-marker")),
        )
        .await
        .unwrap()
}
