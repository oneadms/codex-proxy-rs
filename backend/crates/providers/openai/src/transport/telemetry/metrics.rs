//! 官方 Codex 同版本 SDK、Delta 聚合和 OTLP HTTP JSON 导出

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter, MeterProvider as _},
};
use opentelemetry_otlp::{
    Compression, MetricExporter, Protocol, WithExportConfig as _, WithHttpConfig as _,
};
use opentelemetry_sdk::{
    Resource,
    metrics::{
        InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Temporality,
        data::ResourceMetrics, exporter::PushMetricExporter as _, reader::MetricReader,
    },
};

use super::{CodexMetricsExporter, CodexTelemetrySettings, CodexWireProfile, sanitize_tag};

// 依据 openai/codex 98f8a3311ba7 的 otel/src/config.rs 与 metrics/{config,client}.rs
// 此值是官方公开客户端标识，不是账号令牌；模型请求的认证头不会进入指标出口
const STATSIG_CLIENT_KEY: &str = "client-MkRuleRQBd6qakfnDYqJVR9JuXcY57Ljly3vi5JVUIO";
const STATSIG_ENDPOINT: &str = "https://ab.chatgpt.com/otlp/v1/metrics";
const STATSIG_DISABLED: &[&str] = &[
    "codex.api_request",
    "codex.api_request.duration_ms",
    "codex.conversation.turn.count",
    "exec_server_client_requests_total",
    "codex.responses_api_engine_iapi_ttft.duration_ms",
    "codex.responses_api_engine_service_tbt.duration_ms",
    "codex.responses_api_engine_service_ttft.duration_ms",
    "codex.tool.call",
    "codex.tool.call.duration_ms",
    "codex.turn.cost_microusd",
    "codex.turn.token_usage",
];
const DURATION_BOUNDARIES: &[f64] = &[
    0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 1250.0, 1500.0, 1750.0,
    2000.0, 2250.0, 2500.0, 3000.0, 3500.0, 4000.0, 4500.0, 5000.0, 6000.0, 7000.0, 7500.0, 8000.0,
    9000.0, 10000.0, 12000.0, 15000.0, 20000.0, 30000.0, 60000.0, 120000.0,
];

pub(super) struct Metrics {
    _provider: SdkMeterProvider,
    reader: Arc<ManualReader>,
    exporter: MetricExporter,
    meter: Meter,
    statsig: bool,
    counters: Mutex<HashMap<&'static str, Counter<u64>>>,
    histograms: Mutex<HashMap<&'static str, Histogram<f64>>>,
    pending: AtomicBool,
}

impl Metrics {
    pub(super) fn new(
        settings: &CodexTelemetrySettings,
        profile: &CodexWireProfile,
        client: reqwest::Client,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let statsig = matches!(settings.metrics_exporter, CodexMetricsExporter::Statsig);
        let (endpoint, headers) = match &settings.metrics_exporter {
            CodexMetricsExporter::Statsig => (
                STATSIG_ENDPOINT.to_owned(),
                HashMap::from([("statsig-api-key".to_owned(), STATSIG_CLIENT_KEY.to_owned())]),
            ),
            CodexMetricsExporter::OtlpHttp { endpoint, headers } => {
                (endpoint.clone(), headers.clone())
            }
            CodexMetricsExporter::None => return Err("metrics exporter is disabled".into()),
        };
        let mut builder = MetricExporter::builder()
            .with_http()
            .with_http_client(client)
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpJson)
            .with_temporality(Temporality::Delta)
            .with_timeout(Duration::from_secs(10))
            .with_headers(headers);
        if statsig
            && [
                "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
                "OTEL_EXPORTER_OTLP_COMPRESSION",
                "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
                "OTEL_EXPORTER_OTLP_HEADERS",
            ]
            .iter()
            .all(|key| std::env::var_os(key).is_none())
        {
            builder = builder.with_compression(Compression::Zstd);
        }
        let exporter = builder.build()?;
        let reader = Arc::new(
            ManualReader::builder()
                .with_temporality(Temporality::Delta)
                .build(),
        );
        let provider = SdkMeterProvider::builder()
            .with_resource(
                Resource::builder()
                    .with_service_name(profile.originator.clone())
                    .with_attributes([
                        KeyValue::new("service.version", profile.codex_version.clone()),
                        KeyValue::new("env", settings.environment.clone()),
                        KeyValue::new("os", sanitize_tag(&profile.os_type)),
                        KeyValue::new("os_version", sanitize_tag(&profile.os_version)),
                    ])
                    .build(),
            )
            .with_reader(SharedReader(Arc::clone(&reader)))
            .build();
        let meter = provider.meter("codex");
        Ok(Self {
            _provider: provider,
            reader,
            exporter,
            meter,
            statsig,
            counters: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
            pending: AtomicBool::new(false),
        })
    }

    pub(super) fn add(&self, name: &'static str, value: u64, attributes: &[KeyValue]) {
        if self.statsig && STATSIG_DISABLED.contains(&name) {
            return;
        }
        self.counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(name)
            .or_insert_with(|| self.meter.u64_counter(name).build())
            .add(value.min(i64::MAX as u64), attributes);
        self.pending.store(true, Ordering::Release);
    }

    pub(super) fn histogram(&self, name: &'static str, value: f64, attributes: &[KeyValue]) {
        if self.statsig && STATSIG_DISABLED.contains(&name) {
            return;
        }
        if !value.is_finite() || value < 0.0 {
            return;
        }
        let value = if matches!(
            name,
            "codex.responses_api_engine_iapi_tbt.duration_ms"
                | "codex.responses_api_engine_service_tbt.duration_ms"
        ) {
            value
        } else {
            value.min(i64::MAX as f64).floor()
        };
        self.histograms
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(name)
            .or_insert_with(|| {
                let mut instrument = self.meter.f64_histogram(name);
                if name.ends_with("_ms") {
                    instrument = instrument
                        .with_unit("ms")
                        .with_description("Duration in milliseconds.");
                }
                if let Some(boundaries) = boundaries(name) {
                    instrument = instrument.with_boundaries(boundaries);
                }
                instrument.build()
            })
            .record(value, attributes);
        self.pending.store(true, Ordering::Release);
    }

    pub(super) fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    pub(super) async fn export(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 先清标记再收集，收集期间写入的新样本会保留聚合器到下一轮
        self.pending.store(false, Ordering::Release);
        let mut metrics = ResourceMetrics::default();
        self.reader.collect(&mut metrics)?;
        if metrics
            .scope_metrics()
            .any(|scope| scope.metrics().next().is_some())
        {
            tokio::time::timeout(Duration::from_secs(10), self.exporter.export(&metrics)).await??;
        }
        Ok(())
    }
}

fn boundaries(name: &str) -> Option<Vec<f64>> {
    // 普通数量直方图沿用 SDK 默认桶；仅覆盖官方显式指定的桶和单位
    Some(match name {
        "codex.responses.reasoning_items_removed" => {
            vec![0., 1., 2., 4., 8., 16., 32., 64., 128., 256.]
        }
        "codex.guardian.review.truncation.bytes" => vec![
            0., 1., 2., 4., 8., 16., 64., 256., 1024., 4096., 16384., 65536., 262144., 1048576.,
            4194304., 8388608., 16777216.,
        ],
        "codex.tools.registered" => vec![
            0., 1., 2., 5., 10., 20., 30., 50., 75., 100., 150., 200., 300., 500., 750., 1000.,
            2000., 5000., 10000., 20000.,
        ],
        "codex.thread.skills.enabled_total" | "codex.thread.skills.kept_total" => {
            (0..=512).map(f64::from).collect()
        }
        "codex.thread.skills.truncated" => vec![0., 1.],
        "codex.thread.skills.description_truncated_chars" => log_boundaries(17.),
        "codex.tools.spec_description_bytes" | "codex.tools.exec_update_bytes" => {
            log_boundaries(20.)
        }
        "codex.additional_context.value_bytes" => {
            let mut values = log_boundaries(22.);
            values.push(4096.);
            values.sort_by(f64::total_cmp);
            values.dedup();
            values
        }
        "codex.thread.tools.namespaces_total" | "codex.thread.tools.fragment_bytes" => {
            log_boundaries(15.)
        }
        "codex.turn.critical_path.duration_ms" | "codex.turn.critical_path.component_ms" => {
            let mut values = DURATION_BOUNDARIES.to_vec();
            values.extend([
                180000., 300000., 600000., 900000., 1800000., 3600000., 7200000.,
            ]);
            values
        }
        _ if name.ends_with("_ms") => DURATION_BOUNDARIES.to_vec(),
        _ => return None,
    })
}

fn log_boundaries(exponent: f64) -> Vec<f64> {
    (0..511)
        .map(|index| {
            if index == 0 {
                0.
            } else {
                2_f64.powf(exponent * f64::from(index - 1) / 509.)
            }
        })
        .collect()
}

#[derive(Debug)]
struct SharedReader(Arc<ManualReader>);

impl MetricReader for SharedReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.0.register_pipeline(pipeline);
    }
    fn collect(&self, metrics: &mut ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.collect(metrics)
    }
    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}
