//! 为传输、目录和轮次采样共用冻结的指标身份

use std::sync::Arc;

use opentelemetry::KeyValue;

use super::{metrics::Metrics, sanitize_tag};

#[derive(Clone)]
pub(super) struct Recorder {
    pub(super) metrics: Arc<Metrics>,
    pub(super) attributes: Vec<KeyValue>,
    pub(super) product_sku: Option<String>,
    pub(super) excluded: Vec<&'static str>,
}

impl Recorder {
    pub(super) fn tags(&self, tags: &[(&str, &str)]) -> Vec<KeyValue> {
        let mut attributes = self.attributes.clone();
        for (key, value) in tags {
            attributes.retain(|attribute| attribute.key.as_str() != *key);
            attributes.push(KeyValue::new((*key).to_owned(), sanitize_tag(value)));
        }
        attributes
    }

    pub(super) fn count(&self, name: &'static str, value: u64, tags: &[(&str, &str)]) {
        if self.excluded.contains(&name) {
            return;
        }
        self.metrics.add(name, value, &self.tags(tags));
    }

    pub(super) fn sample(&self, name: &'static str, value: f64, tags: &[(&str, &str)]) {
        if self.excluded.contains(&name) {
            return;
        }
        self.metrics.histogram(name, value, &self.tags(tags));
    }

    pub(super) fn with_model(&self, model: &str) -> Self {
        Self {
            attributes: self.tags(&[("model", model)]),
            ..self.clone()
        }
    }
}

pub(super) fn boolean(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}
