//! 官方 analytics 的有界队列、账号认证出口与重复事件隔离

use std::{num::NonZeroUsize, sync::Mutex, time::Duration};

use lru::LruCache;
use reqwest::{Client, header::HeaderMap};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

// 对齐 openai/codex 98f8a3311ba7 analytics/src/client.rs 的容量与请求期限
const QUEUE_CAPACITY: usize = 256;
const DEDUPE_CAPACITY: usize = 4096;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const FLUSH_TIMEOUT: Duration = Duration::from_secs(25);

pub(super) struct Analytics {
    sender: mpsc::Sender<Delivery>,
    pub(super) receiver: tokio::sync::Mutex<mpsc::Receiver<Delivery>>,
    emitted: Mutex<LruCache<[u8; 32], ()>>,
}

/// 凭据仅随当前请求的事件短暂排队，不进入共享出口注册表或调试输出
pub(super) struct Route {
    pub(super) client: Client,
    pub(super) endpoint: String,
    pub(super) headers: HeaderMap,
    pub(super) scope: String,
}

pub(super) struct Delivery {
    route: std::sync::Arc<Route>,
    event: Event,
}

#[derive(Serialize)]
pub(super) struct Event {
    pub(super) event_type: &'static str,
    pub(super) event_params: Value,
}

impl Analytics {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        Self {
            sender,
            receiver: tokio::sync::Mutex::new(receiver),
            emitted: Mutex::new(LruCache::new(
                NonZeroUsize::new(DEDUPE_CAPACITY).expect("nonzero analytics dedupe capacity"),
            )),
        }
    }

    pub(super) fn enqueue(&self, route: std::sync::Arc<Route>, event: Event) {
        let mut digest = Sha256::new();
        for part in [
            route.scope.as_str(),
            route.endpoint.as_str(),
            event.event_type,
            event.event_params["thread_id"].as_str().unwrap_or_default(),
            event.event_params["turn_id"].as_str().unwrap_or_default(),
            event.event_params["item_id"].as_str().unwrap_or_default(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part.as_bytes());
        }
        let key: [u8; 32] = digest.finalize().into();
        let mut emitted = self
            .emitted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if emitted.contains(&key) {
            return;
        }
        // 在模型收发路径上只尝试入队，队列满时沿官方 best-effort 语义丢弃
        if self.sender.try_send(Delivery { route, event }).is_ok() {
            emitted.put(key, ());
        }
    }
}

impl Delivery {
    pub(super) async fn send(self) -> Result<(), ()> {
        #[derive(Serialize)]
        struct Envelope {
            events: [Event; 1],
        }
        let response = self
            .route
            .client
            .post(&self.route.endpoint)
            .headers(self.route.headers.clone())
            .timeout(REQUEST_TIMEOUT)
            .json(&Envelope {
                events: [self.event],
            })
            .send()
            .await
            .map_err(|_| ())?;
        // 不重放结果不明的 POST，也不把响应正文或认证信息写入日志
        response.status().is_success().then_some(()).ok_or(())
    }
}
