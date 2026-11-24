//! 官方 analytics 的有界队列、账号认证出口与重复事件隔离

use std::{collections::VecDeque, num::NonZeroUsize, sync::Mutex, time::Duration};

use futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered};
use gateway_core::task::{WorkerDaemonContext, WorkerTaskError};
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
const BATCH_SIZE: usize = 256;
const SEND_CONCURRENCY: usize = 4;
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

struct Batch {
    route: std::sync::Arc<Route>,
    events: Vec<Event>,
}

/// 队列和发送 future 都归同一受监督任务持有，停机时继续消费至排空
pub(super) async fn deliver(
    receiver: &mut mpsc::Receiver<Delivery>,
    context: &WorkerDaemonContext,
) -> Result<(), WorkerTaskError> {
    let mut pending = VecDeque::<Batch>::new();
    let mut sending = FuturesUnordered::new();
    let mut deliveries = Vec::with_capacity(BATCH_SIZE);
    let mut closing = false;
    let mut exhausted = false;
    let mut failed = false;
    loop {
        while sending.len() < SEND_CONCURRENCY
            && let Some(batch) = pending.pop_front()
        {
            sending.push(batch.send().boxed());
        }
        // 接收端 EOF 才确认排空，避免遗漏关闭前已取得入队许可的发送者
        if exhausted && pending.is_empty() && sending.is_empty() {
            return if failed {
                Err(WorkerTaskError::safe("Codex analytics 上报失败"))
            } else {
                Ok(())
            };
        }
        tokio::select! {
            biased;
            _ = context.cancellation().cancelled(), if !closing => {
                receiver.close();
                closing = true;
            }
            Some(result) = sending.next(), if !sending.is_empty() => {
                if result.is_ok() {
                    context.succeeded();
                } else {
                    // 失败批次不重试，也不重启共享指标调度
                    tracing::warn!("Codex analytics 上报失败");
                    failed |= closing;
                }
            }
            count = receiver.recv_many(&mut deliveries, BATCH_SIZE),
                if !exhausted && pending.is_empty() && sending.len() < SEND_CONCURRENCY => {
                if count == 0 {
                    closing = true;
                    exhausted = true;
                    continue;
                }
                for delivery in deliveries.drain(..) {
                    // scope 包含账号出口；认证、画像和产品归因头必须完全相同才能混批
                    if let Some(batch) = pending.iter_mut().find(|batch| {
                        batch.route.scope == delivery.route.scope
                            && batch.route.endpoint == delivery.route.endpoint
                            && batch.route.headers == delivery.route.headers
                    }) {
                        batch.events.push(delivery.event);
                    } else {
                        pending.push_back(Batch {
                            route: delivery.route,
                            events: vec![delivery.event],
                        });
                    }
                }
            }
        }
    }
}

impl Batch {
    async fn send(self) -> Result<(), ()> {
        #[derive(Serialize)]
        struct Envelope {
            events: Vec<Event>,
        }
        let response = self
            .route
            .client
            .post(&self.route.endpoint)
            .headers(self.route.headers.clone())
            .timeout(REQUEST_TIMEOUT)
            .json(&Envelope {
                events: self.events,
            })
            .send()
            .await
            .map_err(|_| ())?;
        // 不重放结果不明的 POST，也不把响应正文或认证信息写入日志
        response.status().is_success().then_some(()).ok_or(())
    }
}
