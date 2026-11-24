//! 用隔离代理响应浏览器通信合同，登录编排测试不依赖浏览器安装

use async_trait::async_trait;
use gateway_admin::ports::javascript::{JavaScriptError, JavaScriptRuntime, JavaScriptSession};
use reqwest::{Client, Method, cookie::Jar, redirect::Policy};
use secrecy::{ExposeSecret as _, SecretString};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use url::Url;

#[derive(Default)]
pub(super) struct RecoveryLeases {
    pub(super) busy: bool,
    pub(super) held: Arc<std::sync::atomic::AtomicBool>,
}

struct RecoveryGuard(Arc<std::sync::atomic::AtomicBool>);

impl Drop for RecoveryGuard {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl gateway_core::provider_ports::ProviderLeaseGuard for RecoveryGuard {
    fn release(
        self: Box<Self>,
    ) -> futures::future::BoxFuture<
        'static,
        Result<(), gateway_core::provider_ports::ProviderStoreError>,
    > {
        Box::pin(async move {
            drop(self);
            Ok(())
        })
    }
}

impl gateway_core::provider_ports::ProviderLeasePort for RecoveryLeases {
    fn load_state<'a>(
        &'a self,
        _: &'a gateway_core::policy::ClientApiKeyId,
        _: &'a gateway_core::routing::ProviderKind,
        _: &'a [gateway_core::account::ProviderAccountId],
        _: gateway_core::provider_ports::ProviderConcurrencyPool,
    ) -> futures::future::BoxFuture<
        'a,
        Result<
            gateway_core::provider_ports::ProviderSchedulingState,
            gateway_core::provider_ports::ProviderStoreError,
        >,
    > {
        Box::pin(async { panic!("protocol recovery does not schedule model requests") })
    }

    fn try_acquire(
        &self,
        request: gateway_core::provider_ports::ProviderLeaseRequest,
    ) -> futures::future::BoxFuture<
        '_,
        Result<
            gateway_core::provider_ports::ProviderLeaseAcquisition,
            gateway_core::provider_ports::ProviderStoreError,
        >,
    > {
        use gateway_core::provider_ports::{ProviderLeaseAcquisition, ProviderLeaseRequest};
        Box::pin(async move {
            if self.busy {
                return Ok(ProviderLeaseAcquisition::Busy { retry_after: None });
            }
            match request {
                ProviderLeaseRequest::RefreshCapacity(_) => {
                    Ok(ProviderLeaseAcquisition::Acquired(Box::new(())))
                }
                ProviderLeaseRequest::Refresh(_) => {
                    if self.held.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        Ok(ProviderLeaseAcquisition::Busy { retry_after: None })
                    } else {
                        Ok(ProviderLeaseAcquisition::Acquired(Box::new(RecoveryGuard(
                            self.held.clone(),
                        ))))
                    }
                }
                ProviderLeaseRequest::Scheduling(_) => {
                    panic!("protocol recovery must use refresh leases")
                }
            }
        })
    }
}

#[derive(Default)]
pub(super) struct FixtureBrowser {
    pub(super) devices: Arc<Mutex<Vec<Value>>>,
    pub(super) closed: Arc<Mutex<usize>>,
}

struct Session {
    client: Option<Client>,
    device: Value,
    next: Value,
    devices: Arc<Mutex<Vec<Value>>>,
    closed: Arc<Mutex<usize>>,
}

#[async_trait]
impl JavaScriptRuntime for FixtureBrowser {
    async fn start(&self, _: &str, _: &str) -> Result<Box<dyn JavaScriptSession>, JavaScriptError> {
        Ok(Box::new(Session {
            client: None,
            device: Value::Null,
            next: Value::Null,
            devices: self.devices.clone(),
            closed: self.closed.clone(),
        }))
    }
}

impl Session {
    async fn request(&self, input: &Value) -> Result<Value, JavaScriptError> {
        let mut target = Url::parse(input["url"].as_str().unwrap()).unwrap();
        let navigation = input["type"] == "navigate";
        for _ in 0..12 {
            if target.scheme() == "http"
                && target.host_str() == Some("127.0.0.1")
                && target.port() == Some(1455)
            {
                return Ok(
                    json!({"type": "response", "url": target.as_str(), "status": 200, "headers": {}, "body": ""}),
                );
            }
            if target.scheme() != "https"
                || !matches!(
                    target.host_str(),
                    Some("chatgpt.com" | "auth.openai.com" | "sentinel.openai.com")
                )
            {
                return Ok(json!({"type": "error", "code": "untrusted_url"}));
            }
            let method = if navigation {
                Method::GET
            } else {
                Method::from_bytes(input["method"].as_str().unwrap().as_bytes()).unwrap()
            };
            let mut request = self
                .client
                .as_ref()
                .unwrap()
                .request(method, target.clone());
            if let Some(headers) = input["headers"].as_object() {
                for (key, value) in headers {
                    request = request.header(key, value.as_str().unwrap());
                }
            }
            if let Some(body) = input["body"].as_str() {
                request = request.body(body.to_owned());
            }
            let response = request
                .send()
                .await
                .map_err(|_| JavaScriptError::Communication)?;
            let status = response.status();
            let headers: serde_json::Map<_, _> = response
                .headers()
                .iter()
                .map(|(name, value)| (name.to_string(), json!(value.to_str().unwrap())))
                .collect();
            if navigation && status.is_redirection() {
                target = target.join(headers["location"].as_str().unwrap()).unwrap();
                continue;
            }
            let body = response
                .text()
                .await
                .map_err(|_| JavaScriptError::Communication)?;
            return Ok(
                json!({"type": "response", "url": target.as_str(), "status": status.as_u16(), "headers": headers, "body": body}),
            );
        }
        Err(JavaScriptError::Communication)
    }
}

#[async_trait]
impl JavaScriptSession for Session {
    async fn send(&mut self, message: &SecretString) -> Result<(), JavaScriptError> {
        let input: Value = serde_json::from_str(message.expose_secret()).unwrap();
        self.next = match input["type"].as_str().unwrap() {
            "init" => {
                let proxy =
                    gateway_core::account::OutboundProxy::parse(input["proxy"].as_str().unwrap())
                        .unwrap();
                self.device = input.get("device").filter(|value| value.is_object()).cloned().unwrap_or_else(|| json!({
                    "id": uuid::Uuid::new_v4().to_string(), "config": {"navigator.platform": "Linux x86_64", "canvas:seed": 123},
                }));
                self.devices.lock().unwrap().push(self.device.clone());
                let jar = Arc::new(Jar::default());
                for origin in ["https://chatgpt.com/", "https://auth.openai.com/"] {
                    jar.add_cookie_str(
                        &format!(
                            "oai-did={}; Path=/; Secure",
                            self.device["id"].as_str().unwrap()
                        ),
                        &Url::parse(origin).unwrap(),
                    );
                }
                self.client = Some(
                    provider_openai::build_reqwest_client_with_custom_ca(
                        gateway_egress::http_client(Some(&proxy))
                            .unwrap()
                            .cookie_provider(jar)
                            .redirect(Policy::none()),
                    )
                    .unwrap(),
                );
                json!({"type": "ready", "device": self.device})
            }
            "navigate" | "request" => self.request(&input).await?,
            "sentinel" => {
                let loader = self.request(&json!({"type":"request", "url":"https://sentinel.openai.com/backend-api/sentinel/sdk.js", "method":"GET"})).await?;
                let sdk = self.request(&json!({"type":"request", "url":"https://sentinel.openai.com/sentinel/synthetic/sdk.js", "method":"GET"})).await?;
                assert!(loader["body"].is_string());
                if sdk["body"]
                    .as_str()
                    .unwrap()
                    .contains("https://example.invalid/blocked")
                {
                    json!({"type":"error", "code":"sentinel_failed"})
                } else {
                    let response = self.request(&json!({"type":"request", "url":"https://sentinel.openai.com/backend-api/sentinel/req", "method":"POST", "body":"{}"})).await?;
                    let challenge: Value =
                        serde_json::from_str(response["body"].as_str().unwrap()).unwrap();
                    if challenge.pointer("/turnstile/required") == Some(&json!(true)) {
                        json!({"type":"error", "code":"sentinel_failed"})
                    } else {
                        let id = self.device["id"].as_str().unwrap();
                        json!({"type":"sentinel", "token":json!({"id":id,"flow":"password_verify","p":"proof","t":null,"c":"challenge"}).to_string(),
                            "sessionObserver": challenge.pointer("/so/required").and_then(Value::as_bool).filter(|required| *required)
                                .map(|_| json!({"id":id,"flow":"password_verify","so":"observer","c":"challenge"}).to_string())})
                    }
                }
            }
            _ => panic!("unexpected browser command"),
        };
        Ok(())
    }

    async fn receive(&mut self) -> Result<SecretString, JavaScriptError> {
        Ok(SecretString::from(self.next.to_string()))
    }

    async fn close(self: Box<Self>) {
        *self.closed.lock().unwrap() += 1;
    }
}
