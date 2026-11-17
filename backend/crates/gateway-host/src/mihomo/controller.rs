//! 通过带随机凭据的本机控制接口重载 Mihomo，保留配置未变的监听与连接

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use serde_json::{Value, json};

use super::{MihomoError, MihomoRuntimeNode};

static NEXT_CONTROL_PORT: AtomicU32 = AtomicU32::new(0);

pub(super) struct Controller {
    address: SocketAddr,
    secret: String,
    client: reqwest::Client,
}

impl Controller {
    pub(super) fn new(nodes: &[MihomoRuntimeNode]) -> Result<Self, MihomoError> {
        // 避开持久节点的 20000–39999 端口池，后续新增节点也不会占用控制入口
        let socket = (0..25_536)
            .map(|_| 40_000 + (NEXT_CONTROL_PORT.fetch_add(1, Ordering::Relaxed) % 25_536) as u16)
            .filter(|port| !nodes.iter().any(|node| node.port == *port))
            .find_map(|port| TcpListener::bind((Ipv4Addr::LOCALHOST, port)).ok())
            .ok_or_else(|| MihomoError::unavailable("无法分配 Mihomo 本机控制端口"))?;
        let address = socket
            .local_addr()
            .map_err(|_| MihomoError::unavailable("无法读取 Mihomo 本机控制端口"))?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|_| MihomoError::unavailable("无法创建 Mihomo 本机控制客户端"))?;
        Ok(Self {
            address,
            secret: uuid::Uuid::new_v4().to_string(),
            client,
        })
    }

    pub(super) fn configure(&self, document: &mut Value) {
        document["external-controller"] = json!(self.address.to_string());
        document["secret"] = json!(self.secret);
    }

    pub(super) async fn ready(&self) -> bool {
        self.client
            .get(format!("http://{}/version", self.address))
            .bearer_auth(&self.secret)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
    }

    pub(super) async fn reload(&self, document: &str) -> Result<(), MihomoError> {
        // 禁止 force 重建未变监听，也不调用关闭连接接口；响应正文可能含节点凭据，不能写入错误
        let response = self
            .client
            .put(format!("http://{}/configs?force=false", self.address))
            .bearer_auth(&self.secret)
            .json(&json!({"payload": document}))
            .send()
            .await
            .map_err(|_| MihomoError::unavailable("Mihomo 节点配置重载失败"))?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            return Err(MihomoError::unavailable("Mihomo 未接受节点配置重载"));
        }
        Ok(())
    }
}
