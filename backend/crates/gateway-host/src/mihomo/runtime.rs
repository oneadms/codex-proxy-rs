//! 专属 Mihomo 子进程的候选校验、固定节点监听、切换与异常退出恢复

use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
    io::Write as _,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use futures::{StreamExt as _, stream};
use serde_json::{Value, json};
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    sync::Mutex,
};

use super::{
    MAX_SUBSCRIPTION_NODES, MihomoConfig, MihomoError, MihomoNodeConfig, MihomoRuntimeNode,
    subscription,
};

const VALIDATION_TIMEOUT: Duration = Duration::from_secs(20);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
// 稳定监听使用的数据库端口池共 20,000 个端口
const MAX_RUNTIME_NODES: usize = 20_000;
static NEXT_CONFIGURATION: AtomicU64 = AtomicU64::new(0);

pub struct MihomoHost {
    config: MihomoConfig,
    directory: PathBuf,
    state: Mutex<Option<RunningProcess>>,
    client: reqwest::Client,
}

impl MihomoHost {
    pub fn new(config: MihomoConfig, runtime_data_dir: PathBuf) -> Result<Self, MihomoError> {
        if config.executable.as_os_str().is_empty() {
            return Err(MihomoError::invalid("请配置 Mihomo 可执行文件"));
        }
        let client = gateway_egress::http_client(None)
            .map_err(|_| MihomoError::unavailable("Mihomo 订阅读取的部署代理配置不合法"))?
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .user_agent("codex-proxy-rs-mihomo")
            .build()
            .map_err(|_| MihomoError::unavailable("无法创建 Mihomo 订阅读取客户端"))?;
        Ok(Self {
            config,
            directory: runtime_data_dir.join("mihomo"),
            state: Mutex::new(None),
            client,
        })
    }

    pub async fn validate(&self, nodes: &[MihomoRuntimeNode]) -> Result<(), MihomoError> {
        let _state = self.state.lock().await;
        self.check_configuration(configuration(nodes)?).await?;
        Ok(())
    }

    pub async fn validate_nodes(&self, nodes: &[MihomoNodeConfig]) -> Result<(), MihomoError> {
        if nodes.is_empty() || nodes.len() > MAX_SUBSCRIPTION_NODES {
            return Err(MihomoError::invalid("Mihomo 订阅节点数量不合法"));
        }
        let ports = available_ports(nodes.len())?;
        let runtime_nodes = nodes
            .iter()
            .zip(ports)
            .enumerate()
            .map(|(index, (node, port))| {
                let mut config = node.config.clone();
                config["type"] = Value::String(node.kind.clone());
                MihomoRuntimeNode {
                    id: format!("validation-{index}"),
                    port,
                    config,
                }
            })
            .collect::<Vec<_>>();
        self.validate(&runtime_nodes).await
    }

    pub async fn apply(&self, mut nodes: Vec<MihomoRuntimeNode>) -> Result<(), MihomoError> {
        nodes.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        let mut state = self.state.lock().await;
        if let Some(process) = state.as_mut()
            && process.nodes == nodes
            && matches!(process.child.try_wait(), Ok(None))
        {
            return Ok(());
        }

        // apply 只接收已提交配置，切换失败或取消时不能保留旧端口到节点的映射
        if let Some(mut process) = state.take() {
            process.stop().await;
        }
        if nodes.is_empty() {
            return Ok(());
        }
        let candidate = self.check_configuration(configuration(&nodes)?).await?;
        *state = Some(self.start(candidate, nodes).await?);
        Ok(())
    }

    pub async fn shutdown(&self) {
        if let Some(mut process) = self.state.lock().await.take() {
            process.stop().await;
        }
    }

    pub async fn fetch(&self, url: &str) -> Result<Vec<super::MihomoNodeConfig>, MihomoError> {
        subscription::fetch(&self.client, url).await
    }

    async fn check_configuration(&self, document: Vec<u8>) -> Result<ConfigFile, MihomoError> {
        let file = ConfigFile::write(&self.directory, &document)?;
        let mut command = self.command(&file.path);
        command.arg("-t");
        let mut child = command
            .spawn()
            .map_err(|_| MihomoError::unavailable("无法启动 Mihomo，请检查可执行文件配置"))?;
        match tokio::time::timeout(VALIDATION_TIMEOUT, child.wait()).await {
            Ok(Ok(status)) if status.success() => Ok(file),
            Ok(Ok(_)) => Err(MihomoError::invalid("Mihomo 无法接受订阅中的节点配置")),
            Ok(Err(_)) => Err(MihomoError::unavailable("Mihomo 配置校验进程异常退出")),
            Err(_) => {
                let _ = child.kill().await;
                Err(MihomoError::unavailable("Mihomo 配置校验超时"))
            }
        }
    }

    fn command(&self, path: &Path) -> Command {
        let mut command = Command::new(&self.config.executable);
        command
            .arg("-d")
            .arg(&self.directory)
            .arg("-f")
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(windows)]
        command.creation_flags(0x0800_0000);
        command
    }

    async fn start(
        &self,
        file: ConfigFile,
        nodes: Vec<MihomoRuntimeNode>,
    ) -> Result<RunningProcess, MihomoError> {
        // 先确认端口可用，避免其他本地服务被误判成当前候选的就绪监听
        let ports = nodes
            .iter()
            .map(|node| TcpListener::bind((Ipv4Addr::LOCALHOST, node.port)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| MihomoError::unavailable("Mihomo 节点监听端口已被占用"))?;
        drop(ports);
        let child = self
            .command(&file.path)
            .spawn()
            .map_err(|_| MihomoError::unavailable("无法启动 Mihomo，请检查可执行文件配置"))?;
        let mut process = RunningProcess {
            child,
            nodes,
            _file: file,
        };
        let ready = tokio::time::timeout(STARTUP_TIMEOUT, process.wait_ready()).await;
        if matches!(ready, Ok(Ok(()))) {
            Ok(process)
        } else {
            process.stop().await;
            Err(MihomoError::unavailable("Mihomo 节点监听未能启动"))
        }
    }
}

impl Drop for MihomoHost {
    fn drop(&mut self) {
        if let Some(process) = self.state.get_mut().as_mut() {
            let _ = process.child.start_kill();
        }
    }
}

struct RunningProcess {
    child: Child,
    nodes: Vec<MihomoRuntimeNode>,
    // 配置文件与进程共存，停止或取消后由 Drop 清理
    _file: ConfigFile,
}

impl RunningProcess {
    async fn stop(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }

    async fn wait_ready(&mut self) -> Result<(), ()> {
        loop {
            if self.child.try_wait().map_err(|_| ())?.is_some() {
                return Err(());
            }
            let ports = self.nodes.iter().map(|node| node.port).collect::<Vec<_>>();
            let all_ready = stream::iter(ports)
                .map(|port| async move {
                    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
                    matches!(
                        tokio::time::timeout(
                            Duration::from_millis(100),
                            TcpStream::connect(address)
                        )
                        .await,
                        Ok(Ok(_))
                    )
                })
                .buffer_unordered(32)
                .all(|ready| async move { ready })
                .await;
            if all_ready && self.child.try_wait().map_err(|_| ())?.is_none() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

struct ConfigFile {
    path: PathBuf,
}

impl ConfigFile {
    fn write(directory: &Path, document: &[u8]) -> Result<Self, MihomoError> {
        fs::create_dir_all(directory)
            .map_err(|_| MihomoError::unavailable("无法创建 Mihomo 运行目录"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
                .map_err(|_| MihomoError::unavailable("无法保护 Mihomo 运行目录"))?;
        }
        let path = directory.join(format!(
            "configuration-{}-{}.json",
            std::process::id(),
            NEXT_CONFIGURATION.fetch_add(1, Ordering::Relaxed)
        ));
        let file = Self { path };
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        options
            .open(&file.path)
            .and_then(|mut output| output.write_all(document))
            .map_err(|_| MihomoError::unavailable("无法保存 Mihomo 运行配置"))?;
        Ok(file)
    }
}

impl Drop for ConfigFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn configuration(nodes: &[MihomoRuntimeNode]) -> Result<Vec<u8>, MihomoError> {
    if nodes.len() > MAX_RUNTIME_NODES {
        return Err(MihomoError::invalid("Mihomo 全局节点数量超过监听端口上限"));
    }
    let mut identifiers = HashSet::with_capacity(nodes.len());
    let mut ports = HashSet::with_capacity(nodes.len());
    let mut proxies = Vec::with_capacity(nodes.len());
    let mut listeners = Vec::with_capacity(nodes.len());
    for node in nodes {
        if node.id.is_empty()
            || node.id.len() > 128
            || node.id.chars().any(char::is_control)
            || node.port == 0
            || !identifiers.insert(&node.id)
            || !ports.insert(node.port)
        {
            return Err(MihomoError::invalid("Mihomo 节点监听配置不合法"));
        }
        subscription::validate_node(&node.config)?;
        let name = format!("cpr-node-{}", node.id);
        let mut config = node.config.clone();
        config["name"] = Value::String(name.clone());
        proxies.push(config);
        listeners.push(json!({
            "name": format!("cpr-listener-{}", node.id),
            "type": "mixed",
            "listen": "127.0.0.1",
            "port": node.port,
            "udp": false,
            "proxy": name,
        }));
    }
    serde_json::to_vec(&json!({
        "allow-lan": false,
        "bind-address": "127.0.0.1",
        "mode": "rule",
        "log-level": "silent",
        "ipv6": true,
        "profile": { "store-selected": false, "store-fake-ip": false },
        "proxies": proxies,
        "listeners": listeners,
        "rules": ["MATCH,REJECT"],
    }))
    .map_err(|_| MihomoError::invalid("Mihomo 节点配置不合法"))
}

fn available_ports(count: usize) -> Result<Vec<u16>, MihomoError> {
    let sockets = (0..count)
        .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| MihomoError::unavailable("无法为 Mihomo 候选配置分配监听端口"))?;
    let ports = sockets
        .iter()
        .map(|socket| {
            socket
                .local_addr()
                .map(|address| address.port())
                .map_err(|_| MihomoError::unavailable("无法读取 Mihomo 候选监听端口"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    drop(sockets);
    Ok(ports)
}
