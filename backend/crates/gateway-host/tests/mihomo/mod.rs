//! 验证 Mihomo 订阅解析、候选切换、固定监听与进程回收

use std::{
    net::{Ipv4Addr, TcpListener},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use gateway_host::mihomo::{MihomoConfig, MihomoHost, MihomoNodeConfig, MihomoRuntimeNode};
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

fn runtime(directory: &std::path::Path) -> MihomoHost {
    MihomoHost::new(
        MihomoConfig {
            executable: PathBuf::from(env!("CARGO_BIN_EXE_gateway-host-mihomo-worker")),
        },
        directory.to_path_buf(),
    )
    .expect("Mihomo host")
}

fn node(id: &str, port: u16, kind: &str) -> MihomoRuntimeNode {
    MihomoRuntimeNode {
        id: id.to_owned(),
        port,
        config: json!({
            "name": "subscription supplied name",
            "type": kind,
            "server": "node.example.invalid",
            "port": 443,
            "password": "sensitive-node-secret"
        }),
    }
}

fn port() -> u16 {
    let socket = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    socket.local_addr().unwrap().port()
}

async fn connects(port: u16) -> bool {
    tokio::time::timeout(
        Duration::from_millis(200),
        tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
    )
    .await
    .is_ok_and(|result| result.is_ok())
}

#[tokio::test]
async fn subscription_fetch_negotiates_yaml_and_keeps_only_node_config() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(|request: &wiremock::Request| {
            let body = if request
                .headers
                .get("user-agent")
                .is_some_and(|value| value == "clash.meta")
            {
                "proxies:\n  - name: edge\n    type: socks5\n    server: edge.example\n    port: 1080\n    password: node-secret\nlisteners:\n  - type: mixed\n    port: 10000\nrules:\n  - MATCH,DIRECT\n"
            } else {
                "c3M6Ly9ZV1Z6TFRFeU9DMW5ZMjA2Ym05a1pTMXpaV055WlhSQVpXUm5aUzVsZUdGdGNHeGxPakV3T0RBI2VkZ2U="
            };
            ResponseTemplate::new(200).set_body_string(body)
        })
        .mount(&server)
        .await;

    let runtime = runtime(std::env::temp_dir().as_path());
    let nodes = runtime
        .fetch(&format!("{}/subscription?token=secret", server.uri()))
        .await
        .expect("subscription nodes");

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].name, "edge");
    assert_eq!(nodes[0].kind, "socks5");
    assert_eq!(nodes[0].config["password"], "node-secret");
    let debug = format!("{nodes:?}");
    assert!(!debug.contains("node-secret"));
    assert!(!debug.contains("token=secret"));
}

#[tokio::test]
async fn subscription_fetch_rejects_node_names_over_store_limit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "proxies:\n  - name: {}\n    type: socks5\n    server: edge.example\n    port: 1080\n",
            "n".repeat(101)
        )))
        .mount(&server)
        .await;

    let runtime = runtime(std::env::temp_dir().as_path());
    let error = runtime
        .fetch(&server.uri())
        .await
        .expect_err("node name exceeds Store limit");

    assert_eq!(error.kind(), gateway_host::mihomo::MihomoErrorKind::Invalid);
}

#[tokio::test]
async fn validation_rejects_chain_nodes_and_invalid_binary_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(directory.path());
    let listener_port = port();
    runtime
        .apply(vec![node("previous", listener_port, "socks5")])
        .await
        .unwrap();
    let mut chained = node("chained", port(), "socks5");
    chained.config["dialer-proxy"] = json!("another-node");
    let error = runtime.validate(&[chained]).await.unwrap_err();
    assert_eq!(error.kind(), gateway_host::mihomo::MihomoErrorKind::Invalid);
    assert!(connects(listener_port).await);

    let invalid = node("invalid", port(), "invalid-test");
    let error = runtime
        .validate_nodes(&[MihomoNodeConfig {
            name: "invalid".to_owned(),
            kind: "invalid-test".to_owned(),
            config: invalid.config,
        }])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), gateway_host::mihomo::MihomoErrorKind::Invalid);
    assert!(!format!("{error:?} {error}").contains("sensitive-node-secret"));
    assert!(connects(listener_port).await);
    runtime.shutdown().await;
}

#[tokio::test]
async fn apply_uses_per_node_loopback_listeners_and_shutdown_reaps_process() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(directory.path());
    let sockets = [
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
    ];
    let first_port = sockets[0].local_addr().unwrap().port();
    let second_port = sockets[1].local_addr().unwrap().port();
    drop(sockets);
    runtime
        .apply(vec![
            node("first", first_port, "socks5"),
            node("second", second_port, "vless"),
        ])
        .await
        .unwrap();

    assert!(connects(first_port).await);
    assert!(connects(second_port).await);
    runtime.shutdown().await;
    assert!(!connects(first_port).await);
    assert!(!connects(second_port).await);
}

#[tokio::test]
async fn failed_committed_configuration_stops_previous_listener() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(directory.path());
    let listener_port = port();
    let mut chained = node("chained", listener_port, "socks5");
    chained.config["dialer-proxy"] = json!("another-node");
    for (candidate, expected) in [
        (chained, gateway_host::mihomo::MihomoErrorKind::Invalid),
        (
            node("invalid", listener_port, "invalid-test"),
            gateway_host::mihomo::MihomoErrorKind::Invalid,
        ),
        (
            node("not-ready", listener_port, "socks5"),
            gateway_host::mihomo::MihomoErrorKind::Unavailable,
        ),
    ] {
        runtime
            .apply(vec![node("previous", listener_port, "socks5")])
            .await
            .unwrap();
        assert!(connects(listener_port).await);

        let error = runtime.apply(vec![candidate]).await.unwrap_err();
        assert_eq!(error.kind(), expected);
        assert!(!connects(listener_port).await);
    }
    runtime.shutdown().await;
}

#[tokio::test]
async fn cancelled_reload_reconciles_persisted_nodes_without_restarting_the_child() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = Arc::new(runtime(directory.path()));
    let sockets = [
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap(),
    ];
    let previous_port = sockets[0].local_addr().unwrap().port();
    let candidate_port = sockets[1].local_addr().unwrap().port();
    let delayed_port = sockets[2].local_addr().unwrap().port();
    drop(sockets);
    runtime
        .apply(vec![node("previous", previous_port, "socks5")])
        .await
        .unwrap();
    let delay = directory.path().join("mihomo/delay-start");
    std::fs::write(&delay, []).unwrap();
    let nodes = vec![
        node("candidate", candidate_port, "socks5"),
        node("delayed", delayed_port, "socks5"),
    ];
    let applying = {
        let runtime = Arc::clone(&runtime);
        let nodes = nodes.clone();
        tokio::spawn(async move { runtime.apply(nodes).await })
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while !connects(candidate_port).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("candidate reaches partial listener readiness");
    assert!(!connects(previous_port).await);
    assert!(!connects(delayed_port).await);

    applying.abort();
    assert!(applying.await.unwrap_err().is_cancelled());
    // 已交给子进程的重载可能继续完成，取消后必须重新确认持久快照，不能按旧确认状态跳过
    assert!(connects(candidate_port).await);
    assert!(!connects(previous_port).await);
    std::fs::remove_file(delay).unwrap();
    runtime.apply(nodes).await.unwrap();
    assert!(connects(candidate_port).await);
    assert!(connects(delayed_port).await);
    assert!(!connects(previous_port).await);
    runtime.shutdown().await;
}

#[tokio::test]
async fn reload_preserves_open_tunnels_during_node_changes_and_failures() {
    let directory = tempfile::tempdir().unwrap();
    let executable = std::env::var_os("CPR_TEST_MIHOMO_EXECUTABLE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_gateway-host-mihomo-worker")));
    let runtime =
        MihomoHost::new(MihomoConfig { executable }, directory.path().to_path_buf()).unwrap();
    let upstream = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let upstream_port = upstream.local_addr().unwrap().port();
    let upstream_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = upstream.accept().await.unwrap();
            tokio::spawn(async move {
                let mut header = Vec::new();
                loop {
                    let byte = stream.read_u8().await.unwrap();
                    header.push(byte);
                    if header.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                assert!(header.starts_with(b"CONNECT "));
                stream
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .unwrap();
                let (mut reader, mut writer) = stream.split();
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
            });
        }
    });
    let stable = MihomoRuntimeNode {
        id: "stable".to_owned(),
        port: port(),
        config: json!({
            "type": "http", "server": "127.0.0.1", "port": upstream_port,
        }),
    };
    runtime.apply(vec![stable.clone()]).await.unwrap();
    let mut tunnel = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, stable.port))
        .await
        .unwrap();
    tunnel
        .write_all(
            b"CONNECT echo.example.invalid:443 HTTP/1.1\r\nHost: echo.example.invalid:443\r\n\r\n",
        )
        .await
        .unwrap();
    let mut header = Vec::new();
    loop {
        let byte = tokio::time::timeout(Duration::from_secs(3), tunnel.read_u8())
            .await
            .unwrap()
            .unwrap();
        header.push(byte);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    assert!(header.starts_with(b"HTTP/1.1 200"));
    let added = MihomoRuntimeNode {
        id: "added".to_owned(),
        port: port(),
        config: stable.config.clone(),
    };
    runtime
        .apply(vec![added.clone(), stable.clone()])
        .await
        .unwrap();
    assert!(connects(added.port).await);
    tunnel.write_all(b"after-add").await.unwrap();
    let mut echoed = [0; 9];
    tokio::time::timeout(Duration::from_secs(3), tunnel.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"after-add");

    let mut invalid = added.clone();
    invalid.config["dialer-proxy"] = json!("unsupported");
    assert!(runtime.apply(vec![stable.clone(), invalid]).await.is_err());
    assert!(!connects(added.port).await);
    tunnel.write_all(b"after-err").await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), tunnel.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"after-err");

    runtime.apply(vec![stable]).await.unwrap();
    tunnel.write_all(b"after-del").await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), tunnel.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"after-del");
    drop(tunnel);
    runtime.shutdown().await;
    upstream_task.abort();
    let _ = upstream_task.await;
}

#[tokio::test]
async fn same_configuration_restarts_after_unexpected_child_exit() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(directory.path());
    let listener_port = port();
    let node = node("stable", listener_port, "socks5");
    runtime.apply(vec![node.clone()]).await.unwrap();
    assert!(connects(listener_port).await);
    let exit = directory.path().join("mihomo/exit");
    std::fs::write(&exit, []).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while connects(listener_port).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("simulated Mihomo child exits");
    std::fs::remove_file(exit).unwrap();
    runtime.apply(vec![node]).await.unwrap();
    assert!(connects(listener_port).await);
    runtime.shutdown().await;
}

#[test]
fn mihomo_configuration_defaults_to_path_lookup_and_resolves_explicit_paths() {
    let mut config = MihomoConfig::default();
    config
        .resolve_and_validate(std::path::Path::new("/srv/cpr/deploy"))
        .unwrap();
    assert_eq!(config.executable, PathBuf::from("mihomo"));

    let mut config = MihomoConfig {
        executable: PathBuf::from("bin/mihomo"),
    };
    config
        .resolve_and_validate(std::path::Path::new("/srv/cpr/deploy"))
        .unwrap();
    assert_eq!(
        config.executable,
        PathBuf::from("/srv/cpr/deploy/bin/mihomo")
    );
}

#[tokio::test]
async fn validate_nodes_builds_runtime_candidates_without_changing_source_names() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(directory.path());
    let source = MihomoNodeConfig {
        name: "subscription node".to_owned(),
        kind: "socks5".to_owned(),
        config: json!({
            "name": "subscription node",
            "type": "socks5",
            "server": "node.example.invalid",
            "port": 443,
            "password": "source-secret"
        }),
    };
    runtime
        .validate_nodes(std::slice::from_ref(&source))
        .await
        .unwrap();
    assert_eq!(source.name, "subscription node");
    assert_eq!(source.config["password"], "source-secret");
}

#[tokio::test]
async fn runtime_validation_allows_nodes_from_multiple_subscriptions() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(directory.path());
    let nodes = (0..1_001)
        .map(|index| {
            node(
                &format!("node-{index}"),
                u16::try_from(index + 1).unwrap(),
                "socks5",
            )
        })
        .collect::<Vec<_>>();

    runtime.validate(&nodes).await.unwrap();
}

#[tokio::test]
async fn dropping_host_kills_its_managed_child() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = runtime(directory.path());
    let listener_port = port();
    runtime
        .apply(vec![node("drop", listener_port, "socks5")])
        .await
        .unwrap();
    assert!(connects(listener_port).await);
    drop(runtime);
    tokio::time::timeout(Duration::from_secs(2), async {
        while connects(listener_port).await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("dropping Mihomo host should close the listener");
}
