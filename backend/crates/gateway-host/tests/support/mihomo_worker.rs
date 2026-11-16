//! 模拟 Mihomo 命令行校验及每节点本地监听的测试进程

use std::{net::TcpListener, path::PathBuf, thread, time::Duration};

use serde_json::Value;

fn argument(name: &str) -> String {
    std::env::args()
        .skip(1)
        .collect::<Vec<_>>()
        .windows(2)
        .find(|args| args[0] == name)
        .map(|args| args[1].clone())
        .unwrap_or_else(|| panic!("missing argument: {name}"))
}

fn main() {
    let testing = std::env::args().any(|argument| argument == "-t");
    let configuration: Value =
        serde_json::from_slice(&std::fs::read(argument("-f")).expect("read Mihomo configuration"))
            .expect("parse Mihomo configuration");
    let proxies = configuration["proxies"].as_array().expect("proxies");
    if testing && proxies.iter().any(|proxy| proxy["type"] == "invalid-test") {
        std::process::exit(2);
    }
    if testing {
        return;
    }
    let directory = PathBuf::from(argument("-d"));
    let listeners = configuration["listeners"].as_array().expect("listeners");
    let mut sockets = Vec::with_capacity(listeners.len());
    for listener in listeners {
        if listener["proxy"] == "cpr-node-not-ready" {
            continue;
        }
        if listener["proxy"] == "cpr-node-delayed" {
            while directory.join("delay-start").exists() {
                thread::sleep(Duration::from_millis(20));
            }
        }
        let port = listener["port"].as_u64().expect("listener port") as u16;
        sockets.push(TcpListener::bind(("127.0.0.1", port)).expect("bind Mihomo listener"));
    }
    let _sockets = sockets;
    while !directory.join("exit").exists() {
        thread::sleep(Duration::from_millis(20));
    }
}
