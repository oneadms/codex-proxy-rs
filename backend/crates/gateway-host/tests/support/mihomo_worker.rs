//! 模拟 Mihomo 校验、受鉴权的配置重载与可持续读写的节点连接

use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

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
    let nodes = Arc::new(Mutex::new(HashMap::new()));
    apply(&configuration, &directory, &mut nodes.lock().unwrap());
    let controller =
        TcpListener::bind(configuration["external-controller"].as_str().unwrap()).unwrap();
    controller.set_nonblocking(true).unwrap();
    let secret = configuration["secret"].as_str().unwrap().to_owned();
    while !directory.join("exit").exists() {
        if let Ok((stream, _)) = controller.accept() {
            stream.set_nonblocking(false).unwrap();
            let nodes = Arc::clone(&nodes);
            let directory = directory.clone();
            let secret = secret.clone();
            thread::spawn(move || handle_control(stream, &secret, &directory, &nodes));
        }
        thread::sleep(Duration::from_millis(5));
    }
}

struct Listener {
    config: Value,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

fn apply(configuration: &Value, directory: &Path, nodes: &mut HashMap<String, Listener>) {
    let listeners = configuration["listeners"].as_array().unwrap();
    nodes.retain(|name, current| {
        listeners
            .iter()
            .any(|listener| listener["name"] == *name && current.config == *listener)
    });
    for listener in listeners {
        let name = listener["name"].as_str().unwrap();
        if nodes.contains_key(name) || listener["proxy"] == "cpr-node-not-ready" {
            continue;
        }
        if listener["proxy"] == "cpr-node-delayed" {
            while directory.join("delay-start").exists() {
                thread::sleep(Duration::from_millis(20));
            }
        }
        let port = listener["port"].as_u64().unwrap() as u16;
        let socket = TcpListener::bind(("127.0.0.1", port)).expect("bind Mihomo listener");
        socket.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                if let Ok((mut stream, _)) = socket.accept() {
                    stream.set_nonblocking(false).unwrap();
                    thread::spawn(move || {
                        let mut buffer = [0; 1024];
                        let mut first = true;
                        while let Ok(length) = stream.read(&mut buffer) {
                            if first && buffer[..length].starts_with(b"CONNECT ") {
                                first = false;
                                if stream
                                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                                    .is_err()
                                {
                                    break;
                                }
                                continue;
                            }
                            first = false;
                            if length == 0 || stream.write_all(&buffer[..length]).is_err() {
                                break;
                            }
                        }
                    });
                }
                thread::sleep(Duration::from_millis(5));
            }
        });
        nodes.insert(
            name.to_owned(),
            Listener {
                config: listener.clone(),
                stop,
                thread: Some(thread),
            },
        );
    }
}

fn handle_control(
    mut stream: TcpStream,
    secret: &str,
    directory: &Path,
    nodes: &Mutex<HashMap<String, Listener>>,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut input = Vec::new();
    let mut buffer = [0; 4096];
    let (header_end, length) = loop {
        let Ok(length) = stream.read(&mut buffer) else {
            return;
        };
        if length == 0 {
            return;
        }
        input.extend_from_slice(&buffer[..length]);
        if let Some(end) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&input[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            break (end + 4, length);
        }
    };
    let headers = String::from_utf8_lossy(&input[..header_end]).into_owned();
    let authenticated = headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("authorization") && value.trim() == format!("Bearer {secret}")
        })
    });
    let status = if !authenticated {
        "401 Unauthorized"
    } else if headers.starts_with("GET /version ") {
        "200 OK"
    } else if headers.starts_with("PUT /configs?force=false ") {
        while input.len() < header_end + length {
            let Ok(length) = stream.read(&mut buffer) else {
                return;
            };
            if length == 0 {
                return;
            }
            input.extend_from_slice(&buffer[..length]);
        }
        let body: Value = serde_json::from_slice(&input[header_end..header_end + length]).unwrap();
        let configuration: Value = serde_json::from_str(body["payload"].as_str().unwrap()).unwrap();
        apply(&configuration, directory, &mut nodes.lock().unwrap());
        "204 No Content"
    } else {
        "404 Not Found"
    };
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
}
