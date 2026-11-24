//! 有界客户端状态，关联重试、目录、工具回传和轮次，不持有认证快照

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use gateway_protocol::openai::{codex_account_session_id, codex_account_thread_id, codex_turn_id};
use lru::LruCache;
use reqwest::header::HeaderMap;
use serde_json::{Map, Value};

use super::{
    catalog::Catalog, local::Tools, metadata::RequestMetadata, record::Recorder, turn::Turn,
};

const IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

pub(super) struct Activity(Mutex<State>);

struct State {
    clients: LruCache<String, Client>,
    processes: LruCache<String, ()>,
}

struct Client {
    last_seen: Instant,
    catalog: Catalog,
    tools: Tools,
    samples: LruCache<String, ()>,
    events: LruCache<String, ()>,
    turns: LruCache<String, Arc<Mutex<Turn>>>,
    prewarm: Option<(Instant, &'static str)>,
}

pub(super) struct RequestObservation {
    owner: Arc<Activity>,
    client_key: String,
    turn: Option<Arc<Mutex<Turn>>>,
    recorder: Recorder,
    started: Instant,
    model: String,
    warmup: bool,
    terminal: Mutex<bool>,
    first_token: Mutex<Option<f64>>,
    guardian: Option<Value>,
    after_prewarm: bool,
}

impl Activity {
    pub(super) fn new() -> Self {
        Self(Mutex::new(State {
            clients: LruCache::new(NonZeroUsize::new(1024).unwrap()),
            processes: LruCache::new(NonZeroUsize::new(4096).unwrap()),
        }))
    }

    pub(super) fn request(
        self: &Arc<Self>,
        mut recorder: Recorder,
        metadata: &RequestMetadata<'_>,
        headers: &HeaderMap,
        account: &str,
        request_id: &str,
    ) -> RequestObservation {
        let identity: Map<String, Value> = [
            ("session_id", "session-id"),
            ("thread_id", "thread-id"),
            ("turn_metadata", "x-codex-turn-metadata"),
        ]
        .into_iter()
        .filter_map(|(field, header)| {
            Some((
                field.to_owned(),
                Value::String(headers.get(header)?.to_str().ok()?.to_owned()),
            ))
        })
        .collect();
        let thread_id = codex_account_thread_id(metadata.body, &identity);
        let turn_id = codex_turn_id(metadata.body, &identity);
        let session = codex_account_session_id(metadata.body, &identity);
        let originator = recorder
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "originator")
            .map(|attribute| attribute.value.to_string())
            .unwrap_or_default();
        let version = recorder
            .attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == "app.version")
            .map(|attribute| attribute.value.to_string())
            .unwrap_or_default();
        let process_key = format!(
            "{account}\0{originator}\0{version}\0{}",
            metadata
                .text("process_id", "processId")
                .or(session.as_deref())
                .or(thread_id.as_deref())
                .unwrap_or(request_id)
        );
        let client_key = format!(
            "{process_key}\0{}",
            thread_id.as_deref().unwrap_or(request_id)
        );
        let warmup = metadata.body.get("generate").and_then(Value::as_bool) == Some(false);
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process_new = !state.processes.contains(&process_key);
        if process_new {
            state.processes.put(process_key, ());
        }
        let thread_new = !state.clients.contains(&client_key);
        if thread_new {
            let client = Client {
                last_seen: Instant::now(),
                catalog: Catalog::default(),
                tools: Tools::new(),
                samples: LruCache::new(NonZeroUsize::new(256).unwrap()),
                events: LruCache::new(NonZeroUsize::new(1024).unwrap()),
                turns: LruCache::new(NonZeroUsize::new(64).unwrap()),
                prewarm: None,
            };
            if let Some((_, old)) = state.clients.push(client_key.clone(), client) {
                retire(old);
            }
        }
        let client = state.clients.get_mut(&client_key).unwrap();
        client.last_seen = Instant::now();
        let sampling_id = metadata
            .text("sampling_request_id", "samplingRequestId")
            .unwrap_or(request_id);
        if client.samples.put(sampling_id.to_owned(), ()).is_none() {
            recorder.excluded =
                super::facts::import(&recorder, metadata, sampling_id, &mut client.events);
            if process_new {
                super::local::process_start(&recorder, metadata);
            }
            if thread_new {
                super::local::thread_start(&recorder, metadata);
            }
            client.catalog.observe(&recorder, metadata);
            super::local::request_facts(&recorder, metadata);
            client.tools.input(&recorder, metadata);
        }
        let mut after_prewarm = false;
        let turn = if warmup {
            client.prewarm = Some((Instant::now(), "running"));
            None
        } else {
            if let Some((prewarm, status)) = client.prewarm.take() {
                after_prewarm = status == "ready";
                recorder.sample(
                    "codex.startup_prewarm.age_at_first_turn_ms",
                    prewarm.elapsed().as_secs_f64() * 1000.,
                    &[(
                        "status",
                        if after_prewarm {
                            "consumed"
                        } else if status == "running" {
                            "failed"
                        } else {
                            status
                        },
                    )],
                );
            }
            let turn_key = turn_id.as_deref().unwrap_or(request_id).to_owned();
            if !client.turns.contains(&turn_key) {
                let subagent = matches!(
                    metadata.text("subagent_source", "subagentSource"),
                    Some("thread_spawn" | "spawn" | "multi_agent")
                );
                let subagent_kind = subagent.then_some(if client.turns.is_empty() {
                    "initial"
                } else {
                    "follow_up"
                });
                let turn = Arc::new(Mutex::new(Turn::new(
                    recorder.clone(),
                    metadata,
                    thread_id,
                    turn_id,
                    subagent_kind,
                )));
                if let Some((_, previous)) = client.turns.push(turn_key.clone(), turn) {
                    retire_turn(&previous);
                }
            }
            let turn = Arc::clone(client.turns.get(&turn_key).unwrap());
            {
                let mut state = turn
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.inflight += 1;
                state.update_facts(metadata);
            }
            Some(turn)
        };
        let guardian = metadata
            .fact("guardian")
            .filter(|value| value.is_object())
            .map(super::local::guardian_facts)
            .or_else(|| {
                (metadata.text("subagent_source", "subagentSource") == Some("guardian"))
                    .then_some(serde_json::json!({}))
            });
        RequestObservation {
            owner: Arc::clone(self),
            client_key,
            turn,
            recorder,
            started: Instant::now(),
            model: metadata
                .body
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            warmup,
            terminal: Mutex::new(false),
            first_token: Mutex::new(None),
            guardian,
            after_prewarm,
        }
    }

    pub(super) fn expire(&self, shutdown: bool) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let keys: Vec<_> = state
            .clients
            .iter()
            .filter(|(_, client)| {
                shutdown
                    || (client.last_seen.elapsed() >= IDLE_TIMEOUT
                        && client.turns.iter().all(|(_, turn)| {
                            turn.lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .inflight
                                == 0
                        }))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            if let Some(client) = state.clients.pop(&key) {
                retire(client);
            }
        }
        if shutdown {
            state.processes.clear();
        }
    }
}

impl RequestObservation {
    pub(super) fn after_prewarm(&self) -> bool {
        self.after_prewarm
    }

    pub(super) fn observe(&self, value: &Value) {
        if super::turn::output_timing(value).0 {
            let mut first = self
                .first_token
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if first.is_none() {
                *first = Some(self.started.elapsed().as_secs_f64() * 1000.);
            }
        }
        if let Some(turn) = &self.turn {
            turn.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .output(value);
        }
        if value["type"] != "response.completed" {
            return;
        }
        let mut terminal = self
            .terminal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *terminal {
            return;
        }
        *terminal = true;
        super::responses::reasoning_replay(&self.recorder, &value["response"]);
        if let Some(guardian) = &self.guardian {
            super::local::guardian(
                &self.recorder,
                guardian,
                &value["response"],
                self.started.elapsed().as_secs_f64() * 1000.,
                *self
                    .first_token
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
        if self.warmup {
            self.prewarm_status("ready");
            self.recorder.sample(
                "codex.startup_prewarm.duration_ms",
                self.started.elapsed().as_secs_f64() * 1000.,
                &[("status", "ready")],
            );
            return;
        }
        if let Some(turn) = &self.turn {
            let accepted = turn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .response(value, self.started.elapsed(), &self.model);
            if accepted {
                let mut state = self
                    .owner
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(client) = state.clients.get_mut(&self.client_key) {
                    client.last_seen = Instant::now();
                    client.tools.output(&value["response"], turn);
                }
            }
        }
    }

    fn prewarm_status(&self, status: &'static str) {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(client) = state.clients.get_mut(&self.client_key)
            && let Some((_, current)) = &mut client.prewarm
        {
            *current = status;
        }
    }
}

impl Drop for RequestObservation {
    fn drop(&mut self) {
        if let Some(turn) = &self.turn {
            let mut turn = turn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            turn.inflight = turn.inflight.saturating_sub(1);
            turn.last_seen = Instant::now();
            if turn.inflight == 0 && (turn.retired || (turn.final_response && !turn.awaiting_tools))
            {
                let interrupted = turn.retired;
                turn.finish(interrupted);
            }
        } else if self.warmup
            && !*self
                .terminal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            self.prewarm_status("cancelled");
            self.recorder.sample(
                "codex.startup_prewarm.duration_ms",
                self.started.elapsed().as_secs_f64() * 1000.,
                &[("status", "cancelled")],
            );
        }
    }
}

fn retire(client: Client) {
    for (_, turn) in client.turns {
        retire_turn(&turn);
    }
}

fn retire_turn(turn: &Mutex<Turn>) {
    let mut turn = turn
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    turn.retired = true;
    if turn.inflight == 0 {
        turn.finish(true);
    }
}
