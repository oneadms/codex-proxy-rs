//! Mihomo 持久配置恢复、取消重放和写入串行化的用例测试

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::future::BoxFuture;
use gateway_admin::{
    AdminBundle,
    model::{AdminError, AdminErrorKind, MutationContext, Revision, mihomo::*},
    ports::{
        mihomo::{MihomoRuntime, MihomoStore, MihomoSubscriptionFetcher},
        store::AdminStoreResult,
    },
};
use gateway_core::{
    lifecycle::CancellationToken,
    routing::ConfigRevision,
    runtime::SnapshotControl,
    task::{ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerRunnable},
};
use secrecy::SecretString;
use serde_json::json;
use tokio::sync::{Notify, oneshot};

use super::{AdminHarness, unavailable};

struct SnapshotStore(Mutex<MihomoRuntimeSnapshot>);

#[async_trait]
impl MihomoStore for SnapshotStore {
    async fn subscriptions(&self) -> AdminStoreResult<Vec<MihomoSubscription>> {
        Err(unavailable("mihomo subscriptions"))
    }

    async fn subscription(&self, _: &str) -> AdminStoreResult<MihomoSubscription> {
        Err(unavailable("mihomo subscription"))
    }

    async fn create(
        &self,
        command: NewMihomoSubscription,
        nodes: Vec<MihomoNodeConfig>,
        _: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult> {
        let mut persisted = self.0.lock().unwrap();
        persisted.config_revision = Revision::new(persisted.config_revision.get() + 1).unwrap();
        persisted.nodes = nodes
            .into_iter()
            .map(|node| MihomoRuntimeNode {
                id: node.name,
                port: 20_010,
                config: node.config,
            })
            .collect();
        Ok(MihomoRefreshResult {
            subscription: MihomoSubscription {
                id: "subscription-test".to_owned(),
                name: command.name,
                url: command.url,
                revision: Revision::new(1).unwrap(),
                node_count: persisted.nodes.len() as u64,
                updated_at: chrono::Utc::now(),
            },
            config_revision: persisted.config_revision,
        })
    }

    async fn update(
        &self,
        _: UpdateMihomoSubscription,
        _: Option<Vec<MihomoNodeConfig>>,
        _: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult> {
        Err(unavailable("mihomo update"))
    }

    async fn sync_nodes(
        &self,
        _: &str,
        _: Revision,
        _: Vec<MihomoNodeConfig>,
        _: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult> {
        Err(unavailable("mihomo sync"))
    }

    async fn delete(
        &self,
        _: &str,
        _: Revision,
        _: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        Err(unavailable("mihomo delete"))
    }

    async fn nodes(&self, _: MihomoNodeQuery) -> AdminStoreResult<MihomoNodePage> {
        Err(unavailable("mihomo nodes"))
    }

    async fn runtime_snapshot(&self) -> AdminStoreResult<MihomoRuntimeSnapshot> {
        Ok(self.0.lock().unwrap().clone())
    }
}

#[derive(Default)]
struct TestRuntime {
    fail_next: AtomicBool,
    applications: Mutex<Vec<Vec<MihomoRuntimeNode>>>,
    blocked_apply: Mutex<Option<oneshot::Receiver<()>>>,
    apply_started: Notify,
}

impl TestRuntime {
    fn block_next_apply(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.blocked_apply.lock().unwrap() = Some(wait);
        release
    }

    async fn wait_blocked(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.apply_started.notified())
            .await
            .expect("runtime application started");
    }

    fn applications(&self) -> Vec<Vec<MihomoRuntimeNode>> {
        self.applications.lock().unwrap().clone()
    }
}

#[async_trait]
impl MihomoRuntime for TestRuntime {
    async fn validate(&self, _: &[MihomoNodeConfig]) -> Result<(), AdminError> {
        Ok(())
    }

    async fn apply(&self, nodes: Vec<MihomoRuntimeNode>) -> Result<(), AdminError> {
        let blocked = self.blocked_apply.lock().unwrap().take();
        if let Some(wait) = blocked {
            self.apply_started.notify_one();
            let _ = wait.await;
        }
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(AdminError::unavailable("runtime apply failed"));
        }
        self.applications.lock().unwrap().push(nodes);
        Ok(())
    }

    async fn shutdown(&self) {}
}

struct SubscriptionFetcher;

#[async_trait]
impl MihomoSubscriptionFetcher for SubscriptionFetcher {
    async fn fetch(&self, _: &str) -> Result<Vec<MihomoNodeConfig>, AdminError> {
        Ok(vec![MihomoNodeConfig {
            name: "node-new".to_owned(),
            kind: "ss".to_owned(),
            config: json!({"server": "new.example.invalid", "port": 443}),
        }])
    }
}

struct CountingSubscriptionFetcher(Arc<AtomicUsize>);

#[async_trait]
impl MihomoSubscriptionFetcher for CountingSubscriptionFetcher {
    async fn fetch(&self, _: &str) -> Result<Vec<MihomoNodeConfig>, AdminError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        SubscriptionFetcher.fetch("").await
    }
}

#[derive(Default)]
struct Publications {
    revisions: Mutex<Vec<u64>>,
    blocked_publish: Mutex<Option<oneshot::Receiver<()>>>,
    publish_started: Notify,
}

impl Publications {
    fn block_next_publish(&self) -> oneshot::Sender<()> {
        let (release, wait) = oneshot::channel();
        *self.blocked_publish.lock().unwrap() = Some(wait);
        release
    }
}

impl SnapshotControl for Publications {
    fn publish_committed(&self, revision: ConfigRevision) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let blocked = self.blocked_publish.lock().unwrap().take();
            if let Some(wait) = blocked {
                self.publish_started.notify_one();
                let _ = wait.await;
            }
            self.revisions.lock().unwrap().push(revision.get());
        })
    }
}

fn persisted(revision: u64, id: &str) -> MihomoRuntimeSnapshot {
    MihomoRuntimeSnapshot {
        config_revision: Revision::new(revision).unwrap(),
        nodes: vec![MihomoRuntimeNode {
            id: id.to_owned(),
            port: 20_010,
            config: json!({"server": format!("{id}.example.invalid"), "port": 443}),
        }],
    }
}

fn command() -> NewMihomoSubscription {
    NewMihomoSubscription {
        name: "Test subscription".to_owned(),
        url: SecretString::from("https://subscription.example.invalid/nodes".to_owned()),
    }
}

async fn build(
    store: Arc<SnapshotStore>,
    runtime: Arc<TestRuntime>,
    publications: Arc<Publications>,
) -> AdminBundle {
    build_with_fetcher(store, runtime, publications, Arc::new(SubscriptionFetcher)).await
}

async fn build_with_fetcher(
    store: Arc<SnapshotStore>,
    runtime: Arc<TestRuntime>,
    publications: Arc<Publications>,
    fetcher: Arc<dyn MihomoSubscriptionFetcher>,
) -> AdminBundle {
    let bundle = AdminHarness::new()
        .mihomo(store, runtime.clone(), fetcher)
        .snapshot(publications.clone())
        .build_bundle()
        .await;
    runtime.applications.lock().unwrap().clear();
    publications.revisions.lock().unwrap().clear();
    bundle
}

#[tokio::test]
async fn subscription_names_are_trimmed_and_invalid_names_do_not_fetch() {
    let store = Arc::new(SnapshotStore(Mutex::new(persisted(17, "node-original"))));
    let runtime = Arc::new(TestRuntime::default());
    let publications = Arc::new(Publications::default());
    let fetch_calls = Arc::new(AtomicUsize::new(0));
    let fetcher: Arc<dyn MihomoSubscriptionFetcher> =
        Arc::new(CountingSubscriptionFetcher(fetch_calls.clone()));
    let bundle = build_with_fetcher(store, runtime, publications, fetcher).await;
    let services = bundle.services();
    let service = services.mihomo();
    let context = crate::backup::support::system_context();
    let valid_name = format!("  {}  ", "订".repeat(100));
    let created = service
        .create(
            NewMihomoSubscription {
                name: valid_name,
                url: command().url,
            },
            &context,
        )
        .await
        .expect("100 Unicode characters are valid");
    assert_eq!(created.subscription.name, "订".repeat(100));
    assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);

    for name in [
        String::new(),
        " \t ".to_owned(),
        "订".repeat(101),
        "bad\nname".to_owned(),
    ] {
        let error = service
            .create(
                NewMihomoSubscription {
                    name,
                    url: command().url,
                },
                &context,
            )
            .await
            .expect_err("invalid subscription name is rejected");
        assert_eq!(error.kind(), AdminErrorKind::Invalid);
        assert_eq!(
            error.message(),
            "订阅名称必须为 1 至 100 个字符且不能包含控制字符"
        );
        assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);
    }

    let error = service
        .update(
            UpdateMihomoSubscription {
                id: "subscription-test".to_owned(),
                revision: Revision::new(1).unwrap(),
                name: Some("invalid\tname".to_owned()),
                url: Some(command().url),
            },
            &context,
        )
        .await
        .expect_err("invalid update name is rejected before fetching its URL");
    assert_eq!(error.kind(), AdminErrorKind::Invalid);
    assert_eq!(fetch_calls.load(Ordering::SeqCst), 1);
}

fn runtime_worker(bundle: &mut AdminBundle) -> (WorkerId, Arc<dyn ScheduledTask>) {
    let registration = bundle
        .take_worker_contributions()
        .into_iter()
        .find_map(|contribution| match contribution {
            WorkerContribution::Registration(registration)
                if registration.id.owner() == "mihomo_runtime" =>
            {
                Some(registration)
            }
            _ => None,
        })
        .expect("Mihomo runtime reconciliation worker");
    let WorkerRunnable::Scheduled { task, lease, .. } = registration.runnable else {
        panic!("Mihomo worker is scheduled")
    };
    assert!(lease.is_none(), "every host owns its local Mihomo process");
    (registration.id, Arc::from(task))
}

fn context(id: WorkerId, cancellation: CancellationToken) -> WorkerCycleContext {
    WorkerCycleContext::new(id, None, cancellation)
}

#[tokio::test]
async fn worker_reapplies_committed_snapshot_after_runtime_failure() {
    let store = Arc::new(SnapshotStore(Mutex::new(persisted(17, "node-original"))));
    let runtime = Arc::new(TestRuntime::default());
    let publications = Arc::new(Publications::default());
    let mut bundle = build(store.clone(), runtime.clone(), publications.clone()).await;
    let (id, task) = runtime_worker(&mut bundle);

    runtime.fail_next.store(true, Ordering::SeqCst);
    let error = bundle
        .services()
        .mihomo()
        .create(command(), &crate::backup::support::system_context())
        .await
        .expect_err("commit survives a failed runtime application");
    assert_eq!(error.kind(), AdminErrorKind::Unavailable);
    let committed = store.0.lock().unwrap().clone();
    assert_eq!(committed.config_revision.get(), 18);
    assert!(publications.revisions.lock().unwrap().is_empty());

    task.run_cycle(context(id, CancellationToken::new()))
        .await
        .expect("next cycle reapplies committed snapshot");
    assert_eq!(runtime.applications(), [committed.nodes]);
    assert_eq!(publications.revisions.lock().unwrap().as_slice(), [18]);
}

#[tokio::test]
async fn cancelled_apply_releases_the_lock_and_replays_the_persisted_snapshot() {
    for cancel_worker in [false, true] {
        let store = Arc::new(SnapshotStore(Mutex::new(persisted(17, "node-original"))));
        let runtime = Arc::new(TestRuntime::default());
        let publications = Arc::new(Publications::default());
        let mut bundle = build(store.clone(), runtime.clone(), publications.clone()).await;
        let (id, task) = runtime_worker(&mut bundle);
        let release = runtime.block_next_apply();
        let cancellation = CancellationToken::new();

        let operation = if cancel_worker {
            *store.0.lock().unwrap() = persisted(18, "node-updated");
            let cycle_context = context(id.clone(), cancellation.clone());
            let cycle_task = task.clone();
            tokio::spawn(async move {
                cycle_task.run_cycle(cycle_context).await.unwrap();
            })
        } else {
            let services = bundle.services();
            tokio::spawn(async move {
                services
                    .mihomo()
                    .create(command(), &crate::backup::support::system_context())
                    .await
                    .unwrap();
            })
        };
        runtime.wait_blocked().await;
        if cancel_worker {
            cancellation.cancel();
            tokio::time::timeout(Duration::from_secs(5), operation)
                .await
                .expect("worker cancellation completes")
                .expect("worker exits cleanly");
        } else {
            operation.abort();
            assert!(operation.await.unwrap_err().is_cancelled());
        }
        assert!(release.send(()).is_err());
        assert!(publications.revisions.lock().unwrap().is_empty());

        tokio::time::timeout(
            Duration::from_secs(5),
            task.run_cycle(context(id, CancellationToken::new())),
        )
        .await
        .expect("next cycle acquires the released mutation lock")
        .expect("next cycle restores persisted nodes");
        assert_eq!(
            runtime.applications(),
            [store.0.lock().unwrap().nodes.clone()]
        );
        assert_eq!(publications.revisions.lock().unwrap().as_slice(), [18]);
    }
}

#[tokio::test]
async fn reconciliation_finishes_before_a_new_mutation_can_commit() {
    let store = Arc::new(SnapshotStore(Mutex::new(persisted(17, "node-original"))));
    let runtime = Arc::new(TestRuntime::default());
    let publications = Arc::new(Publications::default());
    let mut bundle = build(store.clone(), runtime.clone(), publications.clone()).await;
    let (id, task) = runtime_worker(&mut bundle);
    let previous = persisted(18, "node-previous");
    *store.0.lock().unwrap() = previous.clone();

    let release = runtime.block_next_apply();
    let cycle =
        tokio::spawn(async move { task.run_cycle(context(id, CancellationToken::new())).await });
    runtime.wait_blocked().await;
    let services = bundle.services();
    let mutation_context = crate::backup::support::system_context();
    let mutation = services.mihomo().create(command(), &mutation_context);
    tokio::pin!(mutation);
    assert!(futures::poll!(mutation.as_mut()).is_pending());
    assert_eq!(store.0.lock().unwrap().config_revision.get(), 18);

    release
        .send(())
        .expect("release preceding runtime application");
    cycle.await.unwrap().expect("preceding reconciliation");
    let result = mutation
        .await
        .expect("mutation proceeds after reconciliation");
    assert_eq!(result.config_revision.get(), 19);
    assert_eq!(
        runtime.applications(),
        [previous.nodes, store.0.lock().unwrap().nodes.clone()]
    );
    assert_eq!(publications.revisions.lock().unwrap().as_slice(), [18, 19]);
}

#[tokio::test]
async fn unchanged_revisions_still_apply_runtime_without_republishing() {
    let initial = persisted(17, "node-original");
    let store = Arc::new(SnapshotStore(Mutex::new(initial.clone())));
    let runtime = Arc::new(TestRuntime::default());
    let publications = Arc::new(Publications::default());
    let mut bundle = AdminHarness::new()
        .mihomo(
            store.clone(),
            runtime.clone(),
            Arc::new(SubscriptionFetcher),
        )
        .snapshot(publications.clone())
        .build_bundle()
        .await;
    let (id, task) = runtime_worker(&mut bundle);
    assert_eq!(
        runtime.applications().as_slice(),
        std::slice::from_ref(&initial.nodes)
    );
    assert!(publications.revisions.lock().unwrap().is_empty());

    task.run_cycle(context(id.clone(), CancellationToken::new()))
        .await
        .expect("unchanged startup revision is reconciled");
    assert!(publications.revisions.lock().unwrap().is_empty());

    let updated = persisted(18, "node-updated");
    *store.0.lock().unwrap() = updated.clone();
    for _ in 0..2 {
        task.run_cycle(context(id.clone(), CancellationToken::new()))
            .await
            .expect("runtime is checked on every cycle");
    }
    assert_eq!(
        runtime.applications(),
        [
            initial.nodes.clone(),
            initial.nodes,
            updated.nodes.clone(),
            updated.nodes
        ]
    );
    assert_eq!(publications.revisions.lock().unwrap().as_slice(), [18]);
}

#[tokio::test]
async fn cancelled_publication_is_retried_before_revision_is_deduplicated() {
    let store = Arc::new(SnapshotStore(Mutex::new(persisted(17, "node-original"))));
    let runtime = Arc::new(TestRuntime::default());
    let publications = Arc::new(Publications::default());
    let mut bundle = build(store.clone(), runtime.clone(), publications.clone()).await;
    let (id, task) = runtime_worker(&mut bundle);
    let updated = persisted(18, "node-updated");
    *store.0.lock().unwrap() = updated.clone();
    let release = publications.block_next_publish();
    let cancellation = CancellationToken::new();
    let cycle_context = context(id.clone(), cancellation.clone());
    let cycle_task = task.clone();
    let cycle = tokio::spawn(async move { cycle_task.run_cycle(cycle_context).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        publications.publish_started.notified(),
    )
    .await
    .expect("publication started after runtime applied nodes");
    assert_eq!(
        runtime.applications().as_slice(),
        std::slice::from_ref(&updated.nodes)
    );

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(5), cycle)
        .await
        .expect("publication cancellation completes")
        .expect("worker cycle joins")
        .expect("cancellation is a clean exit");
    assert!(release.send(()).is_err());
    assert!(publications.revisions.lock().unwrap().is_empty());

    for _ in 0..2 {
        tokio::time::timeout(
            Duration::from_secs(5),
            task.run_cycle(context(id.clone(), CancellationToken::new())),
        )
        .await
        .expect("mutation lock is released after cancelled publication")
        .expect("publication is retried");
    }
    assert_eq!(runtime.applications(), vec![updated.nodes; 3]);
    assert_eq!(publications.revisions.lock().unwrap().as_slice(), [18]);
}
