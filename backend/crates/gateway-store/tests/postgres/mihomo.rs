//! Mihomo Store 的订阅同步、固定出口和端口生命周期测试

use gateway_admin::ports::store::AccountStore as _;
use gateway_admin::{
    model::{
        MutationActor, MutationContext, PageSize,
        accounts::UpdateAccount,
        mihomo::{
            MihomoNodeConfig, MihomoNodeQuery, NewMihomoSubscription, UpdateMihomoSubscription,
        },
        proxies::{ProxyLocationDetection, ProxyTestResult, UpdateProxy},
    },
    ports::{mihomo::MihomoStore, proxy::ProxyStore, store::AdminStoreErrorKind},
};
use gateway_core::account::{
    OutboundProxy, ProviderAccountId, ProviderAccountStore as _, RequestLocation,
};
use gateway_store::postgres::{
    PgMihomoRepository, PgProviderAccountRepository, PgProxyRepository,
    ProviderAccountRepository as _,
};
use secrecy::SecretString;

use super::{TestDatabase, provider_accounts::account};

#[tokio::test]
async fn refresh_keeps_node_identity_and_accounts_fail_closed() {
    let Some(database) = TestDatabase::create("mihomo_refresh").await else {
        return;
    };
    let mihomo = PgMihomoRepository::new(database.pool.clone());
    let proxies = PgProxyRepository::new(database.pool.clone());
    let accounts = PgProviderAccountRepository::new(database.pool.clone());
    let mut first = mihomo
        .create(
            NewMihomoSubscription {
                name: "Local subscription".to_owned(),
                url: SecretString::from("https://subscription.invalid/token".to_owned()),
            },
            vec![node("Node A", "a1"), node("Node B", "b1")],
            &context(),
        )
        .await
        .unwrap();
    let metadata_update = mihomo
        .update(
            UpdateMihomoSubscription {
                id: first.subscription.id.clone(),
                revision: first.subscription.revision,
                name: Some("Renamed subscription".to_owned()),
                url: None,
            },
            None,
            &context(),
        )
        .await
        .unwrap();
    assert_eq!(metadata_update.config_revision, first.config_revision);
    first = metadata_update;
    let first_page = mihomo.nodes(query(&first.subscription.id)).await.unwrap();
    assert_eq!(first_page.total, 2);
    let node_a = first_page
        .items
        .iter()
        .find(|item| item.name == "Node A")
        .unwrap()
        .clone();
    let node_b = first_page
        .items
        .iter()
        .find(|item| item.name == "Node B")
        .unwrap()
        .clone();
    let saved_a = proxies.get(&node_a.proxy_id).await.unwrap();
    assert!(saved_a.managed && saved_a.available && saved_a.enabled);
    let first_snapshot = mihomo.runtime_snapshot().await.unwrap();
    assert_eq!(first_snapshot.config_revision, first.config_revision);
    let node_a_port = first_snapshot
        .nodes
        .iter()
        .find(|node| node.id == node_a.id)
        .unwrap()
        .port;
    let old_ports = first_snapshot
        .nodes
        .into_iter()
        .map(|node| node.port)
        .collect::<Vec<_>>();
    assert_eq!(old_ports.len(), 2);
    assert!(
        old_ports
            .iter()
            .all(|port| (20_000..=39_999).contains(port))
    );

    accounts
        .insert_provider_account(account("acct_mihomo", "mihomo-user"))
        .await
        .unwrap();
    sqlx::query(
        "update provider_accounts set outbound_proxy_id = $2, outbound_proxy_url = $3 where id = $1",
    )
    .bind("acct_mihomo")
    .bind(&node_a.proxy_id)
    .bind(&node_a.endpoint)
    .execute(&database.pool)
    .await
    .unwrap();

    let refreshed = mihomo
        .sync_nodes(
            &first.subscription.id,
            first.subscription.revision,
            vec![node("Node B", "b1"), node("Node C", "c1")],
            &context(),
        )
        .await
        .unwrap();
    assert!(refreshed.config_revision.get() > first.config_revision.get());
    let current = mihomo.nodes(query(&first.subscription.id)).await.unwrap();
    let missing_a = current
        .items
        .iter()
        .find(|item| item.id == node_a.id)
        .unwrap();
    assert!(!missing_a.available);
    assert_eq!(missing_a.proxy_id, node_a.proxy_id);
    assert_eq!(missing_a.endpoint, node_a.endpoint);
    assert_eq!(missing_a.account_count, 1);
    let node_b_after = current
        .items
        .iter()
        .find(|item| item.id == node_b.id)
        .unwrap();
    assert_eq!(node_b_after.endpoint, node_b.endpoint);
    let runtime = mihomo.runtime_snapshot().await.unwrap();
    assert_eq!(runtime.config_revision, refreshed.config_revision);
    assert_eq!(runtime.nodes.len(), 2);
    assert!(!runtime.nodes.iter().any(|node| node.id == node_a.id));
    assert!(!runtime.nodes.iter().any(|node| node.port == node_a_port));
    let unavailable_a = proxies.get(&node_a.proxy_id).await.unwrap();
    assert!(unavailable_a.managed && !unavailable_a.available && unavailable_a.enabled);
    assert_eq!(
        proxies
            .reserve_import(&node_a.proxy_id)
            .await
            .err()
            .unwrap()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    accounts
        .insert_provider_account(account("acct_mihomo_unbound", "mihomo-unbound"))
        .await
        .unwrap();
    for selection in [
        gateway_admin::model::proxies::AccountProxySelection::Saved(node_a.proxy_id.clone()),
        gateway_admin::model::proxies::AccountProxySelection::Url(unavailable_a.proxy.clone()),
    ] {
        let bind_error = super::admin_account_store(&database.pool)
            .update_account(
                UpdateAccount {
                    account_id: "acct_mihomo_unbound".to_owned(),
                    name: None,
                    notes: None,
                    enabled: true,
                    concurrency_limit: None,
                    weight: gateway_admin::model::accounts::AccountWeight::DEFAULT,
                    model_access: None,
                    group_ids: vec![],
                    outbound_proxy: Some(selection),
                },
                &context(),
            )
            .await
            .unwrap_err();
        assert_eq!(bind_error.kind(), AdminStoreErrorKind::Conflict);
    }
    let account_binding: (Option<String>, Option<String>) = sqlx::query_as(
        "select outbound_proxy_id, outbound_proxy_url from provider_accounts where id = $1",
    )
    .bind("acct_mihomo")
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(account_binding.0.as_deref(), Some(node_a.proxy_id.as_str()));
    assert_eq!(account_binding.1.as_deref(), Some(node_a.endpoint.as_str()));
    let bound_account = accounts
        .get_account(&ProviderAccountId::new("acct_mihomo").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bound_account.outbound_proxy(), Some(&unavailable_a.proxy));

    let updated = proxies
        .update(
            UpdateProxy {
                max_websocket_connections: None,
                enabled: Some(false),
                auto_location: None,
                test: None,
                location: None,
                id: node_a.proxy_id.clone(),
                revision: proxies.get(&node_a.proxy_id).await.unwrap().revision,
                name: "Node A".to_owned(),
                proxy: None,
            },
            &context(),
        )
        .await
        .unwrap_err();
    assert_eq!(updated.kind(), AdminStoreErrorKind::Conflict);
    let deleted = proxies
        .delete(
            &node_a.proxy_id,
            proxies.get(&node_a.proxy_id).await.unwrap().revision,
            &context(),
        )
        .await
        .unwrap_err();
    assert_eq!(deleted.kind(), AdminStoreErrorKind::Conflict);

    let reappeared = mihomo
        .sync_nodes(
            &first.subscription.id,
            refreshed.subscription.revision,
            vec![
                node("Node A", "a1"),
                node("Node B", "b1"),
                node("Node C", "c1"),
            ],
            &context(),
        )
        .await
        .unwrap();
    let active = mihomo.nodes(query(&first.subscription.id)).await.unwrap();
    let restored_a = active
        .items
        .iter()
        .find(|item| item.name == "Node A")
        .unwrap();
    assert_eq!(restored_a.id, node_a.id);
    assert_eq!(restored_a.proxy_id, node_a.proxy_id);
    assert_eq!(restored_a.endpoint, node_a.endpoint);

    let renamed = mihomo
        .sync_nodes(
            &first.subscription.id,
            reappeared.subscription.revision,
            vec![
                node("Node A renamed", "a2"),
                node("Node B", "b1"),
                node("Node C", "c1"),
            ],
            &context(),
        )
        .await
        .unwrap();
    let renamed_page = mihomo.nodes(query(&first.subscription.id)).await.unwrap();
    let renamed_a = renamed_page
        .items
        .iter()
        .find(|item| item.name == "Node A renamed")
        .unwrap();
    let old_a = renamed_page
        .items
        .iter()
        .find(|item| item.id == node_a.id)
        .unwrap();
    assert_ne!(renamed_a.id, node_a.id);
    assert_ne!(renamed_a.endpoint, node_a.endpoint);
    assert!(!old_a.available);
    assert_eq!(old_a.endpoint, node_a.endpoint);

    assert_eq!(
        mihomo
            .delete(
                &first.subscription.id,
                renamed.subscription.revision,
                &context(),
            )
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    proxies
        .remove_account(
            &node_a.proxy_id,
            &ProviderAccountId::new("acct_mihomo").unwrap(),
            &context(),
        )
        .await
        .unwrap();
    let reservation = proxies.reserve_import(&node_b.proxy_id).await.unwrap();
    assert_eq!(
        mihomo
            .delete(
                &first.subscription.id,
                renamed.subscription.revision,
                &context(),
            )
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    drop(reservation);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if mihomo
                .delete(
                    &first.subscription.id,
                    renamed.subscription.revision,
                    &context(),
                )
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let second = mihomo
        .create(
            NewMihomoSubscription {
                name: "Second subscription".to_owned(),
                url: SecretString::from("https://subscription.invalid/other".to_owned()),
            },
            vec![node("Node D", "d1")],
            &context(),
        )
        .await
        .unwrap();
    let second_node = mihomo
        .nodes(query(&second.subscription.id))
        .await
        .unwrap()
        .items
        .remove(0);
    let second_snapshot = mihomo.runtime_snapshot().await.unwrap();
    assert_eq!(second_snapshot.config_revision, second.config_revision);
    let second_port = second_snapshot
        .nodes
        .into_iter()
        .find(|node| node.id == second_node.id)
        .unwrap()
        .port;
    assert!(old_ports.contains(&second_port));
    database.close().await;
}

#[tokio::test]
async fn managed_node_location_following_preserves_subscription_config_and_import_guards() {
    let Some(database) = TestDatabase::create("mihomo_import_location").await else {
        return;
    };
    let mihomo = PgMihomoRepository::new(database.pool.clone());
    let proxies = PgProxyRepository::new(database.pool.clone());
    let created = mihomo
        .create(
            NewMihomoSubscription {
                name: "Location subscription".to_owned(),
                url: SecretString::from("https://subscription.invalid/location".to_owned()),
            },
            vec![node("Node A", "a1")],
            &context(),
        )
        .await
        .unwrap();
    let node = mihomo
        .nodes(query(&created.subscription.id))
        .await
        .unwrap()
        .items
        .remove(0);
    let original = proxies.get(&node.proxy_id).await.unwrap();
    assert!(original.managed && !original.auto_location);
    let location: RequestLocation = serde_json::from_value(serde_json::json!({
        "country": "US", "region": "Arizona", "city": "Phoenix", "timezone": "America/Phoenix"
    }))
    .unwrap();
    let mut enable = UpdateProxy {
        id: original.id.clone(),
        revision: original.revision,
        name: original.name.clone(),
        auto_location: Some(true),
        enabled: None,
        max_websocket_connections: None,
        location: None,
        proxy: None,
        test: Some(ProxyTestResult {
            location: ProxyLocationDetection::Detected {
                location: location.clone(),
            },
            success: true,
            latency_ms: 20,
            exit_ip: None,
            exit_ipv4: None,
            exit_ipv6: None,
            message: "synthetic probe".to_owned(),
        }),
    };
    let enabled = proxies.update(enable.clone(), &context()).await.unwrap();
    assert!(enabled.record.auto_location);
    assert_eq!(enabled.record.effective_location(), Some(&location));
    assert_eq!(enabled.record.proxy, original.proxy);
    assert!(enabled.record.revision > original.revision);
    // 开启跟随后仍需校验版本，不能覆盖已经变化的节点状态
    assert_eq!(
        proxies
            .update(enable.clone(), &context())
            .await
            .unwrap_err()
            .kind(),
        AdminStoreErrorKind::Conflict
    );
    enable.revision = enabled.record.revision;
    for forbidden in [
        UpdateProxy {
            name: "Other node".to_owned(),
            ..enable.clone()
        },
        UpdateProxy {
            proxy: Some(OutboundProxy::parse("http://127.0.0.1:19999").unwrap()),
            ..enable.clone()
        },
        UpdateProxy {
            enabled: Some(false),
            ..enable.clone()
        },
        UpdateProxy {
            auto_location: Some(false),
            ..enable.clone()
        },
        UpdateProxy {
            location: Some(Some(RequestLocation::default())),
            ..enable.clone()
        },
        UpdateProxy {
            max_websocket_connections: Some(1),
            ..enable.clone()
        },
    ] {
        assert_eq!(
            proxies
                .update(forbidden, &context())
                .await
                .unwrap_err()
                .kind(),
            AdminStoreErrorKind::Conflict
        );
        let current = proxies.get(&original.id).await.unwrap();
        assert_eq!(current.revision, enabled.record.revision);
        assert_eq!(current.proxy, original.proxy);
        assert_eq!(current.effective_location(), Some(&location));
        assert_eq!(
            mihomo.runtime_snapshot().await.unwrap().config_revision,
            enabled.config_revision
        );
    }
    let reservation = proxies.reserve_import(&original.id).await.unwrap();
    assert_eq!(reservation.binding.proxy, original.proxy);
    assert_eq!(reservation.request_location, Some(location));
    assert_eq!(
        proxies.update(enable, &context()).await.unwrap_err().kind(),
        AdminStoreErrorKind::Conflict
    );
    drop(reservation);
    database.close().await;
}

#[tokio::test]
async fn active_import_allows_unchanged_nodes_but_rejects_changes_and_withdrawals() {
    let Some(database) = TestDatabase::create("mihomo_import_guard").await else {
        return;
    };
    let mihomo = PgMihomoRepository::new(database.pool.clone());
    let proxies = PgProxyRepository::new(database.pool.clone());
    let original = node("Node A", "a1");
    let created = mihomo
        .create(
            NewMihomoSubscription {
                name: "Import subscription".to_owned(),
                url: SecretString::from("https://subscription.invalid/import".to_owned()),
            },
            vec![original.clone()],
            &context(),
        )
        .await
        .unwrap();
    let saved = mihomo
        .nodes(query(&created.subscription.id))
        .await
        .unwrap()
        .items
        .remove(0);
    let reservation = proxies.reserve_import(&saved.proxy_id).await.unwrap();
    let unchanged = mihomo
        .sync_nodes(
            &created.subscription.id,
            created.subscription.revision,
            vec![original.clone()],
            &context(),
        )
        .await
        .unwrap();
    let mut changed_kind = original.clone();
    changed_kind.kind = "socks5".to_owned();
    for candidate in [vec![node("Node A", "a2")], vec![changed_kind], vec![]] {
        assert_eq!(
            mihomo
                .sync_nodes(
                    &created.subscription.id,
                    unchanged.subscription.revision,
                    candidate,
                    &context(),
                )
                .await
                .unwrap_err()
                .kind(),
            AdminStoreErrorKind::Conflict
        );
        assert_eq!(
            mihomo
                .subscription(&created.subscription.id)
                .await
                .unwrap()
                .revision,
            unchanged.subscription.revision
        );
        let snapshot = mihomo.runtime_snapshot().await.unwrap();
        assert_eq!(snapshot.config_revision, unchanged.config_revision);
        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(snapshot.nodes[0].config, original.config);
        let current = mihomo
            .nodes(query(&created.subscription.id))
            .await
            .unwrap()
            .items
            .remove(0);
        assert_eq!(current.kind, original.kind);
        assert!(current.available);
    }
    drop(reservation);
    database.close().await;
}

fn node(name: &str, server: &str) -> MihomoNodeConfig {
    MihomoNodeConfig {
        name: name.to_owned(),
        kind: "ss".to_owned(),
        config: serde_json::json!({
            "server": format!("{server}.example.invalid"),
            "port": 443,
            "cipher": "chacha20-ietf-poly1305",
            "password": "fixture-secret",
        }),
    }
}

fn query(subscription_id: &str) -> MihomoNodeQuery {
    MihomoNodeQuery {
        subscription_id: subscription_id.to_owned(),
        page: 1,
        page_size: PageSize::new(20).unwrap(),
        search: String::new(),
        sort: Default::default(),
    }
}

fn context() -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: "mihomo-store-test".to_owned(),
    }
}
