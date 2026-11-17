//! Mihomo 订阅、稳定节点入口与运行配置的 PostgreSQL 适配

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use gateway_admin::{
    model::{
        MutationContext, Revision as AdminRevision,
        audit::MutationAuditOperation,
        mihomo::{
            MihomoNode, MihomoNodeConfig, MihomoNodePage, MihomoNodeQuery, MihomoNodeSort,
            MihomoRefreshResult, MihomoRuntimeNode, MihomoRuntimeSnapshot, MihomoSubscription,
            NewMihomoSubscription, UpdateMihomoSubscription,
        },
        proxies::TestProxy,
    },
    ports::{
        mihomo::MihomoStore,
        store::{AdminStoreError, AdminStoreResult},
    },
};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row as _, Transaction, postgres::PgRow, types::Json};

use super::{append_admin_audit_event_in_transaction, bump_config_revision_in_transaction};
use crate::{
    ConflictKind, Revision, StoreError, StoreResult, admin_revision, admin_store_error,
    mutation_audit, postgres_unavailable,
};

const ENTITY: &str = "mihomo subscription";

#[derive(Clone)]
pub struct PgMihomoRepository {
    pool: PgPool,
}

impl PgMihomoRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

struct ExistingNode {
    id: String,
    proxy_id: String,
    kind: String,
    config: Value,
    available: bool,
}

fn store_error(error: StoreError) -> AdminStoreError {
    admin_store_error(ENTITY, error)
}

fn unavailable(source: impl std::error::Error + Send + Sync + 'static) -> StoreError {
    postgres_unavailable("mihomo subscription operation", source)
}

fn conflict(id: &str) -> StoreError {
    StoreError::Conflict {
        source: None,
        entity: ENTITY,
        id: id.to_owned(),
        kind: ConflictKind::InvalidTransition,
    }
}

fn invalid() -> StoreError {
    StoreError::InvalidData {
        source: None,
        entity: ENTITY,
        message: "invalid mihomo subscription record".to_owned(),
    }
}

fn not_found(id: &str) -> StoreError {
    StoreError::NotFound {
        source: None,
        entity: ENTITY,
        id: id.to_owned(),
    }
}

fn subscription(row: PgRow) -> StoreResult<MihomoSubscription> {
    let url: String = row
        .try_get("url")
        .map_err(|source| invalid().with_source(source))?;
    let revision = AdminRevision::new(
        u64::try_from(
            row.try_get::<i64, _>("revision")
                .map_err(|source| invalid().with_source(source))?,
        )
        .map_err(|source| invalid().with_source(source))?,
    )
    .map_err(|source| invalid().with_source(source))?;
    Ok(MihomoSubscription {
        id: row
            .try_get("id")
            .map_err(|source| invalid().with_source(source))?,
        name: row
            .try_get("name")
            .map_err(|source| invalid().with_source(source))?,
        url: SecretString::from(url),
        revision,
        node_count: u64::try_from(
            row.try_get::<i64, _>("node_count")
                .map_err(|source| invalid().with_source(source))?,
        )
        .map_err(|source| invalid().with_source(source))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|source| invalid().with_source(source))?,
    })
}

async fn current_config_revision(
    transaction: &mut Transaction<'_, Postgres>,
) -> StoreResult<Revision> {
    let value = sqlx::query_scalar::<_, i64>(
        "select config_revision from runtime_settings where id = 1 for update",
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or_else(|| StoreError::NotFound {
        source: None,
        entity: "runtime settings",
        id: "1".to_owned(),
    })?;
    Revision::new(u64::try_from(value).map_err(|source| invalid().with_source(source))?)
}

async fn subscription_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    id: &str,
) -> StoreResult<MihomoSubscription> {
    let row = sqlx::query(
        "select s.id, s.name, s.url, s.revision, s.updated_at,
                (select count(*) from mihomo_nodes n
                 where n.subscription_id = s.id and n.available) as node_count
         from mihomo_subscriptions s where s.id = $1",
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unavailable)?
    .ok_or_else(|| not_found(id))?;
    subscription(row)
}

async fn append_audit(
    transaction: &mut Transaction<'_, Postgres>,
    context: &MutationContext,
    operation: MutationAuditOperation,
    id: &str,
    fields: &[&str],
    revision: Revision,
) -> StoreResult<()> {
    append_admin_audit_event_in_transaction(
        transaction,
        mutation_audit(
            context,
            operation,
            id,
            fields.iter().map(|field| (*field).to_owned()).collect(),
        ),
        revision,
    )
    .await
}

async fn existing_nodes(
    transaction: &mut Transaction<'_, Postgres>,
    subscription_id: &str,
) -> StoreResult<HashMap<String, ExistingNode>> {
    let rows = sqlx::query(
        "select id, proxy_id, source_name, kind, config_json, available
         from mihomo_nodes where subscription_id = $1 for update",
    )
    .bind(subscription_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(unavailable)?;
    rows.into_iter()
        .map(|row| {
            let name: String = row
                .try_get("source_name")
                .map_err(|source| invalid().with_source(source))?;
            let config = row
                .try_get::<Json<Value>, _>("config_json")
                .map_err(|source| invalid().with_source(source))?
                .0;
            Ok((
                name,
                ExistingNode {
                    id: row
                        .try_get("id")
                        .map_err(|source| invalid().with_source(source))?,
                    proxy_id: row
                        .try_get("proxy_id")
                        .map_err(|source| invalid().with_source(source))?,
                    kind: row
                        .try_get("kind")
                        .map_err(|source| invalid().with_source(source))?,
                    config,
                    available: row
                        .try_get("available")
                        .map_err(|source| invalid().with_source(source))?,
                },
            ))
        })
        .collect()
}

async fn sync_nodes_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    subscription_id: &str,
    nodes: &[MihomoNodeConfig],
) -> StoreResult<bool> {
    let mut existing = existing_nodes(transaction, subscription_id).await?;
    let incoming_names: HashSet<&str> = nodes.iter().map(|node| node.name.as_str()).collect();
    let mut runtime_changed = false;

    for (name, node) in &existing {
        if node.available && !incoming_names.contains(name.as_str()) {
            if !super::proxies::try_exclude_active_imports(transaction, &node.proxy_id).await? {
                return Err(conflict(subscription_id));
            }
            sqlx::query(
                "update mihomo_nodes set available = false, updated_at = now() where id = $1",
            )
            .bind(&node.id)
            .execute(&mut **transaction)
            .await
            .map_err(unavailable)?;
            invalidate_node_probe(transaction, &node.proxy_id).await?;
            runtime_changed = true;
        }
    }

    for node in nodes {
        if let Some(current) = existing.remove(&node.name) {
            if !current.available || current.kind != node.kind || current.config != node.config {
                // 导入授权预留期间保持节点实际出口不变，未变化节点不占用排他锁
                if !super::proxies::try_exclude_active_imports(transaction, &current.proxy_id)
                    .await?
                {
                    return Err(conflict(subscription_id));
                }
                invalidate_node_probe(transaction, &current.proxy_id).await?;
                runtime_changed = true;
            }
            sqlx::query(
                "update mihomo_nodes
                 set kind = $2, config_json = $3, available = true, updated_at = now()
                 where id = $1",
            )
            .bind(&current.id)
            .bind(&node.kind)
            .bind(Json(node.config.clone()))
            .execute(&mut **transaction)
            .await
            .map_err(unavailable)?;
            continue;
        }

        // 所有订阅写入已持有 runtime_settings 行锁，失效节点仍占用原端口，删除后才可复用
        let port: i32 = sqlx::query_scalar(
            "select port from generate_series(20000, 39999) as candidate(port)
             where not exists(select 1 from mihomo_nodes n where n.listener_port = candidate.port)
             order by port limit 1",
        )
        .fetch_optional(&mut **transaction)
        .await
        .map_err(unavailable)?
        .ok_or_else(invalid)?;
        let node_id = format!("mihomo_node_{}", uuid::Uuid::now_v7().simple());
        let proxy_id = format!("mihomo_proxy_{}", uuid::Uuid::now_v7().simple());
        let proxy =
            gateway_core::account::OutboundProxy::parse(&format!("http://127.0.0.1:{port}"))
                .map_err(|source| invalid().with_source(source))?;
        sqlx::query(
            "insert into outbound_proxies (id, name, proxy_url, enabled)
             values ($1, $2, $3, true)",
        )
        .bind(&proxy_id)
        .bind(&node.name)
        .bind(proxy.expose_url())
        .execute(&mut **transaction)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            "insert into mihomo_nodes
             (id, subscription_id, source_name, proxy_id, listener_port, kind, config_json)
             values ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(node_id)
        .bind(subscription_id)
        .bind(&node.name)
        .bind(proxy_id)
        .bind(port)
        .bind(&node.kind)
        .bind(Json(node.config.clone()))
        .execute(&mut **transaction)
        .await
        .map_err(unavailable)?;
        runtime_changed = true;
    }
    Ok(runtime_changed)
}

fn map_proxy_record(row: PgRow) -> StoreResult<MihomoNode> {
    let id = row
        .try_get("node_id")
        .map_err(|source| invalid().with_source(source))?;
    let kind = row
        .try_get("kind")
        .map_err(|source| invalid().with_source(source))?;
    let record = super::proxies::record(row)?;
    Ok(MihomoNode {
        id,
        proxy_id: record.id,
        name: record.name,
        kind,
        available: record.available,
        account_count: record.account_count,
        endpoint: record.proxy.endpoint(),
        proxy_revision: record.revision,
        auto_location: record.auto_location,
        location: record.location,
        detected_location: record.detected_location,
        last_test: record.last_test,
        last_test_at: record.last_test_at,
    })
}

async fn invalidate_node_probe(
    transaction: &mut Transaction<'_, Postgres>,
    proxy_id: &str,
) -> StoreResult<()> {
    // 本地监听地址不变，实际节点已变化；推进版本以拒绝旧出口的迟到探测结果
    sqlx::query(
        "update outbound_proxies set revision = revision + 1, updated_at = now(),
         last_test_at = null, last_test_success = null, last_test_latency_ms = null,
         last_test_ip = null, last_test_ipv4 = null, last_test_ipv6 = null,
         last_test_message = null, detected_location_json = null,
         last_location_detection_json = '{\"status\":\"notRequested\"}'::jsonb where id = $1",
    )
    .bind(proxy_id)
    .execute(&mut **transaction)
    .await
    .map_err(unavailable)?;
    Ok(())
}

fn map_foreign_key_error(error: sqlx::Error, id: &str) -> AdminStoreError {
    if error.as_database_error().is_some_and(|database| {
        database.is_foreign_key_violation() || database.code().as_deref() == Some("23001")
    }) {
        store_error(conflict(id))
    } else {
        store_error(unavailable(error))
    }
}

#[async_trait]
impl MihomoStore for PgMihomoRepository {
    async fn subscriptions(&self) -> AdminStoreResult<Vec<MihomoSubscription>> {
        let rows = sqlx::query(
            "select s.id, s.name, s.url, s.revision, s.updated_at,
                    (select count(*) from mihomo_nodes n
                     where n.subscription_id = s.id and n.available) as node_count
             from mihomo_subscriptions s order by lower(s.name), s.id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        rows.into_iter()
            .map(subscription)
            .collect::<StoreResult<Vec<_>>>()
            .map_err(store_error)
    }

    async fn subscription(&self, id: &str) -> AdminStoreResult<MihomoSubscription> {
        let row = sqlx::query(
            "select s.id, s.name, s.url, s.revision, s.updated_at,
                    (select count(*) from mihomo_nodes n
                     where n.subscription_id = s.id and n.available) as node_count
             from mihomo_subscriptions s where s.id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|source| store_error(unavailable(source)))?
        .ok_or_else(|| store_error(not_found(id)))?;
        subscription(row).map_err(store_error)
    }

    async fn create(
        &self,
        command: NewMihomoSubscription,
        nodes: Vec<MihomoNodeConfig>,
        context: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let current_revision = current_config_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        let id = format!("mihomo_sub_{}", uuid::Uuid::now_v7().simple());
        sqlx::query("insert into mihomo_subscriptions (id, name, url) values ($1, $2, $3)")
            .bind(&id)
            .bind(&command.name)
            .bind(command.url.expose_secret())
            .execute(&mut *transaction)
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let runtime_changed = sync_nodes_in_transaction(&mut transaction, &id, &nodes)
            .await
            .map_err(store_error)?;
        let revision = if runtime_changed {
            bump_config_revision_in_transaction(&mut transaction)
                .await
                .map_err(store_error)?
        } else {
            current_revision
        };
        append_audit(
            &mut transaction,
            context,
            MutationAuditOperation::MihomoSubscriptionCreate,
            &id,
            &["name", "url", "nodes"],
            revision,
        )
        .await
        .map_err(store_error)?;
        let subscription = subscription_in_transaction(&mut transaction, &id)
            .await
            .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        Ok(MihomoRefreshResult {
            subscription,
            config_revision: admin_revision(revision)?,
        })
    }

    async fn update(
        &self,
        command: UpdateMihomoSubscription,
        nodes: Option<Vec<MihomoNodeConfig>>,
        context: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let current_config_revision = current_config_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        let locked: Option<String> = sqlx::query_scalar(
            "select id from mihomo_subscriptions where id = $1 and revision = $2 for update",
        )
        .bind(&command.id)
        .bind(
            i64::try_from(command.revision.get())
                .map_err(|source| store_error(invalid().with_source(source)))?,
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        if locked.is_none() {
            let error = match transaction.rollback().await {
                Ok(()) => conflict(&command.id),
                Err(cleanup) => conflict(&command.id).with_cleanup(cleanup),
            };
            return Err(store_error(error));
        }
        let updated = sqlx::query(
            "update mihomo_subscriptions
             set name = coalesce($3, name), url = coalesce($4, url),
                 revision = revision + 1, updated_at = now()
             where id = $1 and revision = $2",
        )
        .bind(&command.id)
        .bind(
            i64::try_from(command.revision.get())
                .map_err(|source| store_error(invalid().with_source(source)))?,
        )
        .bind(&command.name)
        .bind(command.url.as_ref().map(|url| url.expose_secret()))
        .execute(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        if updated.rows_affected() != 1 {
            return Err(store_error(conflict(&command.id)));
        }
        let runtime_changed = if let Some(nodes) = &nodes {
            sync_nodes_in_transaction(&mut transaction, &command.id, nodes)
                .await
                .map_err(store_error)?
        } else {
            false
        };
        let revision = if runtime_changed {
            bump_config_revision_in_transaction(&mut transaction)
                .await
                .map_err(store_error)?
        } else {
            current_config_revision
        };
        let mut changed_fields = Vec::new();
        if command.name.is_some() {
            changed_fields.push("name".to_owned());
        }
        if command.url.is_some() {
            changed_fields.push("url".to_owned());
        }
        if nodes.is_some() {
            changed_fields.push("nodes".to_owned());
        }
        append_admin_audit_event_in_transaction(
            &mut transaction,
            mutation_audit(
                context,
                MutationAuditOperation::MihomoSubscriptionUpdate,
                &command.id,
                changed_fields,
            ),
            revision,
        )
        .await
        .map_err(store_error)?;
        let subscription = subscription_in_transaction(&mut transaction, &command.id)
            .await
            .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        Ok(MihomoRefreshResult {
            subscription,
            config_revision: admin_revision(revision)?,
        })
    }

    async fn sync_nodes(
        &self,
        id: &str,
        revision: AdminRevision,
        nodes: Vec<MihomoNodeConfig>,
        context: &MutationContext,
    ) -> AdminStoreResult<MihomoRefreshResult> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let _current_config_revision = current_config_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        let locked: Option<String> = sqlx::query_scalar(
            "select id from mihomo_subscriptions where id = $1 and revision = $2 for update",
        )
        .bind(id)
        .bind(
            i64::try_from(revision.get())
                .map_err(|source| store_error(invalid().with_source(source)))?,
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        if locked.is_none() {
            let error = match transaction.rollback().await {
                Ok(()) => conflict(id),
                Err(cleanup) => conflict(id).with_cleanup(cleanup),
            };
            return Err(store_error(error));
        }
        sqlx::query(
            "update mihomo_subscriptions set revision = revision + 1, updated_at = now() where id = $1",
        )
        .bind(id)
        .execute(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        sync_nodes_in_transaction(&mut transaction, id, &nodes)
            .await
            .map_err(store_error)?;
        let config_revision = bump_config_revision_in_transaction(&mut transaction)
            .await
            .map_err(store_error)?;
        append_audit(
            &mut transaction,
            context,
            MutationAuditOperation::MihomoSubscriptionRefresh,
            id,
            &["nodes"],
            config_revision,
        )
        .await
        .map_err(store_error)?;
        let subscription = subscription_in_transaction(&mut transaction, id)
            .await
            .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        Ok(MihomoRefreshResult {
            subscription,
            config_revision: admin_revision(config_revision)?,
        })
    }

    async fn delete(
        &self,
        id: &str,
        revision: AdminRevision,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminRevision> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let current_config_revision = current_config_revision(&mut transaction)
            .await
            .map_err(store_error)?;
        let locked: Option<String> = sqlx::query_scalar(
            "select id from mihomo_subscriptions where id = $1 and revision = $2 for update",
        )
        .bind(id)
        .bind(
            i64::try_from(revision.get())
                .map_err(|source| store_error(invalid().with_source(source)))?,
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        if locked.is_none() {
            let error = match transaction.rollback().await {
                Ok(()) => conflict(id),
                Err(cleanup) => conflict(id).with_cleanup(cleanup),
            };
            return Err(store_error(error));
        }
        let proxy_ids: Vec<String> = sqlx::query_scalar(
            "select p.id from outbound_proxies p join mihomo_nodes n on n.proxy_id = p.id
             where n.subscription_id = $1 order by p.id for update of p",
        )
        .bind(id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        for proxy_id in &proxy_ids {
            let acquired = super::proxies::try_exclude_active_imports(&mut transaction, proxy_id)
                .await
                .map_err(store_error)?;
            if !acquired {
                let error = match transaction.rollback().await {
                    Ok(()) => conflict(id),
                    Err(cleanup) => conflict(id).with_cleanup(cleanup),
                };
                return Err(store_error(error));
            }
        }
        let active: bool = sqlx::query_scalar(
            "select exists(select 1 from mihomo_nodes where subscription_id = $1 and available)",
        )
        .bind(id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        let referenced: bool = sqlx::query_scalar(
            "select exists(
                 select 1 from mihomo_nodes n
                 where n.subscription_id = $1 and (
                     exists(select 1 from provider_accounts a where a.outbound_proxy_id = n.proxy_id)
                     or exists(select 1 from plugin_update_sources s where s.outbound_proxy_id = n.proxy_id)
                     or exists(select 1 from plugin_artifacts p where p.outbound_proxy_id = n.proxy_id)
                 )
             )",
        )
        .bind(id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        if referenced {
            return Err(store_error(conflict(id)));
        }
        let config_revision = if active {
            bump_config_revision_in_transaction(&mut transaction)
                .await
                .map_err(store_error)?
        } else {
            current_config_revision
        };
        let deleted =
            sqlx::query("delete from mihomo_subscriptions where id = $1 and revision = $2")
                .bind(id)
                .bind(
                    i64::try_from(revision.get())
                        .map_err(|source| store_error(invalid().with_source(source)))?,
                )
                .execute(&mut *transaction)
                .await
                .map_err(|source| store_error(unavailable(source)))?;
        if deleted.rows_affected() != 1 {
            return Err(store_error(conflict(id)));
        }
        sqlx::query("delete from outbound_proxies where id = any($1::text[])")
            .bind(&proxy_ids)
            .execute(&mut *transaction)
            .await
            .map_err(|error| map_foreign_key_error(error, id))?;
        append_audit(
            &mut transaction,
            context,
            MutationAuditOperation::MihomoSubscriptionDelete,
            id,
            &[],
            config_revision,
        )
        .await
        .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        admin_revision(config_revision)
    }

    async fn nodes(&self, query: MihomoNodeQuery) -> AdminStoreResult<MihomoNodePage> {
        if query.page == 0 {
            return Err(store_error(invalid()));
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        sqlx::query("set transaction isolation level repeatable read, read only")
            .execute(&mut *transaction)
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from mihomo_subscriptions where id = $1)")
                .bind(&query.subscription_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(|source| store_error(unavailable(source)))?;
        if !exists {
            return Err(store_error(not_found(&query.subscription_id)));
        }
        let total: i64 = sqlx::query_scalar(
            "select count(*) from mihomo_nodes n
             where n.subscription_id = $1 and strpos(lower(n.source_name), lower($2)) > 0",
        )
        .bind(&query.subscription_id)
        .bind(&query.search)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        let sort = match query.sort {
            MihomoNodeSort::Name => "name",
            MihomoNodeSort::LatencyAsc => "latencyAsc",
            MihomoNodeSort::LatencyDesc => "latencyDesc",
        };
        let rows = sqlx::query(
            "select p.*, n.id as node_id, n.kind, true as managed, n.available,
                    (select count(*) from provider_accounts a where a.outbound_proxy_id = n.proxy_id) as account_count
             from mihomo_nodes n join outbound_proxies p on p.id = n.proxy_id
             where n.subscription_id = $1 and strpos(lower(n.source_name), lower($2)) > 0
             order by case when $5 <> 'name' then n.available end desc,
                      case when $5 = 'latencyAsc' and p.last_test_success then p.last_test_latency_ms end asc nulls last,
                      case when $5 = 'latencyDesc' and p.last_test_success then p.last_test_latency_ms end desc nulls last,
                      lower(n.source_name), n.id limit $3 offset $4",
        )
            .bind(&query.subscription_id)
            .bind(&query.search)
            .bind(i64::from(query.page_size.get()))
            .bind(i64::from(query.page - 1) * i64::from(query.page_size.get()))
            .bind(sort)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let items = rows
            .into_iter()
            .map(map_proxy_record)
            .collect::<StoreResult<Vec<_>>>()
            .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        Ok(MihomoNodePage {
            items,
            total: u64::try_from(total)
                .map_err(|source| store_error(invalid().with_source(source)))?,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }

    async fn probe_targets(&self) -> AdminStoreResult<Vec<TestProxy>> {
        let rows = sqlx::query(
            "select p.id, p.revision from mihomo_nodes n
             join outbound_proxies p on p.id = n.proxy_id
             where n.available and p.enabled and
                (p.last_test_at is null or p.last_test_at < now() -
                 case when p.last_test_success and p.last_location_detection_json->>'status' = 'detected'
                      then interval '30 minutes' else interval '5 minutes' end)
             order by p.last_test_at asc nulls first, n.id limit 2",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        rows.into_iter()
            .map(|row| {
                Ok(TestProxy {
                    id: row
                        .try_get("id")
                        .map_err(|source| store_error(invalid().with_source(source)))?,
                    revision: AdminRevision::new(
                        u64::try_from(
                            row.try_get::<i64, _>("revision")
                                .map_err(|source| store_error(invalid().with_source(source)))?,
                        )
                        .map_err(|source| store_error(invalid().with_source(source)))?,
                    )
                    .map_err(|source| store_error(invalid().with_source(source)))?,
                    detect_location: true,
                })
            })
            .collect()
    }

    async fn runtime_snapshot(&self) -> AdminStoreResult<MihomoRuntimeSnapshot> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        sqlx::query("set transaction isolation level repeatable read, read only")
            .execute(&mut *transaction)
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        let config_revision: i64 =
            sqlx::query_scalar("select config_revision from runtime_settings where id = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(|source| store_error(unavailable(source)))?;
        let config_revision = AdminRevision::new(
            u64::try_from(config_revision)
                .map_err(|source| store_error(invalid().with_source(source)))?,
        )
        .map_err(|source| store_error(invalid().with_source(source)))?;
        let rows = sqlx::query(
            "select id, listener_port, config_json from mihomo_nodes
             where available order by subscription_id, lower(source_name), id",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|source| store_error(unavailable(source)))?;
        let nodes = rows
            .into_iter()
            .map(|row| {
                Ok(MihomoRuntimeNode {
                    id: row
                        .try_get("id")
                        .map_err(|source| invalid().with_source(source))?,
                    port: u16::try_from(
                        row.try_get::<i32, _>("listener_port")
                            .map_err(|source| invalid().with_source(source))?,
                    )
                    .map_err(|source| invalid().with_source(source))?,
                    config: row
                        .try_get::<Json<Value>, _>("config_json")
                        .map_err(|source| invalid().with_source(source))?
                        .0,
                })
            })
            .collect::<StoreResult<Vec<_>>>()
            .map_err(store_error)?;
        transaction
            .commit()
            .await
            .map_err(|source| store_error(unavailable(source)))?;
        Ok(MihomoRuntimeSnapshot {
            config_revision,
            nodes,
        })
    }
}
