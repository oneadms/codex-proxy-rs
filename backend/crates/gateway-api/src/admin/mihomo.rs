//! Mihomo 订阅与节点的管理 HTTP 合同及脱敏投影

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use gateway_admin::model::{
    PageSize, Revision,
    mihomo::{
        MihomoNode, MihomoNodeQuery, MihomoSubscription, NewMihomoSubscription,
        UpdateMihomoSubscription,
    },
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::auth::SessionState;

use super::{AdminAuth, AdminEnvelope, AdminError, AdminJson, AdminQuery, AdminResponse, PageMeta};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateSubscriptionRequest {
    name: String,
    url: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubscriptionRevisionRequest {
    id: String,
    revision: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateSubscriptionRequest {
    id: String,
    revision: u64,
    name: Option<String>,
    url: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeListQuery {
    subscription_id: String,
    page: Option<u32>,
    page_size: Option<u16>,
    search: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionView {
    id: String,
    name: String,
    has_url: bool,
    node_count: u64,
    revision: u64,
    updated_at: String,
    updated_at_display: String,
}

impl From<(MihomoSubscription, crate::time::TimePresenter)> for SubscriptionView {
    fn from((subscription, time): (MihomoSubscription, crate::time::TimePresenter)) -> Self {
        Self {
            id: subscription.id,
            name: subscription.name,
            has_url: !subscription.url.expose_secret().is_empty(),
            node_count: subscription.node_count,
            revision: subscription.revision.get(),
            updated_at_display: time.datetime(&subscription.updated_at),
            updated_at: subscription.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeView {
    id: String,
    proxy_id: String,
    name: String,
    #[serde(rename = "type")]
    kind: String,
    available: bool,
    account_count: u64,
    endpoint: String,
}

impl From<MihomoNode> for NodeView {
    fn from(node: MihomoNode) -> Self {
        Self {
            id: node.id,
            proxy_id: node.proxy_id,
            name: node.name,
            kind: node.kind,
            available: node.available,
            account_count: node.account_count,
            endpoint: node.endpoint,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionPageView {
    items: Vec<SubscriptionView>,
}

#[derive(Serialize)]
struct NodePageView {
    items: Vec<NodeView>,
    page: PageMeta,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RefreshView {
    subscription: SubscriptionView,
    node_count: u64,
    config_revision: u64,
}

pub fn router<S>() -> Router<S>
where
    S: SessionState + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(
            "/api/admin/mihomo/subscriptions",
            get(list_subscriptions::<S>),
        )
        .route(
            "/api/admin/mihomo/subscriptions/create",
            post(create_subscription::<S>),
        )
        .route(
            "/api/admin/mihomo/subscriptions/update",
            post(update_subscription::<S>),
        )
        .route(
            "/api/admin/mihomo/subscriptions/refresh",
            post(refresh_subscription::<S>),
        )
        .route(
            "/api/admin/mihomo/subscriptions/delete",
            post(delete_subscription::<S>),
        )
        .route("/api/admin/mihomo/nodes", get(list_nodes::<S>))
}

fn revision(value: u64) -> Result<Revision, AdminError> {
    if value > i64::MAX as u64 {
        return Err(AdminError::bad_request("订阅版本不合法"));
    }
    Revision::new(value).map_err(|_| AdminError::bad_request("订阅版本不合法"))
}

fn map_error(error: gateway_admin::model::AdminError) -> AdminError {
    super::wire::map_admin_service_error(error)
}

async fn list_subscriptions<S>(
    _: AdminAuth,
    State(state): State<S>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let time = crate::time::TimePresenter::new(state.admin_services().timezone());
    let subscriptions = state
        .admin_services()
        .mihomo()
        .subscriptions()
        .await
        .map_err(map_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(SubscriptionPageView {
            items: subscriptions
                .into_iter()
                .map(|subscription| SubscriptionView::from((subscription, time)))
                .collect(),
        }),
    ))
}

async fn create_subscription<S>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<CreateSubscriptionRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let time = crate::time::TimePresenter::new(state.admin_services().timezone());
    let result = state
        .admin_services()
        .mihomo()
        .create(
            NewMihomoSubscription {
                name: request.name,
                url: SecretString::from(request.url),
            },
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_error)?;
    Ok(AdminResponse::new(
        StatusCode::CREATED,
        AdminEnvelope::ok(SubscriptionView::from((result.subscription, time))),
    ))
}

async fn update_subscription<S>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<UpdateSubscriptionRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let time = crate::time::TimePresenter::new(state.admin_services().timezone());
    let result = state
        .admin_services()
        .mihomo()
        .update(
            UpdateMihomoSubscription {
                id: request.id,
                revision: revision(request.revision)?,
                name: request.name,
                url: request.url.map(SecretString::from),
            },
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(SubscriptionView::from((result.subscription, time))),
    ))
}

async fn refresh_subscription<S>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<SubscriptionRevisionRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    let time = crate::time::TimePresenter::new(state.admin_services().timezone());
    let result = state
        .admin_services()
        .mihomo()
        .refresh(
            &request.id,
            revision(request.revision)?,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_error)?;
    let node_count = result.subscription.node_count;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(RefreshView {
            subscription: SubscriptionView::from((result.subscription, time)),
            node_count,
            config_revision: result.config_revision.get(),
        }),
    ))
}

async fn delete_subscription<S>(
    auth: AdminAuth,
    State(state): State<S>,
    AdminJson(request): AdminJson<SubscriptionRevisionRequest>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    state
        .admin_services()
        .mihomo()
        .delete(
            &request.id,
            revision(request.revision)?,
            &auth.context().mutation_context(),
        )
        .await
        .map_err(map_error)?;
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(serde_json::Value::Null),
    ))
}

async fn list_nodes<S>(
    _: AdminAuth,
    State(state): State<S>,
    AdminQuery(query): AdminQuery<NodeListQuery>,
) -> Result<impl IntoResponse, AdminError>
where
    S: SessionState + Send + Sync,
{
    if query
        .search
        .as_ref()
        .is_some_and(|search| search.len() > 256 || search.chars().any(char::is_control))
    {
        return Err(AdminError::bad_request("节点搜索条件不合法"));
    }
    let result = state
        .admin_services()
        .mihomo()
        .nodes(MihomoNodeQuery {
            subscription_id: query.subscription_id,
            page: query.page.unwrap_or(1),
            page_size: PageSize::new(query.page_size.unwrap_or(20))
                .map_err(|_| AdminError::bad_request("分页大小不合法"))?,
            search: query.search.unwrap_or_default().trim().to_owned(),
        })
        .await
        .map_err(map_error)?;
    let total_pages = result.total.div_ceil(u64::from(result.page_size));
    Ok(AdminResponse::new(
        StatusCode::OK,
        AdminEnvelope::ok(NodePageView {
            items: result.items.into_iter().map(Into::into).collect(),
            page: PageMeta::new(
                result.page,
                u32::from(result.page_size),
                result.total,
                u32::try_from(total_pages).unwrap_or(u32::MAX),
            ),
        }),
    ))
}
