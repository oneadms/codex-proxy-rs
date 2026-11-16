//! 验证 Mihomo 管理接口的身份和请求边界

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt as _;

use super::{AdminTestFixture, AdminTestState};

async fn request(
    fixture: &AdminTestFixture,
    path: &str,
    body: Option<Value>,
    authenticated: bool,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .header("x-request-id", "req_mihomo_tests")
        .uri(path)
        .method(if body.is_some() { "POST" } else { "GET" });
    if authenticated {
        builder = builder.header(header::COOKIE, "cpr_session=valid-session");
    }
    let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
    let response = gateway_api::admin::mihomo::router::<AdminTestState>()
        .with_state(fixture.state())
        .oneshot(
            builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn mihomo_admin_routes_require_auth_and_validate_input() {
    let fixture = AdminTestFixture::new().await;
    fixture.auth.insert_session("valid-session");
    for (path, body) in [
        ("/api/admin/mihomo/subscriptions", None),
        (
            "/api/admin/mihomo/subscriptions/create",
            Some(json!({"name":"Test"})),
        ),
        (
            "/api/admin/mihomo/subscriptions/update",
            Some(json!({"id":"subscription-test","revision":1})),
        ),
        (
            "/api/admin/mihomo/subscriptions/refresh",
            Some(json!({"id":"subscription-test","revision":1})),
        ),
        (
            "/api/admin/mihomo/subscriptions/delete",
            Some(json!({"id":"subscription-test","revision":1})),
        ),
        ("/api/admin/mihomo/nodes?subscriptionId=test", None),
    ] {
        assert_eq!(
            request(&fixture, path, body, false).await.0,
            StatusCode::UNAUTHORIZED,
            "{path}"
        );
    }

    assert_eq!(
        request(&fixture, "/api/admin/mihomo/nodes?page=0", None, true)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(
            &fixture,
            "/api/admin/mihomo/subscriptions/create",
            Some(json!({"name":"Test"})),
            true
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}
