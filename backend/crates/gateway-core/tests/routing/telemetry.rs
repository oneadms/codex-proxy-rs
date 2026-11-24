//! 验证普通调用与内部探测冻结同一遥测设置

use super::*;

#[test]
fn telemetry_is_frozen_in_model_and_probe_routing_plans() {
    let snapshot = snapshot();
    let operation = operation();
    let provider = ProviderKind::new("openai").unwrap();
    let changed = snapshot
        .with_settings(
            &snapshot
                .settings()
                .clone()
                .with_openai_telemetry_enabled(false),
        )
        .unwrap();
    for (view, expected) in [(&snapshot, true), (&changed, false)] {
        let model = view
            .plan(
                &PublicModelId::new("gpt-5.5").unwrap(),
                &operation,
                view.all_account_scope(),
                &RoutingContext::default(),
            )
            .unwrap();
        let probe = view
            .plan_provider_endpoint(
                &provider,
                None,
                &operation,
                view.all_account_scope(),
                &RoutingContext::default(),
            )
            .unwrap();
        assert_eq!(model.openai_telemetry_enabled(), expected);
        assert_eq!(probe.openai_telemetry_enabled(), expected);
    }
    assert!(snapshot.settings().openai_telemetry_enabled());
}
