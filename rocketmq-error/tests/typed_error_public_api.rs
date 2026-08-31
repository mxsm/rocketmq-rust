// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[test]
fn error_crate_public_api_exposes_only_typed_error_surface() {
    let source = include_str!("../src/lib.rs");
    let context_source = include_str!("../src/context.rs");
    let removed_symbols = [
        concat!("Legacy", "RocketMQResult"),
        concat!("pub enum Rocket", "mqError"),
        concat!("pub struct MQBroker", "Err"),
        concat!("pub struct Client", "Err"),
        concat!("pub struct RequestTimeout", "Err"),
        concat!("macro_rules! mq_client_err", "_legacy"),
        concat!("pub enum Legacy", "ServiceError"),
    ];

    for symbol in removed_symbols {
        assert!(
            !source.contains(symbol),
            "`rocketmq-error` should not expose legacy error surface symbol `{symbol}`"
        );
    }

    for removed_builder in ["with_field", "push_field", "with_sensitive", "push_sensitive"] {
        assert!(
            !context_source.contains(removed_builder),
            "removed builder {removed_builder}"
        );
    }
    assert!(!source.contains("RedactionKind"));
    assert!(!context_source.contains("pub fn fields("));
    assert!(context_source.contains("pub fn public_fields("));
    assert!(context_source.contains("with_secret_presence(mut self, key: FieldKey<SecretPresenceField>)"));

    for module in [
        "auth_error",
        "boundary",
        "catalog",
        "cli",
        "context",
        "controller_error",
        "descriptor",
        "domain",
        "field",
        "filter_error",
        "kind",
        "observability_error",
        "policy",
        "projection",
        "recovery",
        "spec",
        "unified",
    ] {
        assert!(
            !source.contains(&format!("pub mod {module};")),
            "`rocketmq-error` implementation module `{module}` must remain private"
        );
    }

    fn accepts_domain_error(_: &dyn rocketmq_error::DomainError) {}
    accepts_domain_error(&rocketmq_error::RocketMQError::invariant_violated(
        "public root contract compiles",
    ));

    let condition = rocketmq_error::CanonicalCondition::Unavailable;
    let recovery = rocketmq_error::RecoveryHint::Backoff;
    assert_eq!(condition.as_str(), "unavailable");
    assert_eq!(recovery.as_str(), "backoff");

    let descriptor: &'static rocketmq_error::ErrorDescriptor =
        rocketmq_error::descriptor_by_code("route.topic.not_found").expect("catalog descriptor");
    let projection: rocketmq_error::ProjectionSpec = descriptor.projection();
    assert_eq!(descriptor, &rocketmq_error::ROUTE_TOPIC_NOT_FOUND);
    assert_eq!(projection.grpc().status, rocketmq_error::GrpcStatusCode::NotFound);
}
