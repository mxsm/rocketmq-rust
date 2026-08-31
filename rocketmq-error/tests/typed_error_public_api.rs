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

    for module in [
        "auth_error",
        "boundary",
        "cli",
        "context",
        "controller_error",
        "descriptor",
        "domain",
        "filter_error",
        "kind",
        "observability_error",
        "policy",
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
}
