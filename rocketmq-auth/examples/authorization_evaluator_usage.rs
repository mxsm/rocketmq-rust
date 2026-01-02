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

//! Authorization Evaluator Examples
//!
//! This example demonstrates how to use the AuthorizationEvaluator to make
//! authorization decisions in RocketMQ.

use rocketmq_auth::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use rocketmq_auth::authorization::evaluator::AuthorizationEvaluator;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_auth::authorization::strategy::stateless_authorization_strategy::StatelessAuthorizationStrategy;
use rocketmq_auth::config::AuthConfig;
use rocketmq_common::common::action::Action;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let config = AuthConfig::default();

    // Create a stateless authorization strategy
    let strategy = StatelessAuthorizationStrategy::new(config, None)?;

    // Create the evaluator with the strategy
    let evaluator = AuthorizationEvaluator::new(strategy);

    // Create authorization contexts
    let mut contexts = Vec::new();

    // Context 1: User "test" wants to publish to topic "test-topic"
    let mut context1 = DefaultAuthorizationContext::default();
    context1.set_resource(Resource::of_topic("test-topic"));
    context1.set_actions(vec![Action::Pub]);
    context1.set_source_ip("192.168.1.100");
    context1.set_rpc_code("10");
    contexts.push(context1);

    // Context 2: User "test" wants to subscribe from group "test-group"
    let mut context2 = DefaultAuthorizationContext::default();
    context2.set_resource(Resource::of_group("test-group".to_string()));
    context2.set_actions(vec![Action::Sub]);
    context2.set_source_ip("192.168.1.100");
    context2.set_rpc_code("11");
    contexts.push(context2);

    // Evaluate all contexts
    match evaluator.evaluate(&contexts) {
        Ok(()) => println!("✓ Authorization successful for all contexts"),
        Err(e) => println!("✗ Authorization failed: {}", e),
    }

    // Evaluate empty contexts (should succeed immediately)
    let empty_contexts: Vec<DefaultAuthorizationContext> = vec![];
    assert!(evaluator.evaluate(&empty_contexts).is_ok());
    println!("✓ Empty contexts evaluation succeeded (fast path)");

    Ok(())
}
