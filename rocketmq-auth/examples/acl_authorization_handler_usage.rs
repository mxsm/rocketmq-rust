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

//! ACL Authorization Handler Usage Example
//!
//! This example demonstrates how to use the AclAuthorizationHandler
//! for ACL-based authorization in RocketMQ.

use std::sync::Arc;

use rocketmq_auth::authentication::enums::subject_type::SubjectType;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_auth::authorization::chain::AclAuthorizationHandler;
use rocketmq_auth::authorization::chain::AuthorizationHandlerChain;
use rocketmq_auth::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use rocketmq_auth::authorization::enums::decision::Decision;
use rocketmq_auth::authorization::metadata_provider::local::LocalAuthorizationMetadataProvider;
use rocketmq_auth::authorization::metadata_provider::AuthorizationMetadataProvider;
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::policy::Policy;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_auth::config::AuthConfig;
use rocketmq_common::common::action::Action;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== ACL Authorization Handler Example ===\n");

    // Step 1: Create and initialize a local authorization metadata provider
    println!("1. Creating local authorization metadata provider...");
    let mut provider = LocalAuthorizationMetadataProvider::new();
    provider.initialize(AuthConfig::default(), None)?;
    let provider = Arc::new(provider);

    // Step 2: Create ACLs for different users
    println!("2. Setting up ACLs for users...\n");

    // Alice: Allow publishing to topic "orders"
    let alice = User::of("alice");
    let alice_resource = Resource::of_topic("orders");
    let alice_policy = Policy::of(vec![alice_resource.clone()], vec![Action::Pub], None, Decision::Allow);
    let alice_acl = Acl::of_subject_and_policy(&alice, alice_policy);
    provider.create_acl(alice_acl).await?;
    println!("   ✓ Created ACL for Alice: ALLOW Pub on topic 'orders'");

    // Bob: Deny all actions on topic "secrets"
    let bob = User::of("bob");
    let bob_resource = Resource::of_topic("secrets");
    let bob_policy = Policy::of(vec![bob_resource.clone()], vec![Action::All], None, Decision::Deny);
    let bob_acl = Acl::of_subject_and_policy(&bob, bob_policy);
    provider.create_acl(bob_acl).await?;
    println!("   ✓ Created ACL for Bob: DENY All on topic 'secrets'");

    // Charlie: Allow subscribing to topic "notifications"
    let charlie = User::of("charlie");
    let charlie_resource = Resource::of_topic("notifications");
    let charlie_policy = Policy::of(vec![charlie_resource.clone()], vec![Action::Sub], None, Decision::Allow);
    let charlie_acl = Acl::of_subject_and_policy(&charlie, charlie_policy);
    provider.create_acl(charlie_acl).await?;
    println!("   ✓ Created ACL for Charlie: ALLOW Sub on topic 'notifications'\n");

    // Step 3: Create ACL authorization handler
    println!("3. Creating ACL authorization handler...");
    let acl_handler = Arc::new(AclAuthorizationHandler::new(provider.clone()));

    // Step 4: Create authorization chain (could add more handlers)
    println!("4. Setting up authorization handler chain...\n");
    let chain = AuthorizationHandlerChain::new().add_handler(acl_handler);

    // Step 5: Test authorization scenarios
    println!("5. Testing authorization scenarios:\n");

    // Scenario 1: Alice publishes to "orders" - Should SUCCEED
    println!("   Scenario 1: Alice publishing to 'orders'");
    let context1 = DefaultAuthorizationContext::of(
        "alice",
        SubjectType::User,
        alice_resource.clone(),
        Action::Pub,
        "192.168.1.100",
    );
    match chain.handle(&context1).await {
        Ok(()) => println!("      ✅ Authorization GRANTED\n"),
        Err(e) => println!("      ❌ Authorization DENIED: {}\n", e),
    }

    // Scenario 2: Alice subscribes to "orders" - Should FAIL (no SUB permission)
    println!("   Scenario 2: Alice subscribing to 'orders' (no SUB permission)");
    let context2 = DefaultAuthorizationContext::of(
        "alice",
        SubjectType::User,
        alice_resource.clone(),
        Action::Sub,
        "192.168.1.100",
    );
    match chain.handle(&context2).await {
        Ok(()) => println!("      ✅ Authorization GRANTED\n"),
        Err(e) => println!("      ❌ Authorization DENIED: {}\n", e),
    }

    // Scenario 3: Bob accesses "secrets" - Should FAIL (DENY policy)
    println!("   Scenario 3: Bob accessing 'secrets' (DENY policy)");
    let context3 =
        DefaultAuthorizationContext::of("bob", SubjectType::User, bob_resource.clone(), Action::Pub, "10.0.0.50");
    match chain.handle(&context3).await {
        Ok(()) => println!("      ✅ Authorization GRANTED\n"),
        Err(e) => println!("      ❌ Authorization DENIED: {}\n", e),
    }

    // Scenario 4: Charlie subscribes to "notifications" - Should SUCCEED
    println!("   Scenario 4: Charlie subscribing to 'notifications'");
    let context4 = DefaultAuthorizationContext::of(
        "charlie",
        SubjectType::User,
        charlie_resource.clone(),
        Action::Sub,
        "172.16.0.10",
    );
    match chain.handle(&context4).await {
        Ok(()) => println!("      ✅ Authorization GRANTED\n"),
        Err(e) => println!("      ❌ Authorization DENIED: {}\n", e),
    }

    // Scenario 5: Unknown user "dave" - Should FAIL (no ACL)
    println!("   Scenario 5: Unknown user 'dave' (no ACL configured)");
    let context5 = DefaultAuthorizationContext::of(
        "dave",
        SubjectType::User,
        Resource::of_topic("any-topic"),
        Action::Pub,
        "203.0.113.42",
    );
    match chain.handle(&context5).await {
        Ok(()) => println!("      ✅ Authorization GRANTED\n"),
        Err(e) => println!("      ❌ Authorization DENIED: {}\n", e),
    }

    println!("=== Example completed successfully ===");

    Ok(())
}
