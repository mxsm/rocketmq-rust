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

//! Example demonstrating the AuthorizationMetadataProvider usage.
//!
//! This example shows how to:
//! - Initialize a metadata provider
//! - Create and manage ACLs
//! - Query and filter ACLs
//! - Handle errors properly

use rocketmq_auth::authentication::enums::subject_type::SubjectType;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_auth::authorization::enums::decision::Decision;
use rocketmq_auth::authorization::enums::policy_type::PolicyType;
use rocketmq_auth::authorization::metadata_provider::AuthorizationMetadataProvider;
use rocketmq_auth::authorization::metadata_provider::NoopMetadataProvider;
use rocketmq_auth::authorization::model::acl::Acl;
use rocketmq_auth::authorization::model::environment::Environment;
use rocketmq_auth::authorization::model::policy::Policy;
use rocketmq_auth::authorization::model::policy_entry::PolicyEntry;
use rocketmq_auth::authorization::model::resource::Resource;
use rocketmq_auth::config::AuthConfig;
use rocketmq_common::common::action::Action;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== AuthorizationMetadataProvider Example ===\n");

    // 1. Initialize the provider
    let mut provider = NoopMetadataProvider::new();
    let config = AuthConfig::default();
    provider.initialize(config, None)?;
    println!("✓ Provider initialized\n");

    // 2. Create a sample ACL
    let acl = create_sample_acl();
    provider.create_acl(acl.clone()).await?;
    println!("✓ ACL created for user: user:alice\n");

    // 3. Query ACL by subject
    let user = User::of("alice");
    match provider.get_acl(&user).await? {
        Some(acl) => {
            println!("✓ Found ACL:");
            println!("  Subject: {}", acl.subject_key());
            println!("  Policies: {} policy(ies)\n", acl.policies().len());
        }
        None => println!("✗ ACL not found\n"),
    }

    // 4. List all ACLs
    let all_acls: Vec<Acl> = provider.list_acl(None, None).await?;
    println!("✓ Total ACLs: {}\n", all_acls.len());

    // 5. List ACLs with filters
    println!("Filtering examples:");

    // Filter by subject
    let user_acls: Vec<Acl> = provider.list_acl(Some("user:"), None).await?;
    println!("  - ACLs for users: {}", user_acls.len());

    // Filter by resource
    let topic_acls: Vec<Acl> = provider.list_acl(None, Some("Topic:")).await?;
    println!("  - ACLs for topics: {}", topic_acls.len());

    // Both filters
    let filtered: Vec<Acl> = provider.list_acl(Some("user:alice"), Some("Topic:orders")).await?;
    println!("  - ACLs for user:alice on Topic:orders: {}\n", filtered.len());

    // 6. Update ACL
    let mut updated_acl = acl;
    // Modify the ACL (e.g., add more policies)
    let new_resource = Resource::of_topic("new-topic");
    let new_entry = PolicyEntry::of(
        new_resource,
        vec![Action::Sub],
        Environment::of("10.0.0.0/8"),
        Decision::Allow,
    );
    let new_policy = Policy::of_entries(PolicyType::Custom, vec![new_entry]);
    updated_acl.update_policy(new_policy);

    provider.update_acl(updated_acl).await?;
    println!("✓ ACL updated\n");

    // 7. Delete ACL
    provider.delete_acl(&user).await?;
    println!("✓ ACL deleted\n");

    // 8. Shutdown
    provider.shutdown();
    println!("✓ Provider shut down\n");

    println!("=== Example completed successfully ===");
    Ok(())
}

/// Create a sample ACL for demonstration
fn create_sample_acl() -> Acl {
    // Define resources
    let order_topic = Resource::of_topic("orders-topic");
    let payment_topic = Resource::of_topic("payments");

    // Define actions
    let pub_actions = vec![Action::Pub];
    let sub_actions = vec![Action::Sub];

    // Define environment (source IPs)
    let internal_network = Environment::of("192.168.0.0/16");

    // Create policy entries
    let order_entry = PolicyEntry::of(
        order_topic,
        pub_actions.clone(),
        internal_network.clone(),
        Decision::Allow,
    );

    let payment_entry = PolicyEntry::of(
        payment_topic,
        sub_actions.clone(),
        internal_network.clone(),
        Decision::Allow,
    );

    // Create policy using of_entries (not of which takes resources Vec)
    let policy = Policy::of_entries(PolicyType::Custom, vec![order_entry, payment_entry]);

    // Create ACL
    Acl::of("user:alice", SubjectType::User, policy)
}
