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

//! Example demonstrating the usage of AuthenticationMetadataManager.
//!
//! This example shows:
//! - Creating and configuring the manager
//! - Initializing default users from configuration
//! - CRUD operations on users
//! - Checking super user privileges

use cheetah_string::CheetahString;
use rocketmq_auth::authentication::enums::user_type::UserType;
use rocketmq_auth::authentication::manager::AuthenticationMetadataManager;
use rocketmq_auth::authentication::manager::AuthenticationMetadataManagerImpl;
use rocketmq_auth::authentication::model::user::User;
use rocketmq_auth::authentication::provider::local_authentication_metadata_provider::LocalAuthenticationMetadataProvider;
use rocketmq_auth::authentication::provider::AuthenticationMetadataProvider;
use rocketmq_auth::authorization::metadata_provider::local::LocalAuthorizationMetadataProvider;
use rocketmq_auth::config::AuthConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== Authentication Metadata Manager Example ===\n");

    // 1. Create providers
    println!("1. Creating authentication and authorization providers...");
    let mut auth_provider = LocalAuthenticationMetadataProvider::new();
    let config_for_init = AuthConfig::default();
    auth_provider.initialize(config_for_init, None).await?;

    let authz_provider = LocalAuthorizationMetadataProvider::new();

    // 2. Create manager with providers
    println!("2. Creating AuthenticationMetadataManager...");
    let manager = AuthenticationMetadataManagerImpl::new(Some(auth_provider), Some(authz_provider));

    // 3. Initialize default users from configuration
    println!("\n3. Initializing default users from configuration...");

    // Example 1: Simple "username:password" format
    let config = AuthConfig {
        init_authentication_user: CheetahString::from("admin:admin123"),
        ..Default::default()
    };
    manager.init_user(&config).await?;
    println!("   ✓ Created super user 'admin' from simple format");

    // Example 2: JSON format (same as Java)
    let config = AuthConfig {
        init_authentication_user: CheetahString::from(
            r#"{"username":"superadmin","password":"superpass","userType":"SUPER"}"#,
        ),
        ..Default::default()
    };
    manager.init_user(&config).await?;
    println!("   ✓ Created super user 'superadmin' from JSON format");

    // Example 3: SessionCredentials format (inner client)
    let config = AuthConfig {
        inner_client_authentication_credentials: CheetahString::from(
            r#"{"accessKey":"innerClient","secretKey":"innerSecret"}"#,
        ),
        ..Default::default()
    };
    manager.init_user(&config).await?;
    println!("   ✓ Created inner client user from SessionCredentials format");

    // 4. Create a normal user
    println!("\n4. Creating a normal user...");
    let normal_user = User::of_with_type("alice", "password123", UserType::Normal);
    manager.create_user(normal_user).await?;
    println!("   ✓ User 'alice' created");

    // 5. Retrieve user
    println!("\n5. Retrieving user information...");
    let retrieved_user: User = manager.get_user("alice").await?;
    println!("   Username: {}", retrieved_user.username());
    println!("   User Type: {:?}", retrieved_user.user_type());
    println!("   User Status: {:?}", retrieved_user.user_status());

    // 6. Update user
    println!("\n6. Updating user...");
    let mut updated_user = User::of("alice");
    updated_user.set_password(CheetahString::from("new_password456"));
    manager.update_user(updated_user).await?;
    println!("   ✓ User 'alice' password updated");

    // 7. List all users
    println!("\n7. Listing all users...");
    let all_users: Vec<User> = manager.list_user(None).await?;
    println!("   Total users: {}", all_users.len());
    for user in &all_users {
        let user_type = user
            .user_type()
            .map(|t| format!("{:?}", t))
            .unwrap_or_else(|| "None".to_string());
        println!("   - {} (Type: {})", user.username(), user_type);
    }

    // 8. Check super user privileges
    println!("\n8. Checking super user privileges...");
    for username in &["admin", "alice", "nonexistent"] {
        let is_super = manager.is_super_user(username).await?;
        println!("   '{}' is super user: {}", username, is_super);
    }

    // 9. Delete user (also deletes related ACLs)
    println!("\n9. Deleting user and related ACLs...");
    manager.delete_user("alice").await?;
    println!("   ✓ User 'alice' and related ACLs deleted");

    // Verify deletion
    let remaining_users: Vec<User> = manager.list_user(None).await?;
    println!("   Remaining users: {}", remaining_users.len());

    println!("\n=== Example completed successfully! ===");
    Ok(())
}
