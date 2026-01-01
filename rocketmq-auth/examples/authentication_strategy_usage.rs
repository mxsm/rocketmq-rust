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

//! # Authentication Strategy Usage Example
//!
//! This example demonstrates how to use the AuthenticationStrategy trait
//! and its implementations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_auth::authentication::context::default_authentication_context::DefaultAuthenticationContext;
use rocketmq_auth::authentication::strategy::AllowAllAuthenticationStrategy;
use rocketmq_auth::authentication::strategy::AuthenticationStrategy;
use rocketmq_error::AuthError;

/// Example 1: Using AllowAllAuthenticationStrategy
fn example_allow_all_strategy() {
    println!("=== Example 1: AllowAllAuthenticationStrategy ===");

    // Create the strategy
    let strategy = AllowAllAuthenticationStrategy::new();

    // Create an authentication context
    let mut context = DefaultAuthenticationContext::new();
    context.set_username(CheetahString::from("testuser"));

    // Authenticate - this will always succeed
    match strategy.authenticate(&context) {
        Ok(()) => println!("[OK] Authentication succeeded (as expected)"),
        Err(e) => println!("[FAIL] Authentication failed: {}", e),
    }

    println!();
}

/// Example 2: Using dynamic dispatch with trait objects
fn example_dynamic_dispatch() {
    println!("=== Example 2: Dynamic Dispatch ===");

    // Store strategies in a vector using trait objects
    let strategies: Vec<Arc<dyn AuthenticationStrategy>> = vec![
        Arc::new(AllowAllAuthenticationStrategy::new()),
        // In production, you would add other strategies here:
        // Arc::new(AkSkAuthenticationStrategy::new()),
        // Arc::new(TokenAuthenticationStrategy::new()),
    ];

    let context = DefaultAuthenticationContext::new();

    // Authenticate using each strategy
    for (i, strategy) in strategies.iter().enumerate() {
        match strategy.authenticate(&context) {
            Ok(()) => println!("[OK] Strategy {} succeeded", i + 1),
            Err(e) => println!("[FAIL] Strategy {} failed: {}", i + 1, e),
        }
    }

    println!();
}

/// Example 3: Custom authentication strategy
struct CustomAuthenticationStrategy {
    allowed_users: Vec<String>,
}

impl CustomAuthenticationStrategy {
    fn new(allowed_users: Vec<String>) -> Self {
        Self { allowed_users }
    }
}

impl AuthenticationStrategy for CustomAuthenticationStrategy {
    fn authenticate(
        &self,
        context: &dyn rocketmq_auth::authorization::context::authentication_context::AuthenticationContext,
    ) -> Result<(), AuthError> {
        // Downcast to DefaultAuthenticationContext
        let ctx = context
            .as_any()
            .downcast_ref::<DefaultAuthenticationContext>()
            .ok_or_else(|| AuthError::Other("Invalid context type".into()))?;

        // Check if username is in allowed list
        if let Some(username) = ctx.username() {
            if self.allowed_users.contains(&username.to_string()) {
                return Ok(());
            }
            return Err(AuthError::AuthenticationFailed(format!(
                "User '{}' is not in the allowed list",
                username
            )));
        }

        Err(AuthError::InvalidCredential("Missing username".into()))
    }
}

fn example_custom_strategy() {
    println!("=== Example 3: Custom Authentication Strategy ===");

    // Create a custom strategy with allowed users
    let strategy =
        CustomAuthenticationStrategy::new(vec!["alice".to_string(), "bob".to_string(), "charlie".to_string()]);

    // Test with allowed user
    let mut context1 = DefaultAuthenticationContext::new();
    context1.set_username(CheetahString::from("alice"));
    match strategy.authenticate(&context1) {
        Ok(()) => println!("[OK] User 'alice' authenticated successfully"),
        Err(e) => println!("[FAIL] User 'alice' failed: {}", e),
    }

    // Test with disallowed user
    let mut context2 = DefaultAuthenticationContext::new();
    context2.set_username(CheetahString::from("eve"));
    match strategy.authenticate(&context2) {
        Ok(()) => println!("[OK] User 'eve' authenticated successfully"),
        Err(e) => println!("[FAIL] User 'eve' failed: {}", e),
    }

    // Test with missing username
    let context3 = DefaultAuthenticationContext::new();
    match strategy.authenticate(&context3) {
        Ok(()) => println!("[OK] Empty username authenticated successfully"),
        Err(e) => println!("[FAIL] Empty username failed: {}", e),
    }

    println!();
}

/// Example 4: Strategy composition
fn example_strategy_composition() {
    println!("=== Example 4: Strategy Composition ===");

    // In a real application, you might have a chain of strategies
    let strategies: Vec<Arc<dyn AuthenticationStrategy>> = vec![
        Arc::new(AllowAllAuthenticationStrategy::new()),
        Arc::new(CustomAuthenticationStrategy::new(vec!["admin".to_string()])),
    ];

    let mut context = DefaultAuthenticationContext::new();
    context.set_username(CheetahString::from("admin"));

    // Try each strategy until one succeeds
    let mut authenticated = false;
    for strategy in &strategies {
        if strategy.authenticate(&context).is_ok() {
            authenticated = true;
            println!("[OK] Authenticated using a strategy");
            break;
        }
    }

    if !authenticated {
        println!("[FAIL] All strategies failed");
    }

    println!();
}

fn main() {
    println!("RocketMQ Rust - Authentication Strategy Examples\n");

    example_allow_all_strategy();
    example_dynamic_dispatch();
    example_custom_strategy();
    example_strategy_composition();

    println!("All examples completed!");
}
