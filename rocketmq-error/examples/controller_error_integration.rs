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

//! # ControllerError Integration Example
//!
//! This example demonstrates how to use ControllerError integrated with RocketMQError.
//!
//! ## Usage
//!
//! ```rust
//! use rocketmq_error::RocketMQError;
//! use rocketmq_error::RocketMQResult;
//!
//! // Function returning controller error
//! fn check_leader() -> RocketMQResult<()> {
//!     // Automatically converts ControllerError to RocketMQError
//!     Err(RocketMQError::controller_not_leader(Some(1)))
//! }
//!
//! // Function using convenient constructors
//! fn handle_request() -> RocketMQResult<String> {
//!     // Check if controller is leader
//!     if !is_leader() {
//!         return Err(RocketMQError::controller_not_leader(None));
//!     }
//!
//!     // Validate request
//!     if request_invalid() {
//!         return Err(RocketMQError::controller_invalid_request(
//!             "missing broker_name",
//!         ));
//!     }
//!
//!     // Check timeout
//!     if operation_timeout() {
//!         return Err(RocketMQError::controller_timeout(5000));
//!     }
//!
//!     Ok("Success".to_string())
//! }
//!
//! // Error propagation works seamlessly
//! fn process() -> RocketMQResult<()> {
//!     check_leader()?; // Propagates controller error
//!     handle_request()?; // Propagates any error
//!     Ok(())
//! }
//! ```
//!
//! ## Available Constructor Methods
//!
//! ```rust
//! use rocketmq_error::RocketMQError;
//!
//! // Not leader error
//! let err = RocketMQError::controller_not_leader(Some(3));
//!
//! // Raft consensus error
//! let err = RocketMQError::controller_raft_error("proposal timeout");
//!
//! // Metadata not found
//! let err = RocketMQError::controller_metadata_not_found("broker-a");
//!
//! // Invalid request
//! let err = RocketMQError::controller_invalid_request("missing field: broker_name");
//!
//! // Timeout error
//! let err = RocketMQError::controller_timeout(5000);
//!
//! // Shutdown error
//! let err = RocketMQError::controller_shutdown();
//! ```
//!
//! ## Pattern Matching
//!
//! ```rust
//! use rocketmq_error::ControllerError;
//! use rocketmq_error::RocketMQError;
//!
//! fn handle_error(err: RocketMQError) {
//!     match err {
//!         RocketMQError::Controller(ControllerError::NotLeader { leader_id }) => {
//!             if let Some(id) = leader_id {
//!                 println!("Redirect to leader: {}", id);
//!             } else {
//!                 println!("No leader elected yet");
//!             }
//!         }
//!         RocketMQError::Controller(ControllerError::Timeout { timeout_ms }) => {
//!             println!("Operation timed out after {}ms", timeout_ms);
//!         }
//!         RocketMQError::Controller(ControllerError::Shutdown) => {
//!             println!("Controller is shutting down");
//!         }
//!         _ => {
//!             println!("Other error: {}", err);
//!         }
//!     }
//! }
//! ```

use rocketmq_error::ControllerError;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

fn main() {
    println!("=== ControllerError Integration Example ===\n");

    // Example 1: Using constructor methods
    println!("1. Creating controller errors using constructor methods:");
    let err1 = RocketMQError::controller_not_leader(Some(1));
    println!("   Not leader: {}", err1);

    let err2 = RocketMQError::controller_timeout(5000);
    println!("   Timeout: {}", err2);

    let err3 = RocketMQError::controller_invalid_request("missing broker_name");
    println!("   Invalid request: {}", err3);

    // Example 2: Automatic conversion
    println!("\n2. Automatic conversion from ControllerError:");
    let controller_err = ControllerError::Raft("consensus failed".to_string());
    let rocketmq_err: RocketMQError = controller_err.into();
    println!("   Converted error: {}", rocketmq_err);

    // Example 3: Error propagation
    println!("\n3. Error propagation in functions:");
    match simulated_operation() {
        Ok(_) => println!("   Operation succeeded"),
        Err(e) => println!("   Operation failed: {}", e),
    }

    // Example 4: Pattern matching
    println!("\n4. Pattern matching on errors:");
    let errors = vec![
        RocketMQError::controller_not_leader(Some(2)),
        RocketMQError::controller_timeout(3000),
        RocketMQError::controller_shutdown(),
    ];

    for err in errors {
        print_error_details(err);
    }

    println!("\n=== Example completed successfully ===");
}

// Simulated operation that returns controller error
fn simulated_operation() -> RocketMQResult<()> {
    // Simulate a not leader scenario
    Err(RocketMQError::controller_not_leader(None))
}

// Helper function to print error details
fn print_error_details(err: RocketMQError) {
    match err {
        RocketMQError::Controller(ControllerError::NotLeader { leader_id }) => {
            println!("   - Not leader error, leader_id: {:?}", leader_id);
        }
        RocketMQError::Controller(ControllerError::Timeout { timeout_ms }) => {
            println!("   - Timeout error after {}ms", timeout_ms);
        }
        RocketMQError::Controller(ControllerError::Shutdown) => {
            println!("   - Controller shutdown error");
        }
        _ => {
            println!("   - Other error: {}", err);
        }
    }
}
