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

//! Integration tests for ControllerError with RocketMQError

use rocketmq_error::ControllerError;
use rocketmq_error::RocketMQError;

#[test]
fn test_controller_error_into_rocketmq_error() {
    let controller_err = ControllerError::NotLeader { leader_id: Some(1) };
    let rocketmq_err: RocketMQError = controller_err.into();

    assert!(matches!(rocketmq_err, RocketMQError::Controller(_)));
    assert!(rocketmq_err.to_string().contains("Not leader"));
}

#[test]
fn test_controller_error_from_conversion() {
    let controller_err = ControllerError::Timeout { timeout_ms: 5000 };
    let rocketmq_err = RocketMQError::from(controller_err);

    assert!(matches!(rocketmq_err, RocketMQError::Controller(_)));
    assert!(rocketmq_err.to_string().contains("5000"));
}

#[test]
fn test_controller_error_constructors() {
    // Test not leader constructor
    let err = RocketMQError::controller_not_leader(Some(3));
    assert!(matches!(
        err,
        RocketMQError::Controller(ControllerError::NotLeader { leader_id: Some(3) })
    ));

    // Test Raft error constructor
    let err = RocketMQError::controller_raft_error("consensus failed");
    assert!(matches!(err, RocketMQError::Controller(ControllerError::Raft(_))));
    assert!(err.to_string().contains("consensus failed"));

    // Test metadata not found constructor
    let err = RocketMQError::controller_metadata_not_found("broker-a");
    assert!(matches!(
        err,
        RocketMQError::Controller(ControllerError::MetadataNotFound { .. })
    ));
    assert!(err.to_string().contains("broker-a"));

    // Test invalid request constructor
    let err = RocketMQError::controller_invalid_request("missing field");
    assert!(matches!(
        err,
        RocketMQError::Controller(ControllerError::InvalidRequest(_))
    ));

    // Test timeout constructor
    let err = RocketMQError::controller_timeout(3000);
    assert!(matches!(
        err,
        RocketMQError::Controller(ControllerError::Timeout { timeout_ms: 3000 })
    ));

    // Test shutdown constructor
    let err = RocketMQError::controller_shutdown();
    assert!(matches!(err, RocketMQError::Controller(ControllerError::Shutdown)));
}

#[test]
fn test_controller_error_result_propagation() {
    fn returns_controller_error() -> Result<(), ControllerError> {
        Err(ControllerError::NotLeader { leader_id: None })
    }

    fn returns_rocketmq_error() -> Result<(), RocketMQError> {
        returns_controller_error()?;
        Ok(())
    }

    let result = returns_rocketmq_error();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, RocketMQError::Controller(_)));
}

#[test]
fn test_all_controller_error_variants_convert() {
    use std::io;

    let test_cases = vec![
        ControllerError::Io(io::Error::other("test")),
        ControllerError::Raft("raft error".to_string()),
        ControllerError::NotLeader { leader_id: Some(1) },
        ControllerError::MetadataNotFound {
            key: "test".to_string(),
        },
        ControllerError::InvalidRequest("invalid".to_string()),
        ControllerError::BrokerRegistrationFailed("failed".to_string()),
        ControllerError::NotInitialized("not init".to_string()),
        ControllerError::InitializationFailed,
        ControllerError::ConfigError("config error".to_string()),
        ControllerError::SerializationError("ser error".to_string()),
        ControllerError::StorageError("storage error".to_string()),
        ControllerError::NetworkError("network error".to_string()),
        ControllerError::Timeout { timeout_ms: 1000 },
        ControllerError::Internal("internal".to_string()),
        ControllerError::Shutdown,
    ];

    for controller_err in test_cases {
        let rocketmq_err: RocketMQError = controller_err.into();
        assert!(matches!(rocketmq_err, RocketMQError::Controller(_)));
    }
}
