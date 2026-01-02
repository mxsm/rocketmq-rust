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

//! Route management error types
//!
//! This module provides route-specific error handling that integrates
//! seamlessly with the unified RocketMQError system.
//!
//! All route errors can be automatically converted to RocketMQError
//! for consistent error handling across the codebase.

pub use rocketmq_error::RocketMQError;
pub use rocketmq_error::RocketMQResult;

/// Result type for route operations using unified error system
pub type RouteResult<T> = RocketMQResult<T>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_not_found_error() {
        let err = RocketMQError::BrokerNotFound {
            name: "DefaultCluster/broker-a".to_string(),
        };

        assert!(err.to_string().contains("broker-a"));
        assert!(err.to_string().contains("DefaultCluster"));
    }

    #[test]
    fn test_topic_not_exist() {
        let err = RocketMQError::TopicNotExist {
            topic: "TestTopic".to_string(),
        };

        match err {
            RocketMQError::TopicNotExist { topic } => {
                assert_eq!(topic, "TestTopic");
            }
            _ => panic!("Expected TopicNotExist error"),
        }
    }

    #[test]
    fn test_route_version_conflict() {
        let err = RocketMQError::RouteVersionConflict {
            expected: 100,
            actual: 50,
        };

        let err_str = err.to_string();
        assert!(err_str.contains("100"));
        assert!(err_str.contains("50"));
    }

    #[test]
    fn test_cluster_not_found() {
        let err = RocketMQError::cluster_not_found("TestCluster");

        assert!(err.to_string().contains("TestCluster"));
    }

    #[test]
    fn test_route_registration_conflict() {
        let err = RocketMQError::route_registration_conflict("broker-a", "duplicate broker ID");

        let err_str = err.to_string();
        assert!(err_str.contains("broker-a"));
        assert!(err_str.contains("duplicate"));
    }
}
