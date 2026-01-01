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

// Re-export the shared ControllerConfig from rocketmq-common so downstream code in
// this crate and tests can continue to refer to `crate::config::ControllerConfig`.
// Re-export common RaftPeer and StorageBackendType (from the controller_config module)
// so examples/tests using `rocketmq_controller::config::RaftPeer` or
// `StorageBackendType` keep working.
pub use rocketmq_common::common::controller::controller_config::RaftPeer;
pub use rocketmq_common::common::controller::controller_config::StorageBackendType;
pub use rocketmq_common::common::controller::ControllerConfig;

// Controller node-specific configuration is now carried by the shared
// `rocketmq_common::common::controller::ControllerConfig` type. Use that type
// in the controller crate where node-local fields are required.

// NOTE: crate-local node tests should construct node-aware configs via
// `rocketmq_common::common::controller::ControllerConfig::new_node(...)` or
// `::test_config()` helpers.
