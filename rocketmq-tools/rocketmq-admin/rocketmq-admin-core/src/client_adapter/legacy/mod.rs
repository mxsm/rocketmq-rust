// Copyright 2026 The RocketMQ Rust Authors
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

//! R0 compatibility implementation. New code should use the contract traits.

pub(crate) use rocketmq_client_rust as client_sdk;
pub use rocketmq_client_rust::MQAdminExt as LegacyMQAdminExt;
pub(crate) use rocketmq_common as common_sdk;

#[doc(hidden)]
pub mod compat_types {
    pub use rocketmq_client_rust::base::client_config::ClientConfig;
    pub use rocketmq_common::common::config::TopicConfig;
}

#[doc(hidden)]
pub mod error_compat {
    pub use rocketmq_client_rust::admin_adapter_compat::error::*;
}

#[doc(hidden)]
pub mod remoting_compat {
    pub use rocketmq_client_rust::admin_adapter_compat::remoting::*;
}

pub(crate) mod admin {
    pub(crate) mod mq_admin_utils;
}

pub(crate) mod core;
