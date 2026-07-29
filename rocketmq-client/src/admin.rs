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

pub mod default_mq_admin_ext;
pub mod default_mq_admin_ext_impl;
#[cfg(feature = "admin-full")]
pub mod mq_admin_ext_async;
pub mod mq_admin_ext_async_inner;
pub mod mq_admin_ext_inner;
#[cfg(feature = "admin-mutation")]
mod mq_admin_mutation_ext;
#[cfg(feature = "admin-read")]
mod mq_admin_read_ext;

pub use default_mq_admin_ext::DefaultMQAdminExt;
pub use default_mq_admin_ext_impl::DefaultMQAdminExtImpl;
#[cfg(feature = "admin-full")]
pub use mq_admin_ext_async::MQAdminExt;
pub use mq_admin_ext_async_inner::MQAdminExtInnerImpl;
pub use mq_admin_ext_inner::MQAdminExtInner;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::BrokerConfigPatchOutcome;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::MQAdminMutationExt;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicConfigPatch;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicConfigPatchOutcome;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicConfigVersioned;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::BrokerConfigAllowlisted;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::MQAdminReadExt;
