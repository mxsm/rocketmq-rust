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

#[cfg(feature = "admin-full")]
mod capability;
pub mod default_mq_admin_ext;
pub mod default_mq_admin_ext_impl;
#[cfg(feature = "admin-mutation")]
mod mq_admin_mutation_ext;
#[cfg(feature = "admin-read")]
mod mq_admin_read_ext;

#[cfg(feature = "admin-full")]
pub use capability::AuthAdmin;
#[cfg(feature = "admin-full")]
pub use capability::BrokerAdmin;
#[cfg(feature = "admin-full")]
pub use capability::ConsumerAdmin;
#[cfg(feature = "admin-full")]
pub use capability::OffsetAdmin;
#[cfg(feature = "admin-full")]
pub use capability::RouteAdmin;
#[cfg(feature = "admin-full")]
pub use capability::TopicAdmin;
pub use default_mq_admin_ext::DefaultMQAdminExt;
pub use default_mq_admin_ext_impl::DefaultMQAdminExtImpl;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::BrokerConfigPatchOutcome;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::MQAdminMutationExt;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::MutationTopicConfigVersioned;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::SubscriptionGroupConfigPatch;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::SubscriptionGroupConfigPatchOutcome;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicConfigPatch;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicConfigPatchOutcome;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicOffsetMutationFailureCode;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicOffsetMutationOutcome;
#[cfg(feature = "admin-mutation")]
pub use mq_admin_mutation_ext::TopicOffsetMutationTargetOutcome;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::BrokerConfigAllowlisted;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::MQAdminMessageReadExt;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::MQAdminReadExt;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::MQAdminTopicInventoryReadExt;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::MessageMetadataRead;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::SubscriptionGroupConfigVersioned;
#[cfg(feature = "admin-read")]
pub use mq_admin_read_ext::TopicConfigVersioned;
