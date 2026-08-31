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

//! Deliberate stable Client entry points.

#[cfg(feature = "admin-full")]
pub use crate::admin::AuthAdmin;
#[cfg(feature = "admin-full")]
pub use crate::admin::BrokerAdmin;
#[cfg(feature = "admin-read")]
pub use crate::admin::BrokerConfigAllowlisted;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::BrokerConfigPatchOutcome;
#[cfg(feature = "admin-read")]
pub use crate::admin::BrokerReadFailure;
#[cfg(feature = "admin-read")]
pub use crate::admin::ConsumeStatsReadResult;
#[cfg(feature = "admin-full")]
pub use crate::admin::ConsumerAdmin;
pub use crate::admin::DefaultMQAdminExt;
pub use crate::admin::DefaultMQAdminExtImpl;
#[cfg(feature = "admin-read")]
pub use crate::admin::MQAdminMessageReadExt;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::MQAdminMutationExt;
#[cfg(feature = "admin-read")]
pub use crate::admin::MQAdminReadExt;
#[cfg(feature = "admin-read")]
pub use crate::admin::MQAdminTopicInventoryReadExt;
#[cfg(feature = "admin-read")]
pub use crate::admin::MQAdminTopicStatsReadExt;
#[cfg(feature = "admin-read")]
pub use crate::admin::MessageMetadataRead;
#[cfg(feature = "admin-full")]
pub use crate::admin::OffsetAdmin;
#[cfg(feature = "admin-read")]
pub use crate::admin::ReadFailureCode;
#[cfg(feature = "admin-full")]
pub use crate::admin::RouteAdmin;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::SubscriptionGroupConfigPatch;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::SubscriptionGroupConfigPatchOutcome;
#[cfg(feature = "admin-read")]
pub use crate::admin::SubscriptionGroupConfigVersioned;
#[cfg(feature = "admin-full")]
pub use crate::admin::TopicAdmin;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::TopicConfigPatch;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::TopicConfigPatchOutcome;
#[cfg(feature = "admin-read")]
pub use crate::admin::TopicConfigVersioned;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::TopicOffsetMutationFailureCode;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::TopicOffsetMutationOutcome;
#[cfg(feature = "admin-mutation")]
pub use crate::admin::TopicOffsetMutationTargetOutcome;
pub use crate::base::client_config::ClientConfig;
pub use crate::base::client_options::ClientOptions;
pub use crate::cluster_session::{
    client_config_for_managed_domain, rpc_hook_from_outbound_signer, ClientInstanceHandle, ClientRpcHook,
};
pub use crate::consumer::AssignmentControl;
pub use crate::consumer::ConsumerLifecycle;
pub use crate::consumer::ConsumerOffsetControl;
pub use crate::consumer::DefaultLitePullConsumer;
pub use crate::consumer::DefaultMQPushConsumer;
pub use crate::consumer::MessagePoll;
pub use crate::consumer::SubscriptionControl;
pub use crate::nameserver_discovery::DnsName;
pub use crate::nameserver_discovery::NameServerAuthority;
pub use crate::nameserver_discovery::NameServerDiscoveryConfig;
pub use crate::nameserver_discovery::NameServerDiscoveryErrorCategory;
pub use crate::nameserver_discovery::NameServerDiscoveryFreshness;
pub use crate::nameserver_discovery::NameServerDiscoverySourceKind;
pub use crate::nameserver_discovery::NameServerDiscoveryStatus;
pub use crate::nameserver_discovery::NameServerSource;
pub use crate::nameserver_discovery::ResolvedNameServerEndpoint;
pub use crate::producer::DefaultMQProducer;
pub use crate::producer::MessageQuery;
pub use crate::producer::MessageRecall;
pub use crate::producer::MessageSend;
pub use crate::producer::ProducerConfig;
pub use crate::producer::ProducerTopicAdmin;
pub use crate::producer::RequestReply;
pub use crate::producer::TransactionSend;
pub use crate::session::ClientSession;
pub use crate::session::ClientSessionProvider;
