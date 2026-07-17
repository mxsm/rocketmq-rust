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

//! Compatibility exports for Proxy session contracts.

use rocketmq_remoting::net::channel::Channel;

pub use rocketmq_proxy_core::session::build_lite_subscription_sync_request;
pub use rocketmq_proxy_core::session::ClientSession;
pub use rocketmq_proxy_core::session::ClientSettingsSnapshot;
pub use rocketmq_proxy_core::session::LiteSubscriptionSnapshot;
pub use rocketmq_proxy_core::session::LiteSubscriptionSyncRequest;
pub use rocketmq_proxy_core::session::PendingLiteUnsubscribeNotice;
pub use rocketmq_proxy_core::session::PendingTelemetryCommand;
pub use rocketmq_proxy_core::session::PreparedTransactionHandle;
pub use rocketmq_proxy_core::session::PreparedTransactionRegistration;
pub use rocketmq_proxy_core::session::ReapSummary;
pub use rocketmq_proxy_core::session::ReceiptHandleRegistration;
pub use rocketmq_proxy_core::session::SubscriptionSettingsSnapshot;
pub use rocketmq_proxy_core::session::TelemetryCommandKind;
pub use rocketmq_proxy_core::session::TelemetryLink;
pub use rocketmq_proxy_core::session::ThreadStackTraceReport;
pub use rocketmq_proxy_core::session::TrackedReceiptHandle;
pub use rocketmq_proxy_core::session::VerifyMessageReport;

/// Legacy session registry specialization retaining the remoting Channel API.
pub type ClientSessionRegistry = rocketmq_proxy_core::session::ClientSessionRegistry<Channel>;
