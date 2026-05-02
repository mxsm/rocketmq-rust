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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use dashmap::DashMap;
use rocketmq_common::get_parent_and_lite_topic;
use rocketmq_common::to_lmq_name;
use rocketmq_remoting::net::channel::Channel;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::context::ProxyContext;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::proto::v2;
use crate::service::ResourceIdentity;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionSettingsSnapshot {
    pub group: Option<ResourceIdentity>,
    pub fifo: bool,
    pub receive_batch_size: Option<u32>,
    pub long_polling_timeout: Option<Duration>,
    pub lite_subscription_quota: Option<u32>,
    pub max_lite_topic_size: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSettingsSnapshot {
    pub client_type: Option<i32>,
    pub request_timeout: Option<Duration>,
    pub subscription: Option<SubscriptionSettingsSnapshot>,
}

impl ClientSettingsSnapshot {
    pub fn from_proto(settings: &v2::Settings) -> Self {
        let subscription = match settings.pub_sub.as_ref() {
            Some(v2::settings::PubSub::Subscription(subscription)) => Some(SubscriptionSettingsSnapshot {
                group: subscription.group.as_ref().map(resource_identity),
                fifo: subscription.fifo.unwrap_or(false),
                receive_batch_size: subscription
                    .receive_batch_size
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| *value > 0),
                long_polling_timeout: subscription.long_polling_timeout.as_ref().and_then(proto_duration),
                lite_subscription_quota: subscription
                    .lite_subscription_quota
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| *value > 0),
                max_lite_topic_size: subscription
                    .max_lite_topic_size
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| *value > 0),
            }),
            _ => None,
        };

        Self {
            client_type: settings.client_type,
            request_timeout: settings.request_timeout.as_ref().and_then(proto_duration),
            subscription,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSession {
    pub client_id: String,
    pub remote_addr: Option<String>,
    pub namespace: Option<String>,
    pub language: Option<String>,
    pub client_version: Option<String>,
    pub connection_id: Option<String>,
    pub client_type: Option<i32>,
    pub producer_groups: BTreeSet<String>,
    pub consumer_groups: BTreeSet<String>,
    pub settings: Option<ClientSettingsSnapshot>,
    pub last_seen: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ReceiptHandleRegistration {
    pub client_id: String,
    pub group: ResourceIdentity,
    pub topic: ResourceIdentity,
    pub message_id: String,
    pub receipt_handle: String,
    pub invisible_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct TrackedReceiptHandle {
    pub client_id: String,
    pub group: ResourceIdentity,
    pub topic: ResourceIdentity,
    pub message_id: String,
    pub receipt_handle: String,
    pub invisible_duration: Duration,
    pub last_touched: SystemTime,
    pub next_renew_at: SystemTime,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiteSubscriptionSyncRequest {
    pub action: v2::LiteSubscriptionAction,
    pub topic: ResourceIdentity,
    pub group: ResourceIdentity,
    pub lite_topic_set: BTreeSet<String>,
    pub version: Option<i64>,
    pub offset_option: Option<v2::OffsetOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiteSubscriptionSnapshot {
    pub client_id: String,
    pub topic: ResourceIdentity,
    pub group: ResourceIdentity,
    pub lite_topic_set: BTreeSet<String>,
    pub version: Option<i64>,
    pub offset_option: Option<v2::OffsetOption>,
    pub last_touched: SystemTime,
}

#[derive(Debug, Clone)]
pub struct PreparedTransactionRegistration {
    pub client_id: String,
    pub topic: ResourceIdentity,
    pub message_id: String,
    pub transaction_id: String,
    pub producer_group: String,
    pub transaction_state_table_offset: u64,
    pub commit_log_message_id: String,
    pub message: v2::Message,
    pub orphaned_transaction_recovery_duration: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedTransactionHandle {
    pub client_id: String,
    pub topic: ResourceIdentity,
    pub message_id: String,
    pub transaction_id: String,
    pub producer_group: String,
    pub transaction_state_table_offset: u64,
    pub commit_log_message_id: String,
    pub message: v2::Message,
    pub orphaned_transaction_recovery_duration: Option<Duration>,
    pub last_touched: SystemTime,
}

#[derive(Clone)]
pub struct TelemetryLink {
    pub sender: mpsc::UnboundedSender<v2::TelemetryCommand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TelemetryCommandKind {
    ReconnectEndpoints,
    PrintThreadStackTrace,
    VerifyMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTelemetryCommand {
    pub client_id: String,
    pub kind: TelemetryCommandKind,
    pub nonce: String,
    pub requested_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadStackTraceReport {
    pub client_id: String,
    pub nonce: String,
    pub thread_stack_trace: Option<String>,
    pub reported_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyMessageReport {
    pub client_id: String,
    pub nonce: String,
    pub reported_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingLiteUnsubscribeNotice {
    pub client_id: String,
    pub lite_topic: String,
    pub requested_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReapSummary {
    pub removed_sessions: usize,
    pub removed_receipt_handles: usize,
    pub removed_lite_subscriptions: usize,
    pub removed_prepared_transactions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ReceiptHandleKey {
    client_id: String,
    group: ResourceIdentity,
    topic: ResourceIdentity,
    message_id: String,
}

impl ReceiptHandleKey {
    fn new(
        client_id: impl Into<String>,
        group: ResourceIdentity,
        topic: ResourceIdentity,
        message_id: impl Into<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            group,
            topic,
            message_id: message_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LiteSubscriptionKey {
    client_id: String,
    group: ResourceIdentity,
    topic: ResourceIdentity,
}

impl LiteSubscriptionKey {
    fn new(client_id: impl Into<String>, group: ResourceIdentity, topic: ResourceIdentity) -> Self {
        Self {
            client_id: client_id.into(),
            group,
            topic,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PreparedTransactionKey {
    client_id: String,
    transaction_id: String,
}

impl PreparedTransactionKey {
    fn new(client_id: impl Into<String>, transaction_id: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            transaction_id: transaction_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TelemetryCommandKey {
    client_id: String,
    kind: TelemetryCommandKind,
    nonce: String,
}

impl TelemetryCommandKey {
    fn new(client_id: impl Into<String>, kind: TelemetryCommandKind, nonce: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            kind,
            nonce: nonce.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ThreadStackTraceKey {
    client_id: String,
    nonce: String,
}

impl ThreadStackTraceKey {
    fn new(client_id: impl Into<String>, nonce: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            nonce: nonce.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LiteUnsubscribeNoticeKey {
    client_id: String,
    lite_topic: String,
}

impl LiteUnsubscribeNoticeKey {
    fn new(client_id: impl Into<String>, lite_topic: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            lite_topic: lite_topic.into(),
        }
    }
}

impl From<&TrackedReceiptHandle> for ReceiptHandleKey {
    fn from(value: &TrackedReceiptHandle) -> Self {
        Self::new(
            value.client_id.clone(),
            value.group.clone(),
            value.topic.clone(),
            value.message_id.clone(),
        )
    }
}

#[derive(Clone, Default)]
pub struct ClientSessionRegistry {
    sessions: Arc<DashMap<String, ClientSession>>,
    receipt_handles: Arc<DashMap<ReceiptHandleKey, TrackedReceiptHandle>>,
    lite_subscriptions: Arc<DashMap<LiteSubscriptionKey, LiteSubscriptionSnapshot>>,
    prepared_transactions: Arc<DashMap<PreparedTransactionKey, PreparedTransactionHandle>>,
    telemetry_links: Arc<DashMap<String, TelemetryLink>>,
    remoting_channels: Arc<DashMap<String, Channel>>,
    pending_telemetry_commands: Arc<DashMap<TelemetryCommandKey, PendingTelemetryCommand>>,
    thread_stack_traces: Arc<DashMap<ThreadStackTraceKey, ThreadStackTraceReport>>,
    verify_message_reports: Arc<DashMap<TelemetryCommandKey, VerifyMessageReport>>,
    pending_lite_unsubscribe_notices: Arc<DashMap<LiteUnsubscribeNoticeKey, PendingLiteUnsubscribeNotice>>,
}

impl ClientSessionRegistry {
    pub fn upsert_from_context(&self, context: &ProxyContext) {
        self.upsert_from_context_with_client_type(context, None);
    }

    pub fn upsert_from_context_with_client_type(&self, context: &ProxyContext, client_type: Option<i32>) {
        let Some(client_id) = context.client_id() else {
            return;
        };

        self.upsert_client_identity(client_id, context, client_type);
    }

    pub fn update_membership_from_remoting_heartbeat(
        &self,
        context: &ProxyContext,
        client_id: &str,
        producer_groups: BTreeSet<String>,
        consumer_groups: BTreeSet<String>,
    ) -> bool {
        self.upsert_client_identity(client_id, context, None);

        let mut changed = false;
        if let Some(mut session) = self.sessions.get_mut(client_id) {
            changed = session.producer_groups != producer_groups || session.consumer_groups != consumer_groups;
            session.producer_groups = producer_groups;
            session.consumer_groups = consumer_groups;
            session.last_seen = SystemTime::now();
        }
        changed
    }

    pub fn unregister_client_groups(
        &self,
        client_id: &str,
        producer_group: Option<&str>,
        consumer_group: Option<&str>,
    ) -> Option<ClientSession> {
        if producer_group.is_none() && consumer_group.is_none() {
            return self.remove_client(client_id);
        }

        let mut snapshot = None;
        if let Some(mut session) = self.sessions.get_mut(client_id) {
            if let Some(producer_group) = producer_group {
                session.producer_groups.remove(producer_group);
            }
            if let Some(consumer_group) = consumer_group {
                session.consumer_groups.remove(consumer_group);
            }
            session.last_seen = SystemTime::now();
            snapshot = Some(session.clone());
        }
        snapshot
    }

    pub fn consumer_client_ids(&self, consumer_group: &str) -> Vec<String> {
        let mut client_ids = self
            .sessions
            .iter()
            .filter(|entry| entry.consumer_groups.contains(consumer_group))
            .map(|entry| entry.client_id.clone())
            .collect::<Vec<_>>();
        client_ids.sort_unstable();
        client_ids
    }

    fn upsert_client_identity(&self, client_id: &str, context: &ProxyContext, client_type: Option<i32>) {
        let now = SystemTime::now();
        if let Some(mut session) = self.sessions.get_mut(client_id) {
            session.remote_addr = context.remote_addr().map(str::to_owned);
            session.namespace = context.namespace().map(str::to_owned);
            session.language = context.language().map(str::to_owned);
            session.client_version = context.client_version().map(str::to_owned);
            session.connection_id = context.connection_id().map(str::to_owned);
            if let Some(client_type) = client_type {
                session.client_type = Some(client_type);
            }
            session.last_seen = now;
            return;
        }

        self.sessions.insert(
            client_id.to_owned(),
            ClientSession {
                client_id: client_id.to_owned(),
                remote_addr: context.remote_addr().map(str::to_owned),
                namespace: context.namespace().map(str::to_owned),
                language: context.language().map(str::to_owned),
                client_version: context.client_version().map(str::to_owned),
                connection_id: context.connection_id().map(str::to_owned),
                client_type,
                producer_groups: BTreeSet::new(),
                consumer_groups: BTreeSet::new(),
                settings: None,
                last_seen: now,
            },
        );
    }

    pub fn update_settings_from_telemetry(
        &self,
        context: &ProxyContext,
        settings: &v2::Settings,
    ) -> Option<ClientSettingsSnapshot> {
        let client_id = context.client_id()?;
        let snapshot = ClientSettingsSnapshot::from_proto(settings);
        self.upsert_from_context_with_client_type(context, snapshot.client_type);

        if let Some(mut session) = self.sessions.get_mut(client_id) {
            session.client_type = snapshot.client_type.or(session.client_type);
            session.settings = Some(snapshot.clone());
            session.last_seen = SystemTime::now();
        }

        Some(snapshot)
    }

    pub fn settings_for_client(&self, client_id: &str) -> Option<ClientSettingsSnapshot> {
        self.sessions.get(client_id).and_then(|entry| entry.settings.clone())
    }

    pub fn sync_lite_subscription(
        &self,
        client_id: &str,
        request: LiteSubscriptionSyncRequest,
        settings: Option<&ClientSettingsSnapshot>,
    ) -> ProxyResult<LiteSubscriptionSnapshot> {
        validate_lite_subscription_request(&request, settings)?;

        let key = LiteSubscriptionKey::new(client_id.to_owned(), request.group.clone(), request.topic.clone());
        let mut current = self
            .lite_subscriptions
            .get(&key)
            .map(|entry| entry.clone())
            .unwrap_or_else(|| LiteSubscriptionSnapshot {
                client_id: client_id.to_owned(),
                topic: request.topic.clone(),
                group: request.group.clone(),
                lite_topic_set: BTreeSet::new(),
                version: None,
                offset_option: None,
                last_touched: SystemTime::now(),
            });

        match request.action {
            v2::LiteSubscriptionAction::PartialAdd => {
                let mut merged = current.lite_topic_set.clone();
                merged.extend(request.lite_topic_set.iter().cloned());
                ensure_lite_subscription_quota(merged.len(), settings)?;
                current.lite_topic_set = merged;
            }
            v2::LiteSubscriptionAction::PartialRemove => {
                for lite_topic in &request.lite_topic_set {
                    current.lite_topic_set.remove(lite_topic);
                }
            }
            v2::LiteSubscriptionAction::CompleteAdd => {
                ensure_lite_subscription_quota(request.lite_topic_set.len(), settings)?;
                current.lite_topic_set = request.lite_topic_set.clone();
            }
            v2::LiteSubscriptionAction::CompleteRemove => {
                current.lite_topic_set.clear();
            }
        }

        for lite_topic in &request.lite_topic_set {
            if !current.lite_topic_set.contains(lite_topic) {
                let _ = self.remove_pending_lite_unsubscribe_notice(client_id, lite_topic);
            }
        }

        current.version = request.version;
        current.offset_option = request.offset_option;
        current.last_touched = SystemTime::now();

        if current.lite_topic_set.is_empty() {
            self.lite_subscriptions.remove(&key);
        } else {
            self.lite_subscriptions.insert(key, current.clone());
        }

        Ok(current)
    }

    pub fn lite_subscription(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
    ) -> Option<LiteSubscriptionSnapshot> {
        self.lite_subscriptions
            .get(&LiteSubscriptionKey::new(
                client_id.to_owned(),
                group.clone(),
                topic.clone(),
            ))
            .map(|entry| entry.clone())
    }

    pub fn lite_subscriptions_for_topic(&self, topic: &ResourceIdentity) -> Vec<LiteSubscriptionSnapshot> {
        self.lite_subscriptions
            .iter()
            .filter(|entry| entry.value().topic == *topic)
            .map(|entry| entry.clone())
            .collect()
    }

    pub fn all_lite_subscriptions(&self) -> Vec<LiteSubscriptionSnapshot> {
        self.lite_subscriptions.iter().map(|entry| entry.clone()).collect()
    }

    pub fn lite_subscriptions_for_topic_and_lite(
        &self,
        topic: &ResourceIdentity,
        lite_topic: &str,
    ) -> Vec<LiteSubscriptionSnapshot> {
        self.lite_subscriptions
            .iter()
            .filter(|entry| {
                let snapshot = entry.value();
                snapshot.topic == *topic && snapshot.lite_topic_set.contains(lite_topic)
            })
            .map(|entry| entry.clone())
            .collect()
    }

    pub fn lite_subscriptions_for_group_and_lite(
        &self,
        group: &ResourceIdentity,
        lite_topic: &str,
    ) -> Vec<LiteSubscriptionSnapshot> {
        self.lite_subscriptions
            .iter()
            .filter(|entry| {
                let snapshot = entry.value();
                snapshot.group == *group && snapshot.lite_topic_set.contains(lite_topic)
            })
            .map(|entry| entry.clone())
            .collect()
    }

    pub fn remove_lite_topic(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        lite_topic: &str,
    ) -> Option<LiteSubscriptionSnapshot> {
        let key = LiteSubscriptionKey::new(client_id.to_owned(), group.clone(), topic.clone());
        let mut snapshot = self.lite_subscriptions.get_mut(&key)?;
        snapshot.lite_topic_set.remove(lite_topic);
        snapshot.last_touched = SystemTime::now();
        let result = snapshot.clone();
        let remove_entry = snapshot.lite_topic_set.is_empty();
        drop(snapshot);
        let _ = self.remove_pending_lite_unsubscribe_notice(client_id, lite_topic);
        if remove_entry {
            self.lite_subscriptions.remove(&key);
        }
        Some(result)
    }

    pub fn track_prepared_transaction(
        &self,
        registration: PreparedTransactionRegistration,
    ) -> PreparedTransactionHandle {
        let tracked = PreparedTransactionHandle {
            client_id: registration.client_id.clone(),
            topic: registration.topic,
            message_id: registration.message_id,
            transaction_id: registration.transaction_id.clone(),
            producer_group: registration.producer_group,
            transaction_state_table_offset: registration.transaction_state_table_offset,
            commit_log_message_id: registration.commit_log_message_id,
            message: registration.message,
            orphaned_transaction_recovery_duration: registration.orphaned_transaction_recovery_duration,
            last_touched: SystemTime::now(),
        };
        self.prepared_transactions.insert(
            PreparedTransactionKey::new(registration.client_id, registration.transaction_id),
            tracked.clone(),
        );
        tracked
    }

    pub fn bind_telemetry_link(
        &self,
        client_id: impl Into<String>,
        sender: mpsc::UnboundedSender<v2::TelemetryCommand>,
    ) {
        let client_id = client_id.into();
        self.telemetry_links.insert(client_id.clone(), TelemetryLink { sender });
        self.clear_pending_telemetry_commands_of_kind(client_id.as_str(), TelemetryCommandKind::ReconnectEndpoints);
    }

    pub fn send_telemetry_command(&self, client_id: &str, command: v2::TelemetryCommand) -> bool {
        let Some(link) = self.telemetry_links.get(client_id) else {
            return false;
        };

        if link.sender.send(command).is_ok() {
            true
        } else {
            drop(link);
            self.telemetry_links.remove(client_id);
            false
        }
    }

    pub fn unbind_telemetry_link(&self, client_id: &str) -> bool {
        self.telemetry_links.remove(client_id).is_some()
    }

    pub fn has_telemetry_link(&self, client_id: &str) -> bool {
        self.telemetry_links.contains_key(client_id)
    }

    pub fn bind_remoting_channel(&self, client_id: impl Into<String>, channel: Channel) {
        self.remoting_channels.insert(client_id.into(), channel);
    }

    pub fn remoting_channel(&self, client_id: &str) -> Option<Channel> {
        self.remoting_channels.get(client_id).map(|entry| entry.clone())
    }

    pub fn register_pending_telemetry_command(&self, client_id: &str, kind: TelemetryCommandKind, nonce: &str) -> bool {
        let trimmed_nonce = nonce.trim();
        if trimmed_nonce.is_empty() {
            return false;
        }

        let key = TelemetryCommandKey::new(client_id, kind, trimmed_nonce);
        if self.pending_telemetry_commands.contains_key(&key) {
            return false;
        }

        self.pending_telemetry_commands.insert(
            key,
            PendingTelemetryCommand {
                client_id: client_id.to_owned(),
                kind,
                nonce: trimmed_nonce.to_owned(),
                requested_at: SystemTime::now(),
            },
        );
        true
    }

    pub fn has_pending_telemetry_command(&self, client_id: &str, kind: TelemetryCommandKind, nonce: &str) -> bool {
        self.pending_telemetry_commands
            .contains_key(&TelemetryCommandKey::new(client_id, kind, nonce.trim()))
    }

    pub fn remove_pending_telemetry_command(
        &self,
        client_id: &str,
        kind: TelemetryCommandKind,
        nonce: &str,
    ) -> Option<PendingTelemetryCommand> {
        let trimmed_nonce = nonce.trim();
        if trimmed_nonce.is_empty() {
            return None;
        }
        self.pending_telemetry_commands
            .remove(&TelemetryCommandKey::new(client_id, kind, trimmed_nonce))
            .map(|(_, command)| command)
    }

    pub fn complete_print_thread_stack_trace(
        &self,
        client_id: &str,
        nonce: &str,
        thread_stack_trace: Option<String>,
    ) -> bool {
        let Some(command) =
            self.remove_pending_telemetry_command(client_id, TelemetryCommandKind::PrintThreadStackTrace, nonce)
        else {
            return false;
        };

        self.thread_stack_traces.insert(
            ThreadStackTraceKey::new(client_id, command.nonce.as_str()),
            ThreadStackTraceReport {
                client_id: client_id.to_owned(),
                nonce: command.nonce,
                thread_stack_trace,
                reported_at: SystemTime::now(),
            },
        );
        true
    }

    pub fn complete_verify_message(&self, client_id: &str, nonce: &str) -> bool {
        let Some(command) =
            self.remove_pending_telemetry_command(client_id, TelemetryCommandKind::VerifyMessage, nonce)
        else {
            return false;
        };
        self.verify_message_reports.insert(
            TelemetryCommandKey::new(client_id, TelemetryCommandKind::VerifyMessage, command.nonce.as_str()),
            VerifyMessageReport {
                client_id: client_id.to_owned(),
                nonce: command.nonce,
                reported_at: SystemTime::now(),
            },
        );
        true
    }

    pub fn thread_stack_trace_report(&self, client_id: &str, nonce: &str) -> Option<ThreadStackTraceReport> {
        self.thread_stack_traces
            .get(&ThreadStackTraceKey::new(client_id, nonce.trim()))
            .map(|entry| entry.clone())
    }

    pub fn verify_message_report(&self, client_id: &str, nonce: &str) -> Option<VerifyMessageReport> {
        self.verify_message_reports
            .get(&TelemetryCommandKey::new(
                client_id,
                TelemetryCommandKind::VerifyMessage,
                nonce.trim(),
            ))
            .map(|entry| entry.clone())
    }

    pub fn register_pending_lite_unsubscribe_notice(&self, client_id: &str, lite_topic: &str) -> bool {
        let trimmed_lite_topic = lite_topic.trim();
        if trimmed_lite_topic.is_empty() {
            return false;
        }

        let key = LiteUnsubscribeNoticeKey::new(client_id, trimmed_lite_topic);
        if self.pending_lite_unsubscribe_notices.contains_key(&key) {
            return false;
        }

        self.pending_lite_unsubscribe_notices.insert(
            key,
            PendingLiteUnsubscribeNotice {
                client_id: client_id.to_owned(),
                lite_topic: trimmed_lite_topic.to_owned(),
                requested_at: SystemTime::now(),
            },
        );
        true
    }

    pub fn remove_pending_lite_unsubscribe_notice(
        &self,
        client_id: &str,
        lite_topic: &str,
    ) -> Option<PendingLiteUnsubscribeNotice> {
        let trimmed_lite_topic = lite_topic.trim();
        if trimmed_lite_topic.is_empty() {
            return None;
        }
        self.pending_lite_unsubscribe_notices
            .remove(&LiteUnsubscribeNoticeKey::new(client_id, trimmed_lite_topic))
            .map(|(_, notice)| notice)
    }

    pub fn has_pending_lite_unsubscribe_notice(&self, client_id: &str, lite_topic: &str) -> bool {
        self.pending_lite_unsubscribe_notices
            .contains_key(&LiteUnsubscribeNoticeKey::new(client_id, lite_topic.trim()))
    }

    pub fn prepared_transaction(
        &self,
        client_id: &str,
        transaction_id: &str,
        message_id: &str,
    ) -> Option<PreparedTransactionHandle> {
        let transaction_id = transaction_id.trim();
        if !transaction_id.is_empty() {
            let key = PreparedTransactionKey::new(client_id.to_owned(), transaction_id.to_owned());
            if let Some(mut tracked) = self.prepared_transactions.get_mut(&key) {
                tracked.last_touched = SystemTime::now();
                return Some(tracked.clone());
            }
        }

        let trimmed_message_id = message_id.trim();
        if trimmed_message_id.is_empty() {
            return None;
        }

        let matching_key = self
            .prepared_transactions
            .iter()
            .find(|entry| entry.key().client_id == client_id && entry.value().message_id == trimmed_message_id)
            .map(|entry| entry.key().clone())?;
        let mut tracked = self.prepared_transactions.get_mut(&matching_key)?;
        tracked.last_touched = SystemTime::now();
        Some(tracked.clone())
    }

    pub fn remove_prepared_transaction(
        &self,
        client_id: &str,
        transaction_id: &str,
        message_id: &str,
    ) -> Option<PreparedTransactionHandle> {
        let transaction_id = transaction_id.trim();
        if !transaction_id.is_empty() {
            if let Some((_, tracked)) = self.prepared_transactions.remove(&PreparedTransactionKey::new(
                client_id.to_owned(),
                transaction_id.to_owned(),
            )) {
                return Some(tracked);
            }
        }

        let trimmed_message_id = message_id.trim();
        if trimmed_message_id.is_empty() {
            return None;
        }

        let matching_key = self
            .prepared_transactions
            .iter()
            .find(|entry| entry.key().client_id == client_id && entry.value().message_id == trimmed_message_id)
            .map(|entry| entry.key().clone())?;
        self.prepared_transactions
            .remove(&matching_key)
            .map(|(_, tracked)| tracked)
    }

    pub fn prepared_transactions_due_for_recovery(&self, now: SystemTime) -> Vec<PreparedTransactionHandle> {
        self.prepared_transactions
            .iter()
            .filter_map(|entry| {
                let tracked = entry.value();
                let recovery_duration = tracked.orphaned_transaction_recovery_duration?;
                is_expired(tracked.last_touched, now, recovery_duration).then(|| tracked.clone())
            })
            .collect()
    }

    pub fn track_receipt_handle(&self, registration: ReceiptHandleRegistration) -> TrackedReceiptHandle {
        let now = SystemTime::now();
        let tracked = TrackedReceiptHandle {
            client_id: registration.client_id,
            group: registration.group,
            topic: registration.topic,
            message_id: registration.message_id,
            receipt_handle: registration.receipt_handle,
            invisible_duration: registration.invisible_duration,
            last_touched: now,
            next_renew_at: renew_at(now, registration.invisible_duration),
            cancellation: CancellationToken::new(),
        };
        let key = ReceiptHandleKey::from(&tracked);
        if let Some((_, previous)) = self.receipt_handles.remove(&key) {
            previous.cancellation.cancel();
        }
        self.receipt_handles.insert(key, tracked.clone());
        tracked
    }

    pub fn tracked_receipt_handle(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        message_id: &str,
    ) -> Option<TrackedReceiptHandle> {
        let key = ReceiptHandleKey::new(client_id, group.clone(), topic.clone(), message_id);
        self.receipt_handles.get(&key).map(|entry| entry.clone())
    }

    pub fn update_receipt_handle(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        message_id: &str,
        new_receipt_handle: &str,
        invisible_duration: Duration,
    ) -> Option<TrackedReceiptHandle> {
        let key = self.resolve_receipt_handle_key(client_id, group, topic, message_id, None)?;
        let mut tracked = self.receipt_handles.get_mut(&key)?;
        let now = SystemTime::now();
        tracked.receipt_handle = new_receipt_handle.to_owned();
        tracked.invisible_duration = invisible_duration;
        tracked.last_touched = now;
        tracked.next_renew_at = renew_at(now, invisible_duration);
        Some(tracked.clone())
    }

    pub fn update_receipt_handle_matching(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        message_id: &str,
        current_receipt_handle: &str,
        new_receipt_handle: &str,
        invisible_duration: Duration,
    ) -> Option<TrackedReceiptHandle> {
        let key = self.resolve_receipt_handle_key(client_id, group, topic, message_id, Some(current_receipt_handle))?;
        let mut tracked = self.receipt_handles.get_mut(&key)?;
        let now = SystemTime::now();
        tracked.receipt_handle = new_receipt_handle.to_owned();
        tracked.invisible_duration = invisible_duration;
        tracked.last_touched = now;
        tracked.next_renew_at = renew_at(now, invisible_duration);
        Some(tracked.clone())
    }

    pub fn mark_receipt_handle_renew_attempted(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        message_id: &str,
        invisible_duration: Duration,
    ) -> Option<TrackedReceiptHandle> {
        let key = self.resolve_receipt_handle_key(client_id, group, topic, message_id, None)?;
        let mut tracked = self.receipt_handles.get_mut(&key)?;
        tracked.next_renew_at = renew_at(SystemTime::now(), invisible_duration);
        Some(tracked.clone())
    }

    pub fn remove_receipt_handle(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        message_id: &str,
    ) -> Option<TrackedReceiptHandle> {
        let key = self.resolve_receipt_handle_key(client_id, group, topic, message_id, None)?;
        self.remove_receipt_handle_by_key(&key)
    }

    pub fn remove_receipt_handle_matching(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        message_id: &str,
        receipt_handle: &str,
    ) -> Option<TrackedReceiptHandle> {
        let key = self.resolve_receipt_handle_key(client_id, group, topic, message_id, Some(receipt_handle))?;
        self.remove_receipt_handle_by_key(&key)
    }

    pub fn remove(&self, client_id: &str) -> Option<ClientSession> {
        self.remove_client(client_id)
    }

    pub fn remove_client(&self, client_id: &str) -> Option<ClientSession> {
        let session = self.sessions.remove(client_id).map(|(_, session)| session);
        let keys = self
            .receipt_handles
            .iter()
            .filter(|entry| entry.key().client_id == client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in keys {
            let _ = self.remove_receipt_handle_by_key(&key);
        }
        let lite_keys = self
            .lite_subscriptions
            .iter()
            .filter(|entry| entry.key().client_id == client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in lite_keys {
            let _ = self.lite_subscriptions.remove(&key);
        }
        let prepared_transaction_keys = self
            .prepared_transactions
            .iter()
            .filter(|entry| entry.key().client_id == client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in prepared_transaction_keys {
            let _ = self.prepared_transactions.remove(&key);
        }
        self.telemetry_links.remove(client_id);
        self.remoting_channels.remove(client_id);
        let pending_telemetry_keys = self
            .pending_telemetry_commands
            .iter()
            .filter(|entry| entry.key().client_id == client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in pending_telemetry_keys {
            let _ = self.pending_telemetry_commands.remove(&key);
        }
        let thread_stack_trace_keys = self
            .thread_stack_traces
            .iter()
            .filter(|entry| entry.key().client_id == client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in thread_stack_trace_keys {
            let _ = self.thread_stack_traces.remove(&key);
        }
        let verify_message_report_keys = self
            .verify_message_reports
            .iter()
            .filter(|entry| entry.key().client_id == client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in verify_message_report_keys {
            let _ = self.verify_message_reports.remove(&key);
        }
        let pending_lite_unsubscribe_keys = self
            .pending_lite_unsubscribe_notices
            .iter()
            .filter(|entry| entry.key().client_id == client_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in pending_lite_unsubscribe_keys {
            let _ = self.pending_lite_unsubscribe_notices.remove(&key);
        }
        session
    }

    pub fn get(&self, client_id: &str) -> Option<ClientSession> {
        self.sessions.get(client_id).map(|entry| entry.clone())
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn tracked_handle_count(&self) -> usize {
        self.receipt_handles.len()
    }

    pub fn receipt_handles_due_for_renewal(&self, now: SystemTime) -> Vec<TrackedReceiptHandle> {
        self.receipt_handles
            .iter()
            .filter(|entry| entry.next_renew_at <= now)
            .map(|entry| entry.clone())
            .collect()
    }

    pub fn lite_subscription_count(&self) -> usize {
        self.lite_subscriptions.len()
    }

    pub fn prepared_transaction_count(&self) -> usize {
        self.prepared_transactions.len()
    }

    pub fn telemetry_link_count(&self) -> usize {
        self.telemetry_links.len()
    }

    pub fn remoting_channel_count(&self) -> usize {
        self.remoting_channels.len()
    }

    pub fn pending_telemetry_command_count(&self) -> usize {
        self.pending_telemetry_commands.len()
    }

    pub fn verify_message_report_count(&self) -> usize {
        self.verify_message_reports.len()
    }

    pub fn thread_stack_trace_report_count(&self) -> usize {
        self.thread_stack_traces.len()
    }

    pub fn pending_lite_unsubscribe_notice_count(&self) -> usize {
        self.pending_lite_unsubscribe_notices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
            && self.receipt_handles.is_empty()
            && self.lite_subscriptions.is_empty()
            && self.prepared_transactions.is_empty()
            && self.telemetry_links.is_empty()
            && self.pending_telemetry_commands.is_empty()
            && self.thread_stack_traces.is_empty()
            && self.verify_message_reports.is_empty()
            && self.pending_lite_unsubscribe_notices.is_empty()
    }

    fn clear_pending_telemetry_commands_of_kind(&self, client_id: &str, kind: TelemetryCommandKind) {
        let keys = self
            .pending_telemetry_commands
            .iter()
            .filter(|entry| entry.key().client_id == client_id && entry.key().kind == kind)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in keys {
            let _ = self.pending_telemetry_commands.remove(&key);
        }
    }

    pub fn reap_expired(&self, client_ttl: Duration, receipt_handle_ttl: Duration) -> ReapSummary {
        let now = SystemTime::now();
        let mut summary = ReapSummary::default();

        let expired_sessions = self
            .sessions
            .iter()
            .filter(|entry| is_expired(entry.last_seen, now, client_ttl))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for client_id in expired_sessions {
            if self.remove_client(&client_id).is_some() {
                summary.removed_sessions += 1;
            }
        }

        let expired_receipt_handles = self
            .receipt_handles
            .iter()
            .filter(|entry| is_expired(entry.last_touched, now, receipt_handle_ttl))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in expired_receipt_handles {
            if self.remove_receipt_handle_by_key(&key).is_some() {
                summary.removed_receipt_handles += 1;
            }
        }

        let expired_lite_subscriptions = self
            .lite_subscriptions
            .iter()
            .filter(|entry| is_expired(entry.last_touched, now, client_ttl))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in expired_lite_subscriptions {
            if self.lite_subscriptions.remove(&key).is_some() {
                summary.removed_lite_subscriptions += 1;
            }
        }

        let expired_prepared_transactions = self
            .prepared_transactions
            .iter()
            .filter(|entry| is_expired(entry.last_touched, now, client_ttl))
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in expired_prepared_transactions {
            if self.prepared_transactions.remove(&key).is_some() {
                summary.removed_prepared_transactions += 1;
            }
        }

        summary
    }

    fn resolve_receipt_handle_key(
        &self,
        client_id: &str,
        group: &ResourceIdentity,
        topic: &ResourceIdentity,
        message_id: &str,
        receipt_handle: Option<&str>,
    ) -> Option<ReceiptHandleKey> {
        let trimmed_message_id = message_id.trim();
        if !trimmed_message_id.is_empty() {
            let key = ReceiptHandleKey::new(
                client_id.to_owned(),
                group.clone(),
                topic.clone(),
                trimmed_message_id.to_owned(),
            );
            if self.receipt_handles.contains_key(&key) {
                return Some(key);
            }
        }

        let receipt_handle = receipt_handle?;
        self.receipt_handles
            .iter()
            .find(|entry| {
                entry.key().client_id == client_id
                    && entry.key().group == *group
                    && entry.key().topic == *topic
                    && entry.value().receipt_handle == receipt_handle
            })
            .map(|entry| entry.key().clone())
    }

    fn remove_receipt_handle_by_key(&self, key: &ReceiptHandleKey) -> Option<TrackedReceiptHandle> {
        self.receipt_handles.remove(key).map(|(_, tracked)| {
            tracked.cancellation.cancel();
            tracked
        })
    }
}

fn resource_identity(resource: &v2::Resource) -> ResourceIdentity {
    ResourceIdentity::new(resource.resource_namespace.clone(), resource.name.clone())
}

pub fn build_lite_subscription_sync_request(
    request: &v2::SyncLiteSubscriptionRequest,
) -> ProxyResult<LiteSubscriptionSyncRequest> {
    let action = v2::LiteSubscriptionAction::try_from(request.action)
        .map_err(|_| ProxyError::illegal_lite_topic(format!("unknown lite subscription action: {}", request.action)))?;
    let topic = resource_identity(
        request
            .topic
            .as_ref()
            .ok_or_else(|| ProxyError::illegal_lite_topic("topic must not be empty"))?,
    );
    let group = resource_identity(
        request
            .group
            .as_ref()
            .ok_or_else(|| ProxyError::illegal_lite_topic("group must not be empty"))?,
    );
    let lite_topic_set = request
        .lite_topic_set
        .iter()
        .map(|lite_topic| lite_topic.trim())
        .filter(|lite_topic| !lite_topic.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    Ok(LiteSubscriptionSyncRequest {
        action,
        topic,
        group,
        lite_topic_set,
        version: request.version,
        offset_option: request.offset_option,
    })
}

fn proto_duration(duration: &prost_types::Duration) -> Option<Duration> {
    if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
        return None;
    }

    let seconds = u64::try_from(duration.seconds).ok()?;
    let nanos = u32::try_from(duration.nanos).ok()?;
    Some(Duration::new(seconds, nanos))
}

fn is_expired(instant: SystemTime, now: SystemTime, ttl: Duration) -> bool {
    match now.duration_since(instant) {
        Ok(elapsed) => elapsed >= ttl,
        Err(_) => false,
    }
}

fn renew_at(now: SystemTime, invisible_duration: Duration) -> SystemTime {
    now.checked_add(auto_renew_interval(invisible_duration)).unwrap_or(now)
}

fn auto_renew_interval(invisible_duration: Duration) -> Duration {
    let half_millis = invisible_duration.as_millis().saturating_div(2).clamp(1_000, 15_000) as u64;
    Duration::from_millis(half_millis)
}

fn validate_lite_subscription_request(
    request: &LiteSubscriptionSyncRequest,
    settings: Option<&ClientSettingsSnapshot>,
) -> ProxyResult<()> {
    if request.topic.name().is_empty() {
        return Err(ProxyError::illegal_lite_topic("topic name must not be empty"));
    }
    if request.group.name().is_empty() {
        return Err(ProxyError::illegal_lite_topic("group name must not be empty"));
    }

    let max_lite_topic_size = settings
        .and_then(|settings| settings.subscription.as_ref())
        .and_then(|subscription| subscription.max_lite_topic_size)
        .unwrap_or(64) as usize;

    for lite_topic in &request.lite_topic_set {
        if lite_topic.is_empty() {
            return Err(ProxyError::illegal_lite_topic("lite topic must not be empty"));
        }
        if lite_topic.len() > max_lite_topic_size {
            return Err(ProxyError::illegal_lite_topic(format!(
                "lite topic '{lite_topic}' exceeds max length {max_lite_topic_size}"
            )));
        }

        let Some(lmq_name) = to_lmq_name(request.topic.name(), lite_topic) else {
            return Err(ProxyError::illegal_lite_topic(format!(
                "failed to compose lite topic '{lite_topic}' for topic '{}'",
                request.topic.name()
            )));
        };
        let Some((parent_topic, parsed_lite_topic)) = get_parent_and_lite_topic(&lmq_name) else {
            return Err(ProxyError::illegal_lite_topic(format!(
                "lite topic '{lite_topic}' cannot be encoded as LMQ name"
            )));
        };
        if parent_topic != request.topic.name() || parsed_lite_topic != *lite_topic {
            return Err(ProxyError::illegal_lite_topic(format!(
                "lite topic '{lite_topic}' contains unsupported characters"
            )));
        }
    }

    Ok(())
}

fn ensure_lite_subscription_quota(size: usize, settings: Option<&ClientSettingsSnapshot>) -> ProxyResult<()> {
    let quota = settings
        .and_then(|settings| settings.subscription.as_ref())
        .and_then(|subscription| subscription.lite_subscription_quota)
        .unwrap_or(1200) as usize;
    if size > quota {
        return Err(ProxyError::lite_subscription_quota_exceeded(format!(
            "lite subscription count {size} exceeds quota {quota}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;
    use std::time::SystemTime;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::ClientSession;
    use super::ClientSessionRegistry;
    use super::ClientSettingsSnapshot;
    use super::LiteSubscriptionSyncRequest;
    use super::PreparedTransactionRegistration;
    use super::ReceiptHandleRegistration;
    use super::SubscriptionSettingsSnapshot;
    use super::TelemetryCommandKind;
    use super::TrackedReceiptHandle;
    use crate::context::ProxyContext;
    use crate::proto::v2;
    use crate::service::ResourceIdentity;

    fn context(client_id: &'static str) -> ProxyContext {
        let mut request = tonic::Request::new(());
        request
            .metadata_mut()
            .insert("x-mq-client-id", tonic::metadata::MetadataValue::from_static(client_id));
        request.metadata_mut().insert(
            "x-mq-channel-id",
            tonic::metadata::MetadataValue::from_static("channel-a"),
        );
        ProxyContext::from_grpc_request("Test", &request).expect("context should be constructed")
    }

    fn tracked_handle(client_id: &str, message_id: &str, receipt_handle: &str) -> ReceiptHandleRegistration {
        ReceiptHandleRegistration {
            client_id: client_id.to_owned(),
            group: ResourceIdentity::new("", "GroupA"),
            topic: ResourceIdentity::new("", "TopicA"),
            message_id: message_id.to_owned(),
            receipt_handle: receipt_handle.to_owned(),
            invisible_duration: Duration::from_secs(30),
        }
    }

    fn prepared_transaction(
        client_id: &str,
        message_id: &str,
        transaction_id: &str,
    ) -> PreparedTransactionRegistration {
        PreparedTransactionRegistration {
            client_id: client_id.to_owned(),
            topic: ResourceIdentity::new("", "TopicA"),
            message_id: message_id.to_owned(),
            transaction_id: transaction_id.to_owned(),
            producer_group: format!("PROXY_SEND-{client_id}"),
            transaction_state_table_offset: 7,
            commit_log_message_id: format!("offset-{message_id}"),
            message: v2::Message {
                topic: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "TopicA".to_owned(),
                }),
                user_properties: Default::default(),
                system_properties: Some(v2::SystemProperties {
                    message_id: message_id.to_owned(),
                    ..Default::default()
                }),
                body: b"hello".to_vec(),
            },
            orphaned_transaction_recovery_duration: None,
        }
    }

    fn lite_sync_request(action: v2::LiteSubscriptionAction, topics: &[&str]) -> LiteSubscriptionSyncRequest {
        LiteSubscriptionSyncRequest {
            action,
            topic: ResourceIdentity::new("", "TopicA"),
            group: ResourceIdentity::new("", "GroupA"),
            lite_topic_set: topics.iter().map(|topic| (*topic).to_owned()).collect(),
            version: Some(1),
            offset_option: None,
        }
    }

    #[test]
    fn telemetry_settings_are_snapshotted() {
        let registry = ClientSessionRegistry::default();
        let context = context("client-a");
        let settings = v2::Settings {
            client_type: Some(v2::ClientType::PushConsumer as i32),
            request_timeout: Some(prost_types::Duration { seconds: 3, nanos: 0 }),
            pub_sub: Some(v2::settings::PubSub::Subscription(v2::Subscription {
                group: Some(v2::Resource {
                    resource_namespace: String::new(),
                    name: "GroupA".to_owned(),
                }),
                subscriptions: Vec::new(),
                fifo: Some(true),
                receive_batch_size: Some(32),
                long_polling_timeout: Some(prost_types::Duration { seconds: 15, nanos: 0 }),
                lite_subscription_quota: None,
                max_lite_topic_size: None,
            })),
            user_agent: None,
            access_point: None,
            backoff_policy: None,
            metric: None,
        };

        let snapshot = registry
            .update_settings_from_telemetry(&context, &settings)
            .expect("settings should be stored");

        assert_eq!(snapshot.client_type, Some(v2::ClientType::PushConsumer as i32));
        assert_eq!(snapshot.request_timeout, Some(Duration::from_secs(3)));
        assert_eq!(
            snapshot
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.receive_batch_size),
            Some(32)
        );
        assert!(registry.get("client-a").and_then(|session| session.settings).is_some());
    }

    #[test]
    fn tracked_receipt_handle_can_be_updated_and_removed() {
        let registry = ClientSessionRegistry::default();
        registry.track_receipt_handle(tracked_handle("client-a", "msg-1", "handle-1"));

        let updated = registry
            .update_receipt_handle_matching(
                "client-a",
                &ResourceIdentity::new("", "GroupA"),
                &ResourceIdentity::new("", "TopicA"),
                "msg-1",
                "handle-1",
                "handle-2",
                Duration::from_secs(45),
            )
            .expect("receipt handle should be updated");
        assert_eq!(updated.receipt_handle, "handle-2");
        assert_eq!(updated.invisible_duration, Duration::from_secs(45));

        let removed = registry
            .remove_receipt_handle_matching(
                "client-a",
                &ResourceIdentity::new("", "GroupA"),
                &ResourceIdentity::new("", "TopicA"),
                "msg-1",
                "handle-2",
            )
            .expect("receipt handle should be removed");
        assert_eq!(removed.receipt_handle, "handle-2");
        assert_eq!(registry.tracked_handle_count(), 0);
    }

    #[test]
    fn receipt_handles_due_for_renewal_returns_only_due_entries() {
        let registry = ClientSessionRegistry::default();
        let tracked = registry.track_receipt_handle(tracked_handle("client-a", "msg-1", "handle-1"));
        let key = super::ReceiptHandleKey::from(&tracked);

        if let Some(mut entry) = registry.receipt_handles.get_mut(&key) {
            entry.next_renew_at = SystemTime::UNIX_EPOCH;
        }

        let due = registry.receipt_handles_due_for_renewal(SystemTime::now());
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].message_id, "msg-1");
    }

    #[test]
    fn sync_lite_subscription_tracks_partial_and_complete_actions() {
        let registry = ClientSessionRegistry::default();

        let snapshot = registry
            .sync_lite_subscription(
                "client-a",
                lite_sync_request(v2::LiteSubscriptionAction::PartialAdd, &["lite-a", "lite-b"]),
                None,
            )
            .expect("partial add should succeed");
        assert_eq!(snapshot.lite_topic_set.len(), 2);

        let snapshot = registry
            .sync_lite_subscription(
                "client-a",
                lite_sync_request(v2::LiteSubscriptionAction::PartialRemove, &["lite-a"]),
                None,
            )
            .expect("partial remove should succeed");
        assert_eq!(snapshot.lite_topic_set.len(), 1);
        assert!(snapshot.lite_topic_set.contains("lite-b"));

        let snapshot = registry
            .sync_lite_subscription(
                "client-a",
                lite_sync_request(v2::LiteSubscriptionAction::CompleteAdd, &["lite-c"]),
                None,
            )
            .expect("complete add should replace existing set");
        assert_eq!(snapshot.lite_topic_set.len(), 1);
        assert!(snapshot.lite_topic_set.contains("lite-c"));

        let snapshot = registry
            .sync_lite_subscription(
                "client-a",
                lite_sync_request(v2::LiteSubscriptionAction::CompleteRemove, &[]),
                None,
            )
            .expect("complete remove should succeed");
        assert!(snapshot.lite_topic_set.is_empty());
        assert_eq!(registry.lite_subscription_count(), 0);
    }

    #[test]
    fn sync_lite_subscription_enforces_quota() {
        let registry = ClientSessionRegistry::default();
        let settings = ClientSettingsSnapshot {
            client_type: Some(v2::ClientType::LitePushConsumer as i32),
            request_timeout: None,
            subscription: Some(SubscriptionSettingsSnapshot {
                group: Some(ResourceIdentity::new("", "GroupA")),
                fifo: false,
                receive_batch_size: None,
                long_polling_timeout: None,
                lite_subscription_quota: Some(1),
                max_lite_topic_size: Some(64),
            }),
        };

        let error = registry
            .sync_lite_subscription(
                "client-a",
                lite_sync_request(v2::LiteSubscriptionAction::CompleteAdd, &["lite-a", "lite-b"]),
                Some(&settings),
            )
            .expect_err("quota should be enforced");

        assert!(matches!(
            error,
            crate::error::ProxyError::LiteSubscriptionQuotaExceeded { .. }
        ));
    }

    #[test]
    fn prepared_transaction_can_be_looked_up_and_removed_by_message_id() {
        let registry = ClientSessionRegistry::default();
        registry.track_prepared_transaction(prepared_transaction("client-a", "msg-1", "tx-1"));

        let tracked = registry
            .prepared_transaction("client-a", "missing", "msg-1")
            .expect("prepared transaction should fall back to message id");
        assert_eq!(tracked.transaction_id, "tx-1");
        assert_eq!(tracked.commit_log_message_id, "offset-msg-1");

        let removed = registry
            .remove_prepared_transaction("client-a", "missing", "msg-1")
            .expect("prepared transaction should be removable by message id");
        assert_eq!(removed.producer_group, "PROXY_SEND-client-a");
        assert_eq!(registry.prepared_transaction_count(), 0);
    }

    #[tokio::test]
    async fn telemetry_link_can_send_and_unbind_commands() {
        let registry = ClientSessionRegistry::default();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        registry.bind_telemetry_link("client-a", sender);

        assert!(registry.send_telemetry_command(
            "client-a",
            v2::TelemetryCommand {
                status: None,
                command: Some(v2::telemetry_command::Command::ReconnectEndpointsCommand(
                    v2::ReconnectEndpointsCommand {
                        nonce: "nonce-a".to_owned(),
                    },
                )),
            },
        ));
        assert!(registry.has_telemetry_link("client-a"));

        let command = receiver.recv().await.expect("telemetry command should be delivered");
        match command.command.expect("command should be present") {
            v2::telemetry_command::Command::ReconnectEndpointsCommand(command) => {
                assert_eq!(command.nonce, "nonce-a");
            }
            other => panic!("unexpected telemetry command: {other:?}"),
        }

        assert!(registry.unbind_telemetry_link("client-a"));
        assert!(!registry.has_telemetry_link("client-a"));
    }

    #[test]
    fn pending_telemetry_command_can_be_completed_with_thread_stack_trace_report() {
        let registry = ClientSessionRegistry::default();

        assert!(registry.register_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::PrintThreadStackTrace,
            "nonce-a",
        ));
        assert!(registry.has_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::PrintThreadStackTrace,
            "nonce-a",
        ));
        assert!(registry.complete_print_thread_stack_trace("client-a", "nonce-a", Some("trace".to_owned()),));
        assert!(!registry.has_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::PrintThreadStackTrace,
            "nonce-a",
        ));
        let report = registry
            .thread_stack_trace_report("client-a", "nonce-a")
            .expect("thread stack trace report should be stored");
        assert_eq!(report.thread_stack_trace.as_deref(), Some("trace"));
    }

    #[test]
    fn pending_verify_message_command_requires_matching_nonce() {
        let registry = ClientSessionRegistry::default();

        assert!(registry.register_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::VerifyMessage,
            "nonce-a",
        ));
        assert!(!registry.complete_verify_message("client-a", "nonce-b"));
        assert!(registry.complete_verify_message("client-a", "nonce-a"));
        assert_eq!(registry.pending_telemetry_command_count(), 0);
        let report = registry
            .verify_message_report("client-a", "nonce-a")
            .expect("verify message report should be stored");
        assert_eq!(report.nonce, "nonce-a");
    }

    #[test]
    fn bind_telemetry_link_clears_pending_reconnect_command() {
        let registry = ClientSessionRegistry::default();

        assert!(registry.register_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::ReconnectEndpoints,
            "nonce-a",
        ));
        let (sender, _receiver) = mpsc::unbounded_channel();
        registry.bind_telemetry_link("client-a", sender);

        assert!(!registry.has_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::ReconnectEndpoints,
            "nonce-a",
        ));
    }

    #[test]
    fn sync_lite_subscription_clears_pending_unsubscribe_notice_once_removed() {
        let registry = ClientSessionRegistry::default();

        let _ = registry.sync_lite_subscription(
            "client-a",
            lite_sync_request(v2::LiteSubscriptionAction::CompleteAdd, &["lite-a", "lite-b"]),
            None,
        );
        assert!(registry.register_pending_lite_unsubscribe_notice("client-a", "lite-a"));
        assert!(registry.has_pending_lite_unsubscribe_notice("client-a", "lite-a"));

        let _ = registry.sync_lite_subscription(
            "client-a",
            lite_sync_request(v2::LiteSubscriptionAction::PartialRemove, &["lite-a"]),
            None,
        );

        assert!(!registry.has_pending_lite_unsubscribe_notice("client-a", "lite-a"));
        assert_eq!(registry.pending_lite_unsubscribe_notice_count(), 0);
    }

    #[test]
    fn remove_client_clears_receipt_handles() {
        let registry = ClientSessionRegistry::default();
        let context = context("client-a");
        registry.upsert_from_context(&context);
        let tracked = registry.track_receipt_handle(tracked_handle("client-a", "msg-1", "handle-1"));
        registry.track_prepared_transaction(prepared_transaction("client-a", "msg-2", "tx-2"));
        let (sender, _receiver) = mpsc::unbounded_channel();
        registry.bind_telemetry_link("client-a", sender);
        assert!(registry.register_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::VerifyMessage,
            "nonce-a",
        ));
        assert!(registry.register_pending_telemetry_command(
            "client-a",
            TelemetryCommandKind::PrintThreadStackTrace,
            "nonce-b",
        ));
        assert!(registry.complete_print_thread_stack_trace("client-a", "nonce-b", Some("trace".to_owned())));
        let _ = registry.sync_lite_subscription(
            "client-a",
            lite_sync_request(v2::LiteSubscriptionAction::CompleteAdd, &["lite-a"]),
            None,
        );

        registry.remove_client("client-a");

        assert!(tracked.cancellation.is_cancelled());
        assert!(registry.get("client-a").is_none());
        assert_eq!(registry.tracked_handle_count(), 0);
        assert_eq!(registry.lite_subscription_count(), 0);
        assert_eq!(registry.prepared_transaction_count(), 0);
        assert_eq!(registry.telemetry_link_count(), 0);
        assert_eq!(registry.pending_telemetry_command_count(), 0);
        assert!(registry.thread_stack_trace_report("client-a", "nonce-b").is_none());
        assert_eq!(registry.verify_message_report_count(), 0);
        assert_eq!(registry.pending_lite_unsubscribe_notice_count(), 0);
    }

    #[test]
    fn reap_expired_removes_stale_sessions_and_receipt_handles() {
        let registry = ClientSessionRegistry::default();
        registry.sessions.insert(
            "client-a".to_owned(),
            ClientSession {
                client_id: "client-a".to_owned(),
                remote_addr: None,
                namespace: None,
                language: None,
                client_version: None,
                connection_id: None,
                client_type: None,
                producer_groups: BTreeSet::new(),
                consumer_groups: BTreeSet::new(),
                settings: None,
                last_seen: SystemTime::UNIX_EPOCH,
            },
        );
        registry.receipt_handles.insert(
            super::ReceiptHandleKey::new(
                "client-b",
                ResourceIdentity::new("", "GroupA"),
                ResourceIdentity::new("", "TopicA"),
                "msg-1",
            ),
            TrackedReceiptHandle {
                client_id: "client-b".to_owned(),
                group: ResourceIdentity::new("", "GroupA"),
                topic: ResourceIdentity::new("", "TopicA"),
                message_id: "msg-1".to_owned(),
                receipt_handle: "handle-1".to_owned(),
                invisible_duration: Duration::from_secs(30),
                last_touched: SystemTime::UNIX_EPOCH,
                next_renew_at: SystemTime::UNIX_EPOCH,
                cancellation: CancellationToken::new(),
            },
        );
        registry.track_prepared_transaction(prepared_transaction("client-c", "msg-2", "tx-2"));
        if let Some(mut tracked) = registry
            .prepared_transactions
            .get_mut(&super::PreparedTransactionKey::new("client-c", "tx-2"))
        {
            tracked.last_touched = SystemTime::UNIX_EPOCH;
        }

        let summary = registry.reap_expired(Duration::from_secs(1), Duration::from_secs(1));

        assert_eq!(summary.removed_sessions, 1);
        assert_eq!(summary.removed_receipt_handles, 1);
        assert_eq!(summary.removed_lite_subscriptions, 0);
        assert_eq!(summary.removed_prepared_transactions, 1);
        assert!(registry.is_empty());
        assert_eq!(registry.tracked_handle_count(), 0);
        assert_eq!(registry.prepared_transaction_count(), 0);
    }
}
