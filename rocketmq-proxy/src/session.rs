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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedTransactionHandle {
    pub client_id: String,
    pub topic: ResourceIdentity,
    pub message_id: String,
    pub transaction_id: String,
    pub producer_group: String,
    pub transaction_state_table_offset: u64,
    pub commit_log_message_id: String,
    pub last_touched: SystemTime,
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
}

impl ClientSessionRegistry {
    pub fn upsert_from_context(&self, context: &ProxyContext) {
        self.upsert_from_context_with_client_type(context, None);
    }

    pub fn upsert_from_context_with_client_type(&self, context: &ProxyContext, client_type: Option<i32>) {
        let Some(client_id) = context.client_id() else {
            return;
        };

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
            last_touched: SystemTime::now(),
        };
        self.prepared_transactions.insert(
            PreparedTransactionKey::new(registration.client_id, registration.transaction_id),
            tracked.clone(),
        );
        tracked
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

    pub fn track_receipt_handle(&self, registration: ReceiptHandleRegistration) -> TrackedReceiptHandle {
        let tracked = TrackedReceiptHandle {
            client_id: registration.client_id,
            group: registration.group,
            topic: registration.topic,
            message_id: registration.message_id,
            receipt_handle: registration.receipt_handle,
            invisible_duration: registration.invisible_duration,
            last_touched: SystemTime::now(),
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
        tracked.receipt_handle = new_receipt_handle.to_owned();
        tracked.invisible_duration = invisible_duration;
        tracked.last_touched = SystemTime::now();
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
        tracked.receipt_handle = new_receipt_handle.to_owned();
        tracked.invisible_duration = invisible_duration;
        tracked.last_touched = SystemTime::now();
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

    pub fn lite_subscription_count(&self) -> usize {
        self.lite_subscriptions.len()
    }

    pub fn prepared_transaction_count(&self) -> usize {
        self.prepared_transactions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
            && self.receipt_handles.is_empty()
            && self.lite_subscriptions.is_empty()
            && self.prepared_transactions.is_empty()
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
    use std::time::Duration;
    use std::time::SystemTime;

    use tokio_util::sync::CancellationToken;

    use super::ClientSession;
    use super::ClientSessionRegistry;
    use super::ClientSettingsSnapshot;
    use super::LiteSubscriptionSyncRequest;
    use super::PreparedTransactionRegistration;
    use super::ReceiptHandleRegistration;
    use super::SubscriptionSettingsSnapshot;
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
        ProxyContext::from_grpc_request("Test", &request)
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

    #[test]
    fn remove_client_clears_receipt_handles() {
        let registry = ClientSessionRegistry::default();
        let context = context("client-a");
        registry.upsert_from_context(&context);
        let tracked = registry.track_receipt_handle(tracked_handle("client-a", "msg-1", "handle-1"));
        registry.track_prepared_transaction(prepared_transaction("client-a", "msg-2", "tx-2"));
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
