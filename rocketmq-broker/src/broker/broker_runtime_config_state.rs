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

use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_runtime::BlockingExecutor;

use crate::config::broker_config::BrokerConfig;
use crate::config::error::BrokerConfigError;
use crate::config::transaction::ConfigUpdateTransaction;
use crate::config::validated::ConfigGeneration;
use crate::config::validated::ValidatedBrokerConfig;
use rocketmq_store::MessageIndexRuntimeSnapshot;
use rocketmq_store::MessageIndexRuntimeSource;
use rocketmq_store::MessageStoreConfig;

/// An atomically published broker and message-store configuration generation.
///
/// Keeping both configurations in one generation prevents an admin update from
/// overwriting a controller role transition (or vice versa) with a stale copy of
/// the other configuration.
pub(crate) struct BrokerRuntimeConfigGeneration {
    id: ConfigGeneration,
    published_at_millis: u64,
    config: Arc<ValidatedBrokerConfig>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BrokerRuntimeMutationSnapshot {
    pub(crate) generation: ConfigGeneration,
    pub(crate) auto_create_topic_enable: bool,
    pub(crate) auto_create_subscription_group: bool,
    pub(crate) broker_permission: u32,
    pub(crate) default_topic_queue_nums: u32,
    pub(crate) message_index_enable: bool,
    pub(crate) trace_topic_enable: bool,
}

impl BrokerRuntimeConfigGeneration {
    pub(crate) const fn id(&self) -> ConfigGeneration {
        self.id
    }

    pub(crate) const fn published_at_millis(&self) -> u64 {
        self.published_at_millis
    }

    pub(crate) fn validated(&self) -> &Arc<ValidatedBrokerConfig> {
        &self.config
    }

    pub(crate) fn broker(&self) -> &Arc<BrokerConfig> {
        // The generation owns the immutable validated envelope. Each legacy
        // capability receives only the narrow Arc it already understands.
        self.config.broker_arc_ref()
    }

    pub(crate) fn store(&self) -> &Arc<MessageStoreConfig> {
        self.config.store_arc_ref()
    }

    pub(crate) fn mutation_snapshot(&self) -> BrokerRuntimeMutationSnapshot {
        BrokerRuntimeMutationSnapshot {
            generation: self.id,
            auto_create_topic_enable: self.broker().auto_create_topic_enable,
            auto_create_subscription_group: self.broker().auto_create_subscription_group,
            broker_permission: self.broker().broker_permission,
            default_topic_queue_nums: self.broker().topic_queue_config.default_topic_queue_nums,
            message_index_enable: self.store().message_index_enable,
            trace_topic_enable: self.broker().trace_topic_enable,
        }
    }
}

#[derive(Clone)]
pub(crate) struct BrokerRuntimeConfigState {
    current: Arc<ArcSwap<BrokerRuntimeConfigGeneration>>,
    publication: Arc<parking_lot::Mutex<()>>,
    mutation: Arc<tokio::sync::Mutex<()>>,
    index_incomplete: Arc<AtomicBool>,
    index_transition: Arc<parking_lot::RwLock<()>>,
    index_gap_marker: Option<Arc<MessageIndexGapMarker>>,
}

pub(crate) struct BrokerRuntimeMutationPermit {
    owner: Arc<tokio::sync::Mutex<()>>,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

const INDEX_GAP_MARKER_FILE: &str = "message-index-gap.marker";

struct MessageIndexGapMarker {
    path: PathBuf,
    blocking: BlockingExecutor,
    io_lock: Arc<parking_lot::Mutex<()>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistedIndexGap {
    Invalid,
    Armed(i64),
}

/// Shared request-path view of the currently committed Broker permission.
#[derive(Clone)]
pub(crate) struct BrokerPermissionState {
    source: BrokerPermissionSource,
}

#[derive(Clone)]
enum BrokerPermissionSource {
    Runtime(BrokerRuntimeConfigState),
    Fixed(Arc<AtomicU32>),
}

impl fmt::Debug for BrokerPermissionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerPermissionState")
            .field("permission", &self.get())
            .finish()
    }
}

impl BrokerPermissionState {
    pub(crate) fn new(permission: u32) -> Self {
        Self {
            source: BrokerPermissionSource::Fixed(Arc::new(AtomicU32::new(permission))),
        }
    }

    pub(crate) fn from_runtime(runtime: BrokerRuntimeConfigState) -> Self {
        Self {
            source: BrokerPermissionSource::Runtime(runtime),
        }
    }

    #[inline]
    pub(crate) fn get(&self) -> u32 {
        match &self.source {
            BrokerPermissionSource::Runtime(runtime) => runtime.snapshot().broker().broker_permission,
            BrokerPermissionSource::Fixed(current) => current.load(Ordering::Acquire),
        }
    }

    #[inline]
    pub(crate) fn update(&self, permission: u32) {
        if let BrokerPermissionSource::Fixed(current) = &self.source {
            current.store(permission, Ordering::Release);
        }
    }

    pub(crate) fn runtime_snapshot(&self) -> Option<BrokerRuntimeMutationSnapshot> {
        match &self.source {
            BrokerPermissionSource::Runtime(runtime) => Some(runtime.snapshot().mutation_snapshot()),
            BrokerPermissionSource::Fixed(_) => None,
        }
    }

    #[inline]
    pub(crate) fn auto_create_subscription_group(&self) -> Option<bool> {
        match &self.source {
            BrokerPermissionSource::Runtime(runtime) => {
                Some(runtime.snapshot().broker().auto_create_subscription_group)
            }
            BrokerPermissionSource::Fixed(_) => None,
        }
    }
}

impl BrokerRuntimeConfigState {
    #[cfg(test)]
    pub(crate) fn new(config: Arc<ValidatedBrokerConfig>) -> Self {
        Self::new_inner(config, None)
    }

    pub(crate) fn new_with_index_gap_marker(config: Arc<ValidatedBrokerConfig>, blocking: BlockingExecutor) -> Self {
        let marker_path = PathBuf::from(config.store().store_path_root_dir.as_str())
            .join("config")
            .join(INDEX_GAP_MARKER_FILE);
        Self::new_inner(
            config,
            Some(Arc::new(MessageIndexGapMarker {
                path: marker_path,
                blocking,
                io_lock: Arc::new(parking_lot::Mutex::new(())),
            })),
        )
    }

    fn new_inner(config: Arc<ValidatedBrokerConfig>, index_gap_marker: Option<Arc<MessageIndexGapMarker>>) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(BrokerRuntimeConfigGeneration {
                id: ConfigGeneration::INITIAL,
                published_at_millis: rocketmq_model::time::current_millis(),
                config,
            })),
            publication: Arc::new(parking_lot::Mutex::new(())),
            mutation: Arc::new(tokio::sync::Mutex::new(())),
            index_incomplete: Arc::new(AtomicBool::new(false)),
            index_transition: Arc::new(parking_lot::RwLock::new(())),
            index_gap_marker,
        }
    }

    pub(crate) async fn lock_mutation(&self) -> BrokerRuntimeMutationPermit {
        let owner = Arc::clone(&self.mutation);
        let guard = Arc::clone(&owner).lock_owned().await;
        BrokerRuntimeMutationPermit { owner, _guard: guard }
    }

    pub(crate) async fn initialize_index_completeness(
        &self,
        max_phy_offset: i64,
        index_safe_offset: Option<i64>,
    ) -> Result<(), BrokerConfigError> {
        let enabled = self.current.load().store().message_index_enable;
        let Some(marker) = self.index_gap_marker.as_ref() else {
            if max_phy_offset > 0 && !enabled {
                self.index_incomplete.store(true, Ordering::Release);
            }
            return Ok(());
        };
        match marker.read().await? {
            Some(PersistedIndexGap::Invalid) => {
                self.index_incomplete.store(true, Ordering::Release);
            }
            Some(PersistedIndexGap::Armed(_))
                if enabled && index_safe_offset.is_some_and(|offset| offset >= max_phy_offset) =>
            {
                marker.clear().await?;
                self.index_incomplete.store(false, Ordering::Release);
            }
            Some(PersistedIndexGap::Armed(baseline)) => {
                let incomplete = max_phy_offset != baseline || index_safe_offset.is_none_or(|offset| offset < baseline);
                self.index_incomplete.store(incomplete, Ordering::Release);
                if incomplete {
                    marker.write_invalid().await?;
                }
            }
            None if !enabled => {
                if max_phy_offset > 0 {
                    marker.write_invalid().await?;
                    self.index_incomplete.store(true, Ordering::Release);
                } else {
                    marker.write_armed(0).await?;
                }
            }
            None => {}
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Arc<BrokerRuntimeConfigGeneration> {
        self.current.load_full()
    }

    pub(crate) fn broker_snapshot(&self) -> Arc<BrokerConfig> {
        Arc::clone(self.snapshot().broker())
    }

    pub(crate) fn store_snapshot(&self) -> Arc<MessageStoreConfig> {
        Arc::clone(self.snapshot().store())
    }

    pub(crate) fn commit_under_mutation(
        &self,
        mutation: &BrokerRuntimeMutationPermit,
        transaction: ConfigUpdateTransaction,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.verify_mutation_permit(mutation)?;
        self.commit_inner(transaction)
    }

    pub(crate) fn commit_message_index_enable_under_mutation(
        &self,
        mutation: &BrokerRuntimeMutationPermit,
        transaction: ConfigUpdateTransaction,
        offsets: impl FnOnce() -> (i64, Option<i64>),
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.verify_mutation_permit(mutation)?;
        let _transition = self.index_transition.write();
        let _publication = self.publication.lock();
        if self.index_incomplete.load(Ordering::Acquire) {
            if let Some(marker) = self.index_gap_marker.as_ref() {
                marker.replace_sync("invalid\n")?;
            }
            return Err(index_gap_coordination_error(
                "message index is incomplete and requires a verified rebuild",
            ));
        }
        let (max_phy_offset, index_safe_offset) = offsets();
        let previous_marker = match self.index_gap_marker.as_ref() {
            Some(marker) => Some(
                marker
                    .verify_and_clear_armed_sync(max_phy_offset, index_safe_offset)
                    .inspect_err(|_| {
                        self.index_incomplete.store(true, Ordering::Release);
                    })?,
            ),
            None => None,
        };
        let result = self.commit_inner_with_publication(transaction);
        if result.is_err() {
            if let (Some(marker), Some(previous)) = (self.index_gap_marker.as_ref(), previous_marker) {
                marker.restore_sync(previous)?;
            }
        }
        result
    }

    pub(crate) fn commit_message_index_disable_under_mutation(
        &self,
        mutation: &BrokerRuntimeMutationPermit,
        transaction: ConfigUpdateTransaction,
        max_phy_offset: impl FnOnce() -> i64,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.verify_mutation_permit(mutation)?;
        let _transition = self.index_transition.write();
        let _publication = self.publication.lock();
        let marker_value = if self.index_incomplete.load(Ordering::Acquire) {
            "invalid\n".to_owned()
        } else {
            format!("armed:{}\n", max_phy_offset())
        };
        let previous_marker = match self.index_gap_marker.as_ref() {
            Some(marker) => Some(marker.replace_sync(&marker_value)?),
            None => None,
        };
        let result = self.commit_inner_with_publication(transaction);
        if result.is_err() {
            if let (Some(marker), Some(previous)) = (self.index_gap_marker.as_ref(), previous_marker) {
                marker.restore_sync(previous)?;
            }
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn commit(
        &self,
        transaction: ConfigUpdateTransaction,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.commit_inner(transaction)
    }

    fn commit_inner(
        &self,
        transaction: ConfigUpdateTransaction,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        let _publication = self.publication.lock();
        self.commit_inner_with_publication(transaction)
    }

    fn commit_inner_with_publication(
        &self,
        transaction: ConfigUpdateTransaction,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        let expected = transaction.expected_generation();
        let current = self.snapshot();
        if current.id() != expected {
            return Err(BrokerConfigError::GenerationConflict {
                expected: expected.value(),
                actual: current.id().value(),
            });
        }
        let next_id = expected.checked_next().ok_or(BrokerConfigError::GenerationExhausted)?;

        let next = Arc::new(BrokerRuntimeConfigGeneration {
            id: next_id,
            published_at_millis: rocketmq_model::time::current_millis(),
            config: Arc::new(transaction.into_candidate()),
        });
        let previous = self.current.compare_and_swap(&current, Arc::clone(&next));
        if Arc::ptr_eq(&previous, &current) {
            return Ok(next);
        }

        Err(BrokerConfigError::GenerationConflict {
            expected: expected.value(),
            actual: previous.id().value(),
        })
    }

    #[cfg(test)]
    pub(crate) fn replace_broker(
        &self,
        broker: BrokerConfig,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.replace_with(|current| current.with_broker_candidate(broker.clone()))
    }

    #[cfg(test)]
    pub(crate) fn replace_store(
        &self,
        store: MessageStoreConfig,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        self.replace_with(|current| current.with_store_candidate(store.clone()))
    }

    pub(crate) fn prepare_role_update(
        &self,
        mutation: &BrokerRuntimeMutationPermit,
        broker_id: u64,
        broker_role: BrokerRole,
    ) -> Result<ConfigUpdateTransaction, BrokerConfigError> {
        self.verify_mutation_permit(mutation)?;
        let current = self.snapshot();
        current
            .id()
            .checked_next()
            .ok_or(BrokerConfigError::GenerationExhausted)?;
        let mut broker = current.broker().as_ref().clone();
        broker.broker_identity.broker_id = broker_id;
        let mut store = current.store().as_ref().clone();
        store.broker_role = broker_role;
        let candidate = current.validated().with_candidates(broker, store)?;
        Ok(ConfigUpdateTransaction::replacement(current.id(), candidate))
    }

    pub(crate) fn prepare_data_read_ahead_update(
        &self,
        mutation: &BrokerRuntimeMutationPermit,
        enabled: bool,
    ) -> Result<ConfigUpdateTransaction, BrokerConfigError> {
        self.verify_mutation_permit(mutation)?;
        let current = self.snapshot();
        current
            .id()
            .checked_next()
            .ok_or(BrokerConfigError::GenerationExhausted)?;
        let mut store = current.store().as_ref().clone();
        store.data_read_ahead_enable = enabled;
        let candidate = current.validated().with_store_candidate(store)?;
        Ok(ConfigUpdateTransaction::replacement(current.id(), candidate))
    }

    fn verify_mutation_permit(&self, mutation: &BrokerRuntimeMutationPermit) -> Result<(), BrokerConfigError> {
        if Arc::ptr_eq(&self.mutation, &mutation.owner) {
            Ok(())
        } else {
            Err(BrokerConfigError::RuntimeCoordination {
                detail: "runtime mutation permit belongs to a different broker state".to_owned(),
            })
        }
    }

    #[cfg(test)]
    fn replace_with(
        &self,
        mut build_candidate: impl FnMut(&ValidatedBrokerConfig) -> Result<ValidatedBrokerConfig, BrokerConfigError>,
    ) -> Result<Arc<BrokerRuntimeConfigGeneration>, BrokerConfigError> {
        loop {
            let current = self.snapshot();
            let candidate = build_candidate(current.validated().as_ref())?;
            let transaction = ConfigUpdateTransaction::replacement(current.id(), candidate);
            match self.commit_inner(transaction) {
                Ok(generation) => return Ok(generation),
                Err(BrokerConfigError::GenerationConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl MessageIndexGapMarker {
    async fn read(&self) -> Result<Option<PersistedIndexGap>, BrokerConfigError> {
        let path = self.path.clone();
        let io_lock = Arc::clone(&self.io_lock);
        let value = self
            .blocking
            .spawn_io("broker.message-index-gap.read", move || {
                let _io = io_lock.lock();
                std::fs::read_to_string(path)
            })
            .await
            .map_err(index_gap_coordination_error)?;
        match value {
            Ok(value) => parse_index_gap_marker(&value).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(index_gap_coordination_error(error)),
        }
    }

    async fn write_armed(&self, baseline: i64) -> Result<(), BrokerConfigError> {
        self.write(format!("armed:{baseline}\n")).await
    }

    async fn write_invalid(&self) -> Result<(), BrokerConfigError> {
        self.write("invalid\n".to_owned()).await
    }

    async fn clear(&self) -> Result<(), BrokerConfigError> {
        let path = self.path.clone();
        let io_lock = Arc::clone(&self.io_lock);
        self.blocking
            .spawn_io("broker.message-index-gap.clear", move || {
                let _io = io_lock.lock();
                match std::fs::remove_file(path) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(error),
                }
            })
            .await
            .map_err(index_gap_coordination_error)?
            .map_err(index_gap_coordination_error)
    }

    async fn write(&self, value: String) -> Result<(), BrokerConfigError> {
        let path = self.path.clone();
        let io_lock = Arc::clone(&self.io_lock);
        self.blocking
            .spawn_io("broker.message-index-gap.write", move || {
                let _io = io_lock.lock();
                rocketmq_runtime::common::file_utils::string_to_file(&value, path)
            })
            .await
            .map_err(index_gap_coordination_error)?
            .map_err(index_gap_coordination_error)
    }

    fn replace_sync(&self, value: &str) -> Result<Option<String>, BrokerConfigError> {
        let _io = self.io_lock.lock();
        let previous = match std::fs::read_to_string(&self.path) {
            Ok(previous) => Some(previous),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(index_gap_coordination_error(error)),
        };
        if let Err(error) = rocketmq_runtime::common::file_utils::string_to_file(value, &self.path) {
            let _ = restore_marker_path(&self.path, previous.as_deref());
            return Err(index_gap_coordination_error(error));
        }
        Ok(previous)
    }

    fn restore_sync(&self, previous: Option<String>) -> Result<(), BrokerConfigError> {
        let _io = self.io_lock.lock();
        restore_marker_path(&self.path, previous.as_deref())
    }

    fn verify_and_clear_armed_sync(
        &self,
        max_phy_offset: i64,
        index_safe_offset: Option<i64>,
    ) -> Result<Option<String>, BrokerConfigError> {
        let _io = self.io_lock.lock();
        let value = std::fs::read_to_string(&self.path).map_err(index_gap_coordination_error)?;
        match parse_index_gap_marker(&value)? {
            PersistedIndexGap::Armed(baseline)
                if baseline == max_phy_offset && index_safe_offset.is_some_and(|offset| offset >= baseline) =>
            {
                std::fs::remove_file(&self.path).map_err(index_gap_coordination_error)?;
                Ok(Some(value))
            }
            PersistedIndexGap::Armed(_) | PersistedIndexGap::Invalid => {
                rocketmq_runtime::common::file_utils::string_to_file("invalid\n", &self.path)
                    .map_err(index_gap_coordination_error)?;
                Err(index_gap_coordination_error(
                    "message index is incomplete and requires a verified rebuild",
                ))
            }
        }
    }
}

fn restore_marker_path(path: &std::path::Path, previous: Option<&str>) -> Result<(), BrokerConfigError> {
    match previous {
        Some(previous) => {
            rocketmq_runtime::common::file_utils::string_to_file(previous, path).map_err(index_gap_coordination_error)
        }
        None => match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(index_gap_coordination_error(error)),
        },
    }
}

fn parse_index_gap_marker(value: &str) -> Result<PersistedIndexGap, BrokerConfigError> {
    let value = value.trim();
    if value == "invalid" {
        return Ok(PersistedIndexGap::Invalid);
    }
    let baseline = value
        .strip_prefix("armed:")
        .ok_or_else(|| index_gap_coordination_error("invalid message-index gap marker state"))?
        .parse::<i64>()
        .map_err(index_gap_coordination_error)?;
    if baseline < 0 {
        return Err(index_gap_coordination_error(
            "message-index gap baseline must be non-negative",
        ));
    }
    Ok(PersistedIndexGap::Armed(baseline))
}

fn index_gap_coordination_error(error: impl fmt::Display) -> BrokerConfigError {
    BrokerConfigError::RuntimeCoordination {
        detail: format!("message-index gap marker: {error}"),
    }
}

impl MessageIndexRuntimeSource for BrokerRuntimeConfigState {
    fn snapshot(&self) -> MessageIndexRuntimeSnapshot {
        MessageIndexRuntimeSnapshot {
            enabled: self.snapshot().store().message_index_enable,
            incomplete: self.index_incomplete.load(Ordering::Acquire),
        }
    }

    fn with_dispatch_admission(&self, dispatch: &mut dyn FnMut(bool)) {
        let _transition = self.index_transition.read();
        let enabled = {
            let _publication = self.publication.lock();
            self.current.load().store().message_index_enable
        };
        if !enabled {
            self.index_incomplete.store(true, Ordering::Release);
        }
        dispatch(enabled);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Barrier;

    use crate::config::broker_config::BrokerConfig;
    use crate::config::error::BrokerConfigError;
    use crate::config::transaction::ConfigUpdateTransaction;
    use crate::config::validated::ConfigGeneration;
    use crate::config::validated::ValidatedBrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::broker::broker_role::BrokerRole;
    use rocketmq_store::MessageIndexRuntimeSource;
    use rocketmq_store::MessageStoreConfig;
    use tempfile::TempDir;

    use super::BrokerRuntimeConfigState;

    fn runtime_config_state(broker: BrokerConfig, store: MessageStoreConfig) -> BrokerRuntimeConfigState {
        let config =
            ValidatedBrokerConfig::try_from_parts(broker, store).expect("test broker configuration should be valid");
        BrokerRuntimeConfigState::new(Arc::new(config))
    }

    fn runtime_config_state_with_marker(root: &TempDir) -> BrokerRuntimeConfigState {
        let store = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            message_index_enable: true,
            ..MessageStoreConfig::default()
        };
        let config = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
            .expect("test broker configuration should be valid");
        let context = crate::test_service_context("message-index-gap-marker-test");
        BrokerRuntimeConfigState::new_with_index_gap_marker(Arc::new(config), context.metadata_io().clone())
    }

    fn dispatch_admitted(state: &BrokerRuntimeConfigState) -> bool {
        let mut admitted = false;
        MessageIndexRuntimeSource::with_dispatch_admission(state, &mut |enabled| admitted = enabled);
        admitted
    }

    #[test]
    fn independent_updates_preserve_the_other_configuration() {
        let broker = BrokerConfig {
            listen_port: 10912,
            ..BrokerConfig::default()
        };
        let store = MessageStoreConfig {
            ha_listen_port: 10913,
            ..MessageStoreConfig::default()
        };
        let state = runtime_config_state(broker, store);

        let mut next_broker = state.broker_snapshot().as_ref().clone();
        next_broker.listen_port = 20912;
        state
            .replace_broker(next_broker)
            .expect("broker replacement should be valid");
        assert_eq!(state.store_snapshot().ha_listen_port, 10913);

        let mut next_store = state.store_snapshot().as_ref().clone();
        next_store.ha_listen_port = 20913;
        state
            .replace_store(next_store)
            .expect("store replacement should be valid");
        assert_eq!(state.broker_snapshot().listen_port, 20912);
    }

    #[tokio::test]
    async fn role_generation_preserves_unrelated_fields() {
        let broker = BrokerConfig {
            listen_port: 30912,
            ..BrokerConfig::default()
        };
        let store = MessageStoreConfig {
            ha_listen_port: 30913,
            ..MessageStoreConfig::default()
        };
        let state = runtime_config_state(broker, store);
        let mutation = state.lock_mutation().await;

        let update = state
            .prepare_role_update(&mutation, 7, BrokerRole::Slave)
            .expect("slave role should produce a valid generation");
        let generation = state
            .commit_under_mutation(&mutation, update)
            .expect("prepared role should commit under the same mutation guard");

        assert_eq!(generation.broker().broker_identity.broker_id, 7);
        assert_eq!(generation.broker().listen_port, 30912);
        assert_eq!(generation.store().broker_role, BrokerRole::Slave);
        assert_eq!(generation.store().ha_listen_port, 30913);
    }

    #[tokio::test]
    async fn read_ahead_generation_preserves_unrelated_fields() {
        let broker = BrokerConfig {
            listen_port: 30912,
            ..BrokerConfig::default()
        };
        let store = MessageStoreConfig {
            ha_listen_port: 30913,
            ..MessageStoreConfig::default()
        };
        let state = runtime_config_state(broker, store);
        let mutation = state.lock_mutation().await;

        let update = state
            .prepare_data_read_ahead_update(&mutation, true)
            .expect("read-ahead update should produce a valid generation");
        let generation = state
            .commit_under_mutation(&mutation, update)
            .expect("prepared read-ahead update should commit under the same mutation guard");

        assert_eq!(generation.broker().listen_port, 30912);
        assert!(generation.store().data_read_ahead_enable);
        assert_eq!(generation.store().ha_listen_port, 30913);
    }

    #[test]
    fn concurrent_broker_and_store_updates_do_not_lose_either_side() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let barrier = Arc::new(Barrier::new(3));

        let broker_state = state.clone();
        let broker_barrier = Arc::clone(&barrier);
        let broker_thread = std::thread::spawn(move || {
            broker_barrier.wait();
            for listen_port in 20_000..20_100 {
                let mut broker = broker_state.broker_snapshot().as_ref().clone();
                broker.listen_port = listen_port;
                broker_state
                    .replace_broker(broker)
                    .expect("concurrent broker update should remain valid");
            }
        });

        let store_state = state.clone();
        let store_barrier = Arc::clone(&barrier);
        let store_thread = std::thread::spawn(move || {
            store_barrier.wait();
            for ha_listen_port in 30_000..30_100 {
                let mut store = store_state.store_snapshot().as_ref().clone();
                store.ha_listen_port = ha_listen_port;
                store_state
                    .replace_store(store)
                    .expect("concurrent store update should remain valid");
            }
        });

        barrier.wait();
        broker_thread.join().expect("broker updater should finish");
        store_thread.join().expect("store updater should finish");

        let generation = state.snapshot();
        assert_eq!(generation.broker().listen_port, 20_099);
        assert_eq!(generation.store().ha_listen_port, 30_099);
        assert_eq!(generation.id().value(), ConfigGeneration::INITIAL.value() + 200);
    }

    #[test]
    fn stale_transaction_is_rejected_without_replacing_the_committed_generation() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let baseline = state.snapshot();
        let first_patch = HashMap::from([(
            CheetahString::from_static_str("defaultTopicQueueNums"),
            CheetahString::from_static_str("16"),
        )]);
        let stale_patch = HashMap::from([(
            CheetahString::from_static_str("defaultTopicQueueNums"),
            CheetahString::from_static_str("32"),
        )]);
        let first = ConfigUpdateTransaction::from_broker_patch(baseline.id(), baseline.validated(), &first_patch)
            .expect("first patch should validate");
        let stale = ConfigUpdateTransaction::from_broker_patch(baseline.id(), baseline.validated(), &stale_patch)
            .expect("stale patch should validate before publication");

        let committed = state.commit(first).expect("first transaction should commit");
        let error = match state.commit(stale) {
            Ok(_) => panic!("stale transaction must not replace a newer generation"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            BrokerConfigError::GenerationConflict { expected: 1, actual: 2 }
        ));
        assert_eq!(committed.id().value(), 2);
        assert_eq!(state.snapshot().id().value(), 2);
        assert_eq!(state.broker_snapshot().topic_queue_config.default_topic_queue_nums, 16);
    }

    #[test]
    fn invalid_patch_preserves_generation_and_snapshot() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let baseline = state.snapshot();
        let invalid_patch = HashMap::from([(
            CheetahString::from_static_str("defaultTopicQueueNums"),
            CheetahString::from_static_str("0"),
        )]);

        let error =
            match ConfigUpdateTransaction::from_broker_patch(baseline.id(), baseline.validated(), &invalid_patch) {
                Ok(_) => panic!("invalid patch must not produce a publishable transaction"),
                Err(error) => error,
            };

        assert!(matches!(
            error,
            BrokerConfigError::InvalidProperty {
                key,
                value,
                expected: "a canonical integer from 1 through 128"
            } if key == "defaultTopicQueueNums" && value == "0"
        ));
        let current = state.snapshot();
        assert!(Arc::ptr_eq(&baseline, &current));
        assert_eq!(current.id(), ConfigGeneration::INITIAL);
    }

    #[test]
    fn index_runtime_distinguishes_empty_enable_from_a_disabled_gap() {
        let mut store = MessageStoreConfig {
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        let empty = runtime_config_state(BrokerConfig::default(), store.clone());
        let baseline = empty.snapshot();
        let enable = ConfigUpdateTransaction::from_broker_patch(
            baseline.id(),
            baseline.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("true"),
            )]),
        )
        .expect("empty store can enable indexing");
        empty.commit(enable).expect("enable should publish");
        assert_eq!(
            MessageIndexRuntimeSource::snapshot(&empty),
            rocketmq_store::MessageIndexRuntimeSnapshot {
                enabled: true,
                incomplete: false,
            }
        );

        store.message_index_enable = false;
        let gapped = runtime_config_state(BrokerConfig::default(), store);
        assert!(!dispatch_admitted(&gapped));
        let baseline = gapped.snapshot();
        let enable = ConfigUpdateTransaction::from_broker_patch(
            baseline.id(),
            baseline.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("true"),
            )]),
        )
        .expect("gapped store can enable indexing in degraded mode");
        gapped.commit(enable).expect("enable should publish");
        assert!(MessageIndexRuntimeSource::snapshot(&gapped).incomplete);
    }

    #[tokio::test]
    async fn initial_disabled_non_empty_store_is_conservatively_incomplete() {
        let root = TempDir::new().expect("temporary Store root");
        let store = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        let config = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
            .expect("test broker configuration should be valid");
        let context = crate::test_service_context("non-empty-message-index-gap-marker-test");
        let state =
            BrokerRuntimeConfigState::new_with_index_gap_marker(Arc::new(config), context.metadata_io().clone());

        state
            .initialize_index_completeness(1, Some(0))
            .await
            .expect("production marker state should initialize");

        assert_eq!(
            MessageIndexRuntimeSource::snapshot(&state),
            rocketmq_store::MessageIndexRuntimeSnapshot {
                enabled: false,
                incomplete: true,
            }
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("config").join(super::INDEX_GAP_MARKER_FILE))
                .expect("startup must persist invalidity"),
            "invalid\n"
        );
    }

    #[tokio::test]
    async fn marker_write_failure_prevents_disable_publication() {
        let root = TempDir::new().expect("temporary Store root");
        std::fs::write(root.path().join("config"), b"not-a-directory").expect("create marker parent conflict");
        let state = runtime_config_state_with_marker(&root);

        let current = state.snapshot();
        let disable = ConfigUpdateTransaction::from_broker_patch(
            current.id(),
            current.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("false"),
            )]),
        )
        .expect("disable candidate");
        let mutation = state.lock_mutation().await;
        let error = match state.commit_message_index_disable_under_mutation(&mutation, disable, || 0) {
            Ok(_) => panic!("marker persistence must fail closed"),
            Err(error) => error,
        };

        assert!(matches!(error, BrokerConfigError::RuntimeCoordination { .. }));
        assert_eq!(state.snapshot().id(), ConfigGeneration::INITIAL);
        assert!(MessageIndexRuntimeSource::snapshot(&state).enabled);
    }

    #[tokio::test]
    async fn initially_disabled_empty_store_enables_without_a_gap() {
        let root = TempDir::new().expect("temporary Store root");
        let store = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        let config = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
            .expect("test broker configuration should be valid");
        let context = crate::test_service_context("empty-message-index-gap-marker-test");
        let state =
            BrokerRuntimeConfigState::new_with_index_gap_marker(Arc::new(config), context.metadata_io().clone());
        state
            .initialize_index_completeness(0, Some(0))
            .await
            .expect("empty disabled Store should arm its baseline marker");
        assert!(!MessageIndexRuntimeSource::snapshot(&state).incomplete);
        assert!(root.path().join("config").join(super::INDEX_GAP_MARKER_FILE).exists());
        let current = state.snapshot();
        let enable = ConfigUpdateTransaction::from_broker_patch(
            current.id(),
            current.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("true"),
            )]),
        )
        .expect("enable candidate");
        let mutation = state.lock_mutation().await;
        state
            .commit_message_index_enable_under_mutation(&mutation, enable, || (0, Some(0)))
            .expect("proven no-gap enable should publish");

        assert_eq!(
            MessageIndexRuntimeSource::snapshot(&state),
            rocketmq_store::MessageIndexRuntimeSnapshot {
                enabled: true,
                incomplete: false,
            }
        );
        assert!(!root.path().join("config").join(super::INDEX_GAP_MARKER_FILE).exists());

        let restarted = runtime_config_state_with_marker(&root);
        restarted
            .initialize_index_completeness(0, Some(0))
            .await
            .expect("successful no-gap enable should remain safe after restart");
        assert!(!MessageIndexRuntimeSource::snapshot(&restarted).incomplete);
    }

    #[tokio::test]
    async fn enable_rejects_missing_index_progress_even_for_an_empty_store() {
        let root = TempDir::new().expect("temporary Store root");
        let store = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        let config = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
            .expect("test broker configuration should be valid");
        let context = crate::test_service_context("missing-index-progress-test");
        let state =
            BrokerRuntimeConfigState::new_with_index_gap_marker(Arc::new(config), context.metadata_io().clone());
        state
            .initialize_index_completeness(0, Some(0))
            .await
            .expect("empty disabled Store should arm its marker");
        let current = state.snapshot();
        let enable = ConfigUpdateTransaction::from_broker_patch(
            current.id(),
            current.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("true"),
            )]),
        )
        .expect("enable candidate");
        let mutation = state.lock_mutation().await;

        let error = match state.commit_message_index_enable_under_mutation(&mutation, enable, || (0, None)) {
            Ok(_) => panic!("missing index progress must fail closed"),
            Err(error) => error,
        };

        assert!(matches!(error, BrokerConfigError::RuntimeCoordination { .. }));
        assert_eq!(state.snapshot().id(), ConfigGeneration::INITIAL);
        assert!(!MessageIndexRuntimeSource::snapshot(&state).enabled);
        assert!(MessageIndexRuntimeSource::snapshot(&state).incomplete);
    }

    #[tokio::test]
    async fn armed_non_empty_store_requires_persisted_index_progress_before_enable() {
        let root = TempDir::new().expect("temporary Store root");
        let marker_path = root.path().join("config").join(super::INDEX_GAP_MARKER_FILE);
        std::fs::create_dir_all(marker_path.parent().expect("marker parent")).expect("create marker parent");
        std::fs::write(&marker_path, "armed:100\n").expect("seed armed marker");
        let store = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        let config = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
            .expect("test broker configuration should be valid");
        let context = crate::test_service_context("armed-index-progress-test");
        let state =
            BrokerRuntimeConfigState::new_with_index_gap_marker(Arc::new(config), context.metadata_io().clone());

        state
            .initialize_index_completeness(100, Some(99))
            .await
            .expect("startup should persist conservative invalidity");

        assert!(MessageIndexRuntimeSource::snapshot(&state).incomplete);
        assert_eq!(
            std::fs::read_to_string(marker_path).expect("invalid marker"),
            "invalid\n"
        );
    }

    #[tokio::test]
    async fn armed_non_empty_store_enables_when_offsets_prove_no_gap() {
        let root = TempDir::new().expect("temporary Store root");
        let marker_path = root.path().join("config").join(super::INDEX_GAP_MARKER_FILE);
        std::fs::create_dir_all(marker_path.parent().expect("marker parent")).expect("create marker parent");
        std::fs::write(&marker_path, "armed:100\n").expect("seed armed marker");
        let store = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        let config = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
            .expect("test broker configuration should be valid");
        let context = crate::test_service_context("armed-no-gap-enable-test");
        let state =
            BrokerRuntimeConfigState::new_with_index_gap_marker(Arc::new(config), context.metadata_io().clone());
        state
            .initialize_index_completeness(100, Some(100))
            .await
            .expect("matching persisted progress should retain the armed proof");
        let current = state.snapshot();
        let enable = ConfigUpdateTransaction::from_broker_patch(
            current.id(),
            current.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("true"),
            )]),
        )
        .expect("enable candidate");
        let mutation = state.lock_mutation().await;

        state
            .commit_message_index_enable_under_mutation(&mutation, enable, || (100, Some(100)))
            .expect("no-gap proof should allow enable");

        assert!(MessageIndexRuntimeSource::snapshot(&state).enabled);
        assert!(!MessageIndexRuntimeSource::snapshot(&state).incomplete);
        assert!(!marker_path.exists());
    }

    #[tokio::test]
    async fn enabled_startup_clears_a_stale_armed_marker_when_the_index_covers_the_store() {
        let root = TempDir::new().expect("temporary Store root");
        let marker_path = root.path().join("config").join(super::INDEX_GAP_MARKER_FILE);
        std::fs::create_dir_all(marker_path.parent().expect("marker parent")).expect("create marker parent");
        std::fs::write(&marker_path, "armed:100\n").expect("seed stale armed marker");
        let state = runtime_config_state_with_marker(&root);

        state
            .initialize_index_completeness(200, Some(200))
            .await
            .expect("current index coverage should retire a stale transition marker");

        assert!(MessageIndexRuntimeSource::snapshot(&state).enabled);
        assert!(!MessageIndexRuntimeSource::snapshot(&state).incomplete);
        assert!(!marker_path.exists());
    }

    #[tokio::test]
    async fn dispatch_during_enable_invalidates_marker_and_rejects_publication() {
        let root = TempDir::new().expect("temporary Store root");
        let store = MessageStoreConfig {
            store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
            message_index_enable: false,
            ..MessageStoreConfig::default()
        };
        let config = ValidatedBrokerConfig::try_from_parts(BrokerConfig::default(), store)
            .expect("test broker configuration should be valid");
        let context = crate::test_service_context("concurrent-message-index-enable-test");
        let state =
            BrokerRuntimeConfigState::new_with_index_gap_marker(Arc::new(config), context.metadata_io().clone());
        state
            .initialize_index_completeness(0, Some(0))
            .await
            .expect("empty disabled Store should arm its marker");
        assert!(!dispatch_admitted(&state));
        let current = state.snapshot();
        let enable = ConfigUpdateTransaction::from_broker_patch(
            current.id(),
            current.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("true"),
            )]),
        )
        .expect("enable candidate");
        let mutation = state.lock_mutation().await;
        let error = match state.commit_message_index_enable_under_mutation(&mutation, enable, || (1, Some(0))) {
            Ok(_) => panic!("a concurrent disabled dispatch must reject enable publication"),
            Err(error) => error,
        };

        assert!(matches!(error, BrokerConfigError::RuntimeCoordination { .. }));
        assert!(!MessageIndexRuntimeSource::snapshot(&state).enabled);
        assert!(MessageIndexRuntimeSource::snapshot(&state).incomplete);
        assert_eq!(
            std::fs::read_to_string(root.path().join("config").join(super::INDEX_GAP_MARKER_FILE))
                .expect("concurrent dispatch should persist invalidity"),
            "invalid\n"
        );

        let restarted = runtime_config_state_with_marker(&root);
        restarted
            .initialize_index_completeness(1, Some(0))
            .await
            .expect("invalid marker should load after restart");
        assert!(MessageIndexRuntimeSource::snapshot(&restarted).incomplete);
    }

    #[tokio::test]
    async fn stale_disable_restores_an_absent_marker() {
        let root = TempDir::new().expect("temporary Store root");
        let state = runtime_config_state_with_marker(&root);
        let initial = state.snapshot();
        let stale_disable = ConfigUpdateTransaction::from_broker_patch(
            initial.id(),
            initial.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("false"),
            )]),
        )
        .expect("disable candidate");
        let winner = ConfigUpdateTransaction::from_broker_patch(
            initial.id(),
            initial.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("autoCreateSubscriptionGroup"),
                CheetahString::from_static_str("false"),
            )]),
        )
        .expect("independent winner");
        state.commit(winner).expect("winner should advance generation");

        let mutation = state.lock_mutation().await;
        let error = match state.commit_message_index_disable_under_mutation(&mutation, stale_disable, || 0) {
            Ok(_) => panic!("stale disable must not publish"),
            Err(error) => error,
        };
        assert!(matches!(error, BrokerConfigError::GenerationConflict { .. }));
        assert!(MessageIndexRuntimeSource::snapshot(&state).enabled);
        assert!(!MessageIndexRuntimeSource::snapshot(&state).incomplete);
        assert!(!root.path().join("config").join(super::INDEX_GAP_MARKER_FILE).exists());
    }

    #[tokio::test]
    async fn stale_disable_restores_the_exact_existing_marker() {
        let root = TempDir::new().expect("temporary Store root");
        let marker_path = root.path().join("config").join(super::INDEX_GAP_MARKER_FILE);
        std::fs::create_dir_all(marker_path.parent().expect("marker parent")).expect("create marker parent");
        std::fs::write(&marker_path, "armed:7\n").expect("seed existing marker");
        let state = runtime_config_state_with_marker(&root);
        let initial = state.snapshot();
        let stale_disable = ConfigUpdateTransaction::from_broker_patch(
            initial.id(),
            initial.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("messageIndexEnable"),
                CheetahString::from_static_str("false"),
            )]),
        )
        .expect("disable candidate");
        let winner = ConfigUpdateTransaction::from_broker_patch(
            initial.id(),
            initial.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("autoCreateSubscriptionGroup"),
                CheetahString::from_static_str("false"),
            )]),
        )
        .expect("independent winner");
        state.commit(winner).expect("winner should advance generation");

        let mutation = state.lock_mutation().await;
        let error = match state.commit_message_index_disable_under_mutation(&mutation, stale_disable, || 11) {
            Ok(_) => panic!("stale disable must not publish"),
            Err(error) => error,
        };

        assert!(matches!(error, BrokerConfigError::GenerationConflict { .. }));
        assert_eq!(
            std::fs::read_to_string(marker_path).expect("restored marker"),
            "armed:7\n"
        );
    }

    #[test]
    fn dispatch_callback_holds_the_transition_read_side_until_completion() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let dispatch_state = state.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let dispatch = std::thread::spawn(move || {
            MessageIndexRuntimeSource::with_dispatch_admission(&dispatch_state, &mut |enabled| {
                assert!(enabled);
                entered_tx.send(()).expect("signal admitted dispatch");
                release_rx.recv().expect("release admitted dispatch");
            });
        });

        entered_rx
            .recv()
            .expect("dispatch should enter its protected operation");
        assert!(
            state.index_transition.try_write().is_none(),
            "disable must not acquire the transition write side before an admitted dispatch completes"
        );
        release_tx.send(()).expect("release dispatch");
        dispatch.join().expect("dispatch thread should join");
        assert!(state.index_transition.try_write().is_some());
    }

    #[tokio::test]
    async fn mutation_permit_cannot_be_used_with_another_runtime_state() {
        let first = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let second = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let current = second.snapshot();
        let update = ConfigUpdateTransaction::from_broker_patch(
            current.id(),
            current.validated(),
            &HashMap::from([(
                CheetahString::from_static_str("autoCreateTopicEnable"),
                CheetahString::from_static_str("false"),
            )]),
        )
        .expect("valid update");
        let wrong_permit = first.lock_mutation().await;

        let error = match second.commit_under_mutation(&wrong_permit, update) {
            Ok(_) => panic!("cross-state permit must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(error, BrokerConfigError::RuntimeCoordination { .. }));
        assert_eq!(second.snapshot().id(), ConfigGeneration::INITIAL);
    }

    #[test]
    fn readers_never_observe_a_mixed_six_field_generation() {
        let state = runtime_config_state(BrokerConfig::default(), MessageStoreConfig::default());
        let barrier = Arc::new(Barrier::new(2));
        let done = Arc::new(AtomicBool::new(false));
        let writer_state = state.clone();
        let writer_barrier = Arc::clone(&barrier);
        let writer_done = Arc::clone(&done);
        let writer = std::thread::spawn(move || {
            let variants = [
                [
                    ("autoCreateTopicEnable", "false"),
                    ("autoCreateSubscriptionGroup", "false"),
                    ("brokerPermission", "4"),
                    ("defaultTopicQueueNums", "16"),
                    ("messageIndexEnable", "false"),
                    ("traceTopicEnable", "true"),
                ],
                [
                    ("autoCreateTopicEnable", "true"),
                    ("autoCreateSubscriptionGroup", "true"),
                    ("brokerPermission", "6"),
                    ("defaultTopicQueueNums", "8"),
                    ("messageIndexEnable", "true"),
                    ("traceTopicEnable", "false"),
                ],
            ];
            writer_barrier.wait();
            for index in 0..200 {
                let current = writer_state.snapshot();
                let properties = variants[index % variants.len()]
                    .into_iter()
                    .map(|(key, value)| (CheetahString::from(key), CheetahString::from(value)))
                    .collect();
                let update = ConfigUpdateTransaction::from_broker_patch(current.id(), current.validated(), &properties)
                    .expect("six-field candidate should validate");
                writer_state.commit(update).expect("serialized writer should commit");
            }
            writer_done.store(true, Ordering::Release);
        });

        barrier.wait();
        let mut last_generation = ConfigGeneration::INITIAL;
        while !done.load(Ordering::Acquire) {
            let snapshot = state.snapshot().mutation_snapshot();
            assert!(snapshot.generation >= last_generation);
            last_generation = snapshot.generation;
            let values = (
                snapshot.auto_create_topic_enable,
                snapshot.auto_create_subscription_group,
                snapshot.broker_permission,
                snapshot.default_topic_queue_nums,
                snapshot.message_index_enable,
                snapshot.trace_topic_enable,
            );
            assert!(matches!(
                values,
                (true, true, 6, 8, true, false) | (false, false, 4, 16, false, true)
            ));
        }
        writer.join().expect("writer should finish");
        assert_eq!(state.snapshot().id().value(), 201);
    }
}
