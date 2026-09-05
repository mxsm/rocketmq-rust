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

use std::collections::BTreeMap;
use std::sync::Arc;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::pop_retry_policy::PopRetryPolicy;
use rocketmq_model::common::pop_retry_policy::PopRetryTopicVersion;
use rocketmq_model::PopRetryPolicyOutcome;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store_rocksdb::profile_marker::pop_consumer_profile_key;
use rocketmq_store_rocksdb::profile_marker::PopConsumerProfileMarker;
use rocketmq_store_rocksdb::profile_marker::POP_CONSUMER_PROFILE_FORMAT_VERSION;
use serde::Deserialize;
use serde::Serialize;

use super::rocksdb_store::broker_storage_error;
use super::rocksdb_store::PopConsumerProfileState;
use super::rocksdb_store::PopConsumerRocksDbStore;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PopConsumerProfile {
    pub(crate) group: CheetahString,
    pub(crate) subscriptions: Vec<SubscriptionData>,
    pub(crate) retry_version: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retry_policy: Option<PopRetryPolicy>,
    pub(crate) generation: u64,
    pub(crate) last_seen: i64,
    pub(crate) format_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "recordType", rename_all = "camelCase")]
enum StoredProfileRecord {
    Profile {
        profile: PopConsumerProfile,
    },
    Tombstone {
        group: CheetahString,
        generation: u64,
        last_seen: i64,
        format_version: u32,
    },
}

#[derive(Debug, Default)]
struct ProfileState {
    generation: u64,
    profiles: BTreeMap<CheetahString, PopConsumerProfile>,
}

pub(crate) struct PopConsumerProfileStore {
    rocksdb: Arc<PopConsumerRocksDbStore>,
    capacity: usize,
    state: Mutex<ProfileState>,
}

impl PopConsumerProfileStore {
    pub(crate) fn load(rocksdb: Arc<PopConsumerRocksDbStore>, capacity: usize) -> Result<Self, RocketMQError> {
        if capacity == 0 {
            return Err(invalid_profile("capacity", "0", "capacity must be greater than zero"));
        }
        let PopConsumerProfileState { marker, records } = rocksdb.load_profile_state()?;
        if marker.is_none() && !records.is_empty() {
            return Err(codec_error("profile records exist without a format marker"));
        }
        let generation = marker
            .as_deref()
            .map(|marker| PopConsumerProfileMarker::decode(marker).map_err(broker_storage_error))
            .transpose()?
            .map_or(0, |marker| marker.generation);
        let mut profiles = BTreeMap::new();
        for (key, value) in records {
            let record: StoredProfileRecord = serde_json::from_slice(&value)
                .map_err(|error| codec_error(format!("profile record JSON is invalid: {error}")))?;
            match record {
                StoredProfileRecord::Profile { mut profile } => {
                    normalize_retry_policy(&mut profile)?;
                    validate_profile(&profile)?;
                    if profile.generation > generation {
                        return Err(codec_error("profile generation is newer than the format marker"));
                    }
                    if pop_consumer_profile_key(profile.group.as_str()).map_err(broker_storage_error)? != key {
                        return Err(codec_error("profile key does not match the encoded group"));
                    }
                    profiles.insert(profile.group.clone(), profile);
                }
                StoredProfileRecord::Tombstone {
                    group,
                    generation: tombstone_generation,
                    format_version,
                    ..
                } => {
                    validate_format_version(format_version)?;
                    if tombstone_generation > generation
                        || pop_consumer_profile_key(group.as_str()).map_err(broker_storage_error)? != key
                    {
                        return Err(codec_error("invalid POP consumer profile tombstone"));
                    }
                    profiles.remove(&group);
                }
            }
        }
        if profiles.len() > capacity {
            return Err(invalid_profile(
                "capacity",
                capacity.to_string(),
                "persisted POP consumer profiles exceed configured capacity",
            ));
        }
        Ok(Self {
            rocksdb,
            capacity,
            state: Mutex::new(ProfileState { generation, profiles }),
        })
    }

    pub(crate) fn upsert(
        &self,
        group: CheetahString,
        mut subscriptions: Vec<SubscriptionData>,
        retry_policy: PopRetryPolicy,
        last_seen: i64,
    ) -> Result<PopConsumerProfile, RocketMQError> {
        if group.is_empty() {
            return Err(invalid_profile("group", "", "group must not be empty"));
        }
        if subscriptions.is_empty() {
            return Err(invalid_profile(
                "subscriptions",
                "[]",
                "at least one validated subscription is required",
            ));
        }
        subscriptions.sort_by(|left, right| left.topic.cmp(&right.topic));
        subscriptions.dedup_by(|left, right| left.topic == right.topic);
        let mut state = self.state.lock();
        if !state.profiles.contains_key(&group) && state.profiles.len() >= self.capacity {
            return Err(invalid_profile(
                "capacity",
                self.capacity.to_string(),
                "POP consumer profile capacity has been reached",
            ));
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| codec_error("POP consumer profile generation overflow"))?;
        let retry_policy = next_retry_policy(state.profiles.get(&group), retry_policy, generation)?;
        let profile = PopConsumerProfile {
            group: group.clone(),
            subscriptions,
            retry_version: retry_policy.write_version.number(),
            retry_policy: Some(retry_policy),
            generation,
            last_seen,
            format_version: POP_CONSUMER_PROFILE_FORMAT_VERSION,
        };
        validate_profile(&profile)?;
        let marker = PopConsumerProfileMarker::new(generation)
            .encode()
            .map_err(broker_storage_error)?;
        let value = serde_json::to_vec(&StoredProfileRecord::Profile {
            profile: profile.clone(),
        })
        .map_err(|error| codec_error(format!("profile record encode failed: {error}")))?;
        self.rocksdb.write_profile_record(
            marker,
            pop_consumer_profile_key(group.as_str()).map_err(broker_storage_error)?,
            value,
        )?;
        state.generation = generation;
        state.profiles.insert(group, profile.clone());
        Ok(profile)
    }

    pub(crate) fn remove(&self, group: &CheetahString, last_seen: i64) -> Result<bool, RocketMQError> {
        let mut state = self.state.lock();
        if !state.profiles.contains_key(group) {
            return Ok(false);
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| codec_error("POP consumer profile generation overflow"))?;
        let marker = PopConsumerProfileMarker::new(generation)
            .encode()
            .map_err(broker_storage_error)?;
        let value = serde_json::to_vec(&StoredProfileRecord::Tombstone {
            group: group.clone(),
            generation,
            last_seen,
            format_version: POP_CONSUMER_PROFILE_FORMAT_VERSION,
        })
        .map_err(|error| codec_error(format!("profile tombstone encode failed: {error}")))?;
        self.rocksdb.write_profile_record(
            marker,
            pop_consumer_profile_key(group.as_str()).map_err(broker_storage_error)?,
            value,
        )?;
        state.generation = generation;
        state.profiles.remove(group);
        Ok(true)
    }

    pub(crate) fn snapshot(&self) -> Vec<PopConsumerProfile> {
        self.state.lock().profiles.values().cloned().collect()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state.lock().generation
    }

    pub(crate) fn retry_policy(&self, group: &CheetahString) -> Option<PopRetryPolicy> {
        self.state
            .lock()
            .profiles
            .get(group)
            .and_then(|profile| profile.retry_policy.clone())
    }
}

fn validate_profile(profile: &PopConsumerProfile) -> Result<(), RocketMQError> {
    validate_format_version(profile.format_version)?;
    pop_consumer_profile_key(profile.group.as_str()).map_err(broker_storage_error)?;
    if profile.subscriptions.is_empty() {
        return Err(invalid_profile(
            "subscriptions",
            "[]",
            "persisted profile must contain at least one subscription",
        ));
    }
    if profile
        .subscriptions
        .iter()
        .any(|subscription| subscription.topic.is_empty())
    {
        return Err(invalid_profile(
            "subscriptions.topic",
            "",
            "persisted subscription topic must not be empty",
        ));
    }
    let retry_policy = profile
        .retry_policy
        .as_ref()
        .ok_or_else(|| codec_error("persisted POP profile is missing retry policy"))?;
    retry_policy
        .state()
        .map_err(|error| codec_error(format!("invalid persisted POP retry policy: {error}")))?;
    if retry_policy.generation != profile.generation || retry_policy.write_version.number() != profile.retry_version {
        return Err(codec_error(
            "persisted POP retry policy generation/version does not match the profile",
        ));
    }
    Ok(())
}

fn normalize_retry_policy(profile: &mut PopConsumerProfile) -> Result<(), RocketMQError> {
    if profile.retry_policy.is_none() {
        profile.retry_policy = Some(match PopRetryTopicVersion::from_number(profile.retry_version) {
            Some(PopRetryTopicVersion::V1) => PopRetryPolicy::v1_only(profile.generation),
            Some(PopRetryTopicVersion::V2) => PopRetryPolicy::dual_read_v2_write(profile.generation),
            None => {
                return Err(invalid_profile(
                    "retryVersion",
                    profile.retry_version.to_string(),
                    "retry version must be 1 or 2",
                ));
            }
        });
    }
    Ok(())
}

fn next_retry_policy(
    existing: Option<&PopConsumerProfile>,
    requested: PopRetryPolicy,
    generation: u64,
) -> Result<PopRetryPolicy, RocketMQError> {
    let requested_state = requested.state().map_err(|error| {
        invalid_profile(
            "retryPolicy",
            error.to_string(),
            "a supported POP retry migration state",
        )
    })?;
    let Some(existing) = existing else {
        return Ok(PopRetryPolicy::for_state(requested_state, generation));
    };
    let current = existing
        .retry_policy
        .as_ref()
        .ok_or_else(|| codec_error("existing POP profile is missing retry policy"))?;
    if current.state().map_err(|error| codec_error(error.to_string()))? == requested_state {
        return Ok(PopRetryPolicy::for_state(requested_state, generation));
    }
    match current
        .transition_to(requested_state, generation)
        .map_err(|_| codec_error("existing POP retry policy is invalid"))?
    {
        PopRetryPolicyOutcome::Transitioned(policy) => Ok(policy),
        PopRetryPolicyOutcome::Rejected => Err(invalid_profile(
            "retryPolicy",
            format!("{requested_state:?}"),
            "the next safe POP retry migration state",
        )),
    }
}

fn validate_format_version(format_version: u32) -> Result<(), RocketMQError> {
    if format_version != POP_CONSUMER_PROFILE_FORMAT_VERSION {
        return Err(invalid_profile(
            "formatVersion",
            format_version.to_string(),
            format!("unsupported POP consumer profile format version {format_version}"),
        ));
    }
    Ok(())
}

fn invalid_profile(key: &'static str, value: impl Into<String>, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key,
        value: value.into(),
        reason: reason.into(),
    }
}

fn codec_error(reason: impl Into<String>) -> RocketMQError {
    RocketMQError::deserialization_failed("POP consumer profile", reason.into())
}
