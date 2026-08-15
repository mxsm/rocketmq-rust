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
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store_rocksdb::profile_marker::pop_consumer_profile_key;
use rocketmq_store_rocksdb::profile_marker::PopConsumerProfileMarker;
use rocketmq_store_rocksdb::profile_marker::POP_CONSUMER_PROFILE_FORMAT_VERSION;
use serde::Deserialize;
use serde::Serialize;

use super::rocksdb_store::PopConsumerProfileState;
use super::rocksdb_store::PopConsumerRocksDbStore;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PopConsumerProfile {
    pub(crate) group: CheetahString,
    pub(crate) subscriptions: Vec<SubscriptionData>,
    pub(crate) retry_version: i32,
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
            .map(PopConsumerProfileMarker::decode)
            .transpose()?
            .map_or(0, |marker| marker.generation);
        let mut profiles = BTreeMap::new();
        for (key, value) in records {
            let record: StoredProfileRecord = serde_json::from_slice(&value)
                .map_err(|error| codec_error(format!("profile record JSON is invalid: {error}")))?;
            match record {
                StoredProfileRecord::Profile { profile } => {
                    validate_profile(&profile)?;
                    if profile.generation > generation {
                        return Err(codec_error("profile generation is newer than the format marker"));
                    }
                    if pop_consumer_profile_key(profile.group.as_str())? != key {
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
                    if tombstone_generation > generation || pop_consumer_profile_key(group.as_str())? != key {
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
        retry_version: i32,
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
        let profile = PopConsumerProfile {
            group: group.clone(),
            subscriptions,
            retry_version,
            generation,
            last_seen,
            format_version: POP_CONSUMER_PROFILE_FORMAT_VERSION,
        };
        validate_profile(&profile)?;
        let marker = PopConsumerProfileMarker::new(generation).encode()?;
        let value = serde_json::to_vec(&StoredProfileRecord::Profile {
            profile: profile.clone(),
        })
        .map_err(|error| codec_error(format!("profile record encode failed: {error}")))?;
        self.rocksdb
            .write_profile_record(marker, pop_consumer_profile_key(group.as_str())?, value)?;
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
        let marker = PopConsumerProfileMarker::new(generation).encode()?;
        let value = serde_json::to_vec(&StoredProfileRecord::Tombstone {
            group: group.clone(),
            generation,
            last_seen,
            format_version: POP_CONSUMER_PROFILE_FORMAT_VERSION,
        })
        .map_err(|error| codec_error(format!("profile tombstone encode failed: {error}")))?;
        self.rocksdb
            .write_profile_record(marker, pop_consumer_profile_key(group.as_str())?, value)?;
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
}

fn validate_profile(profile: &PopConsumerProfile) -> Result<(), RocketMQError> {
    validate_format_version(profile.format_version)?;
    pop_consumer_profile_key(profile.group.as_str())?;
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
    Ok(())
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
