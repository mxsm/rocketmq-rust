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

use rocketmq_store_api::StoreError;
use serde::Deserialize;
use serde::Serialize;

pub const POP_CONSUMER_PROFILE_COLUMN_FAMILY: &str = "popConsumerProfile";
pub const POP_CONSUMER_PROFILE_MARKER_KEY: &[u8] = b"__format_marker";
pub const POP_CONSUMER_PROFILE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PopConsumerProfileMarker {
    pub format_version: u32,
    pub generation: u64,
}

impl PopConsumerProfileMarker {
    pub fn new(generation: u64) -> Self {
        Self {
            format_version: POP_CONSUMER_PROFILE_FORMAT_VERSION,
            generation,
        }
    }

    pub fn encode(self) -> Result<Vec<u8>, StoreError> {
        serde_json::to_vec(&self).map_err(profile_invalid_codec)
    }

    pub fn decode(body: &[u8]) -> Result<Self, StoreError> {
        let marker: Self = serde_json::from_slice(body).map_err(profile_invalid_codec)?;
        if marker.format_version != POP_CONSUMER_PROFILE_FORMAT_VERSION {
            return Err(crate::error::codec_corrupted(rocketmq_store_api::StoreOperation::Admin));
        }
        Ok(marker)
    }
}

pub fn pop_consumer_profile_key(group: &str) -> Result<Vec<u8>, StoreError> {
    if group.is_empty() || group.as_bytes().contains(&b'/') {
        return Err(crate::error::request_invalid(rocketmq_store_api::StoreOperation::Admin));
    }
    Ok(format!("{POP_CONSUMER_PROFILE_FORMAT_VERSION}/{group}").into_bytes())
}

pub fn pop_consumer_profile_prefix() -> Vec<u8> {
    format!("{POP_CONSUMER_PROFILE_FORMAT_VERSION}/").into_bytes()
}

fn profile_invalid_codec(error: serde_json::Error) -> StoreError {
    StoreError::new(
        &rocketmq_error::STORAGE_STATE_CORRUPTED,
        rocketmq_store_api::StoreOperation::Admin,
    )
    .in_component(rocketmq_store_api::StoreComponent::RocksDb)
    .with_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_and_key_codec_are_versioned() {
        let marker = PopConsumerProfileMarker::new(9);
        let encoded = marker.encode().expect("marker encodes");

        assert_eq!(
            PopConsumerProfileMarker::decode(&encoded).expect("marker decodes"),
            marker
        );
        assert_eq!(pop_consumer_profile_key("group-a").expect("profile key"), b"1/group-a");
        assert_eq!(pop_consumer_profile_prefix(), b"1/");
    }

    #[test]
    fn unknown_marker_version_fails_closed() {
        let error = PopConsumerProfileMarker::decode(br#"{"formatVersion":2,"generation":1}"#)
            .expect_err("unknown version must fail");

        assert_eq!(&rocketmq_error::STORAGE_STATE_CORRUPTED, error.descriptor());
        assert_eq!(rocketmq_store_api::StoreOperation::Admin, error.operation());
        assert!(std::error::Error::source(&error).is_none());
    }
}
