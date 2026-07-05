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

use rocketmq_error::RocketMQError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieredStoreErrorKind {
    IllegalOffset,
    SegmentFull,
    SegmentClosed,
    SegmentDeleted,
    MetadataCorrupted,
    ProviderReadFailed,
    ProviderWriteFailed,
    Internal,
}

#[inline]
pub fn illegal_argument(message: impl Into<String>) -> RocketMQError {
    RocketMQError::illegal_argument(message)
}

#[inline]
pub fn storage_read_failed(path: impl Into<String>, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::storage_read_failed(path, reason)
}

#[inline]
pub fn storage_write_failed(path: impl Into<String>, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::storage_write_failed(path, reason)
}

#[inline]
pub fn storage_corrupted(path: impl Into<String>) -> RocketMQError {
    RocketMQError::StorageCorrupted { path: path.into() }
}

#[inline]
pub fn storage_out_of_space(path: impl Into<String>) -> RocketMQError {
    RocketMQError::StorageOutOfSpace { path: path.into() }
}

#[inline]
pub fn invalid_segment_type(message: impl Into<String>) -> RocketMQError {
    illegal_argument(message)
}

#[inline]
pub fn internal(message: impl Into<String>) -> RocketMQError {
    RocketMQError::Internal(message.into())
}

pub fn from_kind(kind: TieredStoreErrorKind, path: impl Into<String>, message: impl Into<String>) -> RocketMQError {
    let path = path.into();
    let message = message.into();
    match kind {
        TieredStoreErrorKind::IllegalOffset => illegal_argument(message),
        TieredStoreErrorKind::ProviderReadFailed => storage_read_failed(path, message),
        TieredStoreErrorKind::ProviderWriteFailed => storage_write_failed(path, message),
        TieredStoreErrorKind::SegmentFull => storage_out_of_space(path),
        TieredStoreErrorKind::SegmentClosed | TieredStoreErrorKind::SegmentDeleted => {
            storage_read_failed(path, message)
        }
        TieredStoreErrorKind::MetadataCorrupted => storage_corrupted(path),
        TieredStoreErrorKind::Internal => internal(message),
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::ErrorKind;

    use super::*;

    #[test]
    fn from_kind_maps_segment_full_to_storage_out_of_space() {
        let error = from_kind(TieredStoreErrorKind::SegmentFull, "tiered-segment", "full");

        assert_eq!(error.kind(), ErrorKind::StorageOutOfSpace);
    }

    #[test]
    fn from_kind_maps_metadata_corrupted_to_storage_corrupted() {
        let error = from_kind(TieredStoreErrorKind::MetadataCorrupted, "tiered-metadata", "bad json");

        assert_eq!(error.kind(), ErrorKind::StorageCorrupted);
    }

    #[test]
    fn invalid_segment_type_uses_illegal_argument() {
        let error = invalid_segment_type("index segment is not supported");

        assert_eq!(error.kind(), ErrorKind::IllegalArgument);
    }
}
