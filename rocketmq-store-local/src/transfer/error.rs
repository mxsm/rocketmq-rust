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

use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferError {
    #[error("invalid transfer input: {0}")]
    InvalidInput(String),
    #[error("commitlog segment selection failed: {0}")]
    SegmentSelection(String),
    #[error("unsupported transfer segment source: {0}")]
    UnsupportedSegmentSource(&'static str),
    #[error("transfer I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl TransferError {
    /// Promotes this leaf into the canonical storage facade exactly once.
    ///
    /// Transfer I/O keeps its typed source; input, selection, and
    /// unsupported-source violations are invalid requests. Replication owners
    /// report the high-availability component, while read owners report the
    /// commit log.
    pub(crate) fn into_store_error(self, operation: StoreOperation) -> StoreError {
        let descriptor = match &self {
            Self::Io(_) => &rocketmq_error::STORAGE_IO_FAILED,
            Self::InvalidInput(_) | Self::SegmentSelection(_) | Self::UnsupportedSegmentSource(_) => {
                &rocketmq_error::STORAGE_REQUEST_INVALID
            }
        };
        let component = if matches!(operation, StoreOperation::Replicate) {
            StoreComponent::HighAvailability
        } else {
            StoreComponent::CommitLog
        };
        StoreError::new(descriptor, operation)
            .in_component(component)
            .with_source(self)
    }
}
