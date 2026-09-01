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
pub(crate) enum TransferFailure {
    #[error("transfer I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransferViolation {
    InvalidInput(String),
    SegmentSelection(String),
    UnsupportedSegmentSource(&'static str),
}

impl TransferFailure {
    pub(crate) fn into_store_error(self, operation: StoreOperation, component: StoreComponent) -> StoreError {
        StoreError::new(&rocketmq_error::STORAGE_IO_FAILED, operation)
            .in_component(component)
            .with_source(self)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;

    use super::*;

    #[test]
    fn transfer_io_maps_with_its_typed_cause() {
        let error = TransferFailure::Io(io::Error::new(io::ErrorKind::WriteZero, "write stalled"))
            .into_store_error(StoreOperation::Replicate, StoreComponent::HighAvailability);

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_IO_FAILED);
        let transfer = error
            .source()
            .and_then(|source| source.downcast_ref::<TransferFailure>())
            .expect("transfer failure remains typed");
        assert_eq!(
            transfer
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .map(io::Error::kind),
            Some(io::ErrorKind::WriteZero)
        );
    }
}
