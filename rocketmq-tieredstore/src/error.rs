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

use rocketmq_error::ErrorDescriptor;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

pub(crate) fn contract_error(descriptor: &'static ErrorDescriptor, operation: StoreOperation) -> StoreError {
    StoreError::new(descriptor, operation).in_component(StoreComponent::TieredStore)
}

pub(crate) fn source_error(
    descriptor: &'static ErrorDescriptor,
    operation: StoreOperation,
    source: impl std::error::Error + Send + Sync + 'static,
) -> StoreError {
    StoreError::new(descriptor, operation)
        .in_component(StoreComponent::TieredStore)
        .with_source(source)
}

pub(crate) fn request_invalid(operation: StoreOperation) -> StoreError {
    contract_error(&rocketmq_error::STORAGE_REQUEST_INVALID, operation)
}

pub(crate) fn write_failed(
    operation: StoreOperation,
    source: impl std::error::Error + Send + Sync + 'static,
) -> StoreError {
    source_error(&rocketmq_error::STORAGE_WRITE_FAILED, operation, source)
}

pub(crate) fn io_failed(operation: StoreOperation, source: std::io::Error) -> StoreError {
    source_error(&rocketmq_error::STORAGE_IO_FAILED, operation, source)
}

pub(crate) fn state_corrupted(operation: StoreOperation) -> StoreError {
    contract_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation)
}

pub(crate) fn state_corrupted_source(
    operation: StoreOperation,
    source: impl std::error::Error + Send + Sync + 'static,
) -> StoreError {
    source_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation, source)
}

pub(crate) fn capacity_exhausted(operation: StoreOperation) -> StoreError {
    contract_error(&rocketmq_error::STORAGE_CAPACITY_EXHAUSTED, operation)
}

pub(crate) fn unavailable(operation: StoreOperation) -> StoreError {
    contract_error(&rocketmq_error::STORAGE_BACKEND_UNAVAILABLE, operation)
}

pub(crate) fn unsupported(operation: StoreOperation) -> StoreError {
    contract_error(&rocketmq_error::STORAGE_OPERATION_UNSUPPORTED, operation)
}

pub(crate) fn internal_failure(operation: StoreOperation) -> StoreError {
    contract_error(&rocketmq_error::STORAGE_INTERNAL_FAILURE, operation)
}

pub(crate) fn runtime_error(operation: StoreOperation, source: rocketmq_runtime::RuntimeError) -> StoreError {
    let descriptor = if source.code() == rocketmq_error::RUNTIME_BUILD_FAILED.code()
        || source.code() == rocketmq_error::RUNTIME_IO_FAILED.code()
    {
        &rocketmq_error::STORAGE_IO_FAILED
    } else {
        match source.condition() {
            rocketmq_error::CanonicalCondition::InvalidArgument => &rocketmq_error::STORAGE_REQUEST_INVALID,
            rocketmq_error::CanonicalCondition::Unavailable => &rocketmq_error::STORAGE_BACKEND_UNAVAILABLE,
            rocketmq_error::CanonicalCondition::ResourceExhausted => &rocketmq_error::STORAGE_CAPACITY_EXHAUSTED,
            rocketmq_error::CanonicalCondition::DeadlineExceeded => &rocketmq_error::STORAGE_OPERATION_TIMED_OUT,
            rocketmq_error::CanonicalCondition::Unimplemented => &rocketmq_error::STORAGE_OPERATION_UNSUPPORTED,
            _ => &rocketmq_error::STORAGE_INTERNAL_FAILURE,
        }
    };
    source_error(descriptor, operation, source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_build_failure_maps_to_storage_io_before_unavailable() {
        let error = runtime_error(
            StoreOperation::Load,
            rocketmq_runtime::RuntimeError::build(
                rocketmq_runtime::RuntimeOperation::BuildTokioRuntime,
                std::io::Error::other("injected build failure"),
            ),
        );

        assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_IO_FAILED);
        assert!(std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<rocketmq_runtime::RuntimeError>())
            .is_some());
    }
}
