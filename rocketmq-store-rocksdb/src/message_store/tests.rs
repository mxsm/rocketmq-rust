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

use super::*;

#[derive(Debug, thiserror::Error)]
#[error("private local source")]
struct LocalCause;

#[test]
fn normalize_index_query_time_range_uses_configured_query_days() {
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should follow Unix epoch")
        .as_millis() as i64;
    let (begin, end) = normalize_index_query_time_range(0, i64::MAX, 2);
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should follow Unix epoch")
        .as_millis() as i64;
    assert!(end >= before && end <= after);
    assert_eq!(end - begin, 2 * MILLIS_PER_DAY);
}

#[test]
fn local_store_error_is_forwarded_without_a_second_storage_wrapper() {
    let source = StoreError::new(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, StoreOperation::Append)
        .in_component(StoreComponent::CommitLog)
        .with_source(LocalCause);

    let error = lifecycle_error(StoreOperation::Shutdown, RocksDbMessageStoreError::Store(source));

    assert_eq!(&rocketmq_error::STORAGE_OPERATION_TIMED_OUT, error.descriptor());
    assert_eq!(StoreOperation::Append, error.operation());
    assert_eq!(StoreComponent::CommitLog, error.component());
    assert!(std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<LocalCause>())
        .is_some());
}

#[test]
fn lifecycle_leaf_mapping_retains_descriptor_context_source_and_redaction() {
    let config = lifecycle_error(
        StoreOperation::Start,
        RocksDbMessageStoreError::Violation(RocksDbMessageStoreViolation::InvalidConfiguration),
    );
    assert_eq!(&rocketmq_error::STORAGE_REQUEST_INVALID, config.descriptor());
    assert_eq!(StoreOperation::Start, config.operation());
    assert_eq!(StoreComponent::Configuration, config.component());
    assert!(std::error::Error::source(&config).is_none());
    assert!(config
        .public_view()
        .expect("valid public view")
        .fields()
        .next()
        .is_none());
    assert!(!config.to_string().contains("private configuration"));
    assert!(!format!("{config:?}").contains("private configuration"));

    let backend = lifecycle_error(
        StoreOperation::Shutdown,
        RocksDbMessageStoreError::Io(std::io::Error::other("private backend source")),
    );
    assert_eq!(&rocketmq_error::STORAGE_IO_FAILED, backend.descriptor());
    assert_eq!(StoreOperation::Shutdown, backend.operation());
    assert_eq!(StoreComponent::RocksDb, backend.component());
    assert!(std::error::Error::source(&backend)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some());
    assert!(!backend.to_string().contains("private backend source"));
    assert!(!format!("{backend:?}").contains("private backend source"));
}
