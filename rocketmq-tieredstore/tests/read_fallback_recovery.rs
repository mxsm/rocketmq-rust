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

use rocketmq_error::ErrorKind;
use rocketmq_error::RocketMQError;
use rocketmq_runtime::RuntimeContext;
use rocketmq_tieredstore::TieredLifecycle;
use rocketmq_tieredstore::TieredLocalResidency;
use rocketmq_tieredstore::TieredReadContext;
use rocketmq_tieredstore::TieredReadErrorDisposition;
use rocketmq_tieredstore::TieredReadPolicy;
use rocketmq_tieredstore::TieredReadSource;
use rocketmq_tieredstore::TieredStorageLevel;
use rocketmq_tieredstore::TieredStore;
use rocketmq_tieredstore::TieredStoreConfig;

#[test]
fn read_policy_routes_local_remote_and_forced_reads_deterministically() {
    let memory = TieredReadContext::new(TieredLocalResidency::Memory);
    let disk = TieredReadContext::new(TieredLocalResidency::Disk);
    let missing = TieredReadContext::new(TieredLocalResidency::Missing);

    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::Disable).select(missing),
        TieredReadSource::Local
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::NotInDisk).select(memory),
        TieredReadSource::Local
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::NotInDisk).select(disk),
        TieredReadSource::Local
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::NotInDisk).select(missing),
        TieredReadSource::Tiered
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::NotInMem).select(memory),
        TieredReadSource::Local
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::NotInMem).select(disk),
        TieredReadSource::Tiered
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::Force).select(memory),
        TieredReadSource::Tiered
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::Force).select(memory.force_local()),
        TieredReadSource::Local
    );
    assert_eq!(
        TieredReadPolicy::new(TieredStorageLevel::Disable).select(memory.remote_only()),
        TieredReadSource::Tiered
    );
}

#[test]
fn read_error_policy_never_turns_corruption_into_a_miss() {
    assert_eq!(
        TieredReadPolicy::classify_error(ErrorKind::StorageCorrupted, true),
        TieredReadErrorDisposition::Fatal
    );
    assert_eq!(
        TieredReadPolicy::classify_error(ErrorKind::Timeout, true),
        TieredReadErrorDisposition::FallbackToLocal
    );
    assert_eq!(
        TieredReadPolicy::classify_error(ErrorKind::Timeout, false),
        TieredReadErrorDisposition::Fatal
    );
    assert_eq!(
        TieredReadPolicy::classify_error(ErrorKind::QueryNotFound, false),
        TieredReadErrorDisposition::Miss
    );
}

#[tokio::test]
async fn incompatible_persisted_provider_contract_fails_before_recovery() -> Result<(), RocketMQError> {
    let temp_dir =
        tempfile::tempdir().map_err(|source| RocketMQError::internal("create temporary directory", source))?;
    let root = temp_dir.path().join("tieredstore");
    let metadata_path = root.join("config").join("tieredStoreMetadata.json");
    tokio::fs::create_dir_all(metadata_path.parent().expect("metadata parent"))
        .await
        .map_err(|source| RocketMQError::internal("create metadata parent", source))?;
    tokio::fs::write(
        &metadata_path,
        br#"{
          "format":"rocketmq-tiered-metadata",
          "version":1,
          "provider":{"id":"posix","configVersion":1,"format":"rocketmq-tiered-posix","version":99},
          "topics":{},"queues":{},"segments":{},"index":{}
        }"#,
    )
    .await
    .map_err(|source| RocketMQError::internal("write incompatible metadata", source))?;

    let context = RuntimeContext::from_current("tiered-provider-contract-test");
    let store = TieredStore::new(
        TieredStoreConfig {
            backend_provider: "posix".to_owned(),
            store_path_root_dir: root,
            ..TieredStoreConfig::default()
        },
        context.root_group().clone(),
    )?;
    let error = store
        .load()
        .await
        .expect_err("incompatible provider data must fail load");
    assert_eq!(error.kind(), ErrorKind::StorageCorrupted);

    Ok(())
}
