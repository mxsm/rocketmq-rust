// Copyright 2026 The RocketMQ Rust Authors
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

use std::fs;
use std::sync::Arc;

use rocketmq_tieredstore::JsonMetadataStore;
use rocketmq_tieredstore::TieredMetadataStore;
use rocketmq_tieredstore::TieredStoreConfig;

#[tokio::test]
async fn tiered_v1_metadata_reopens_and_unknown_version_fails_without_rewrite() {
    let temp = tempfile::tempdir().expect("tempdir");
    let config = Arc::new(TieredStoreConfig {
        store_path_root_dir: temp.path().to_path_buf(),
        ..TieredStoreConfig::default()
    });
    let writer = JsonMetadataStore::new(Arc::clone(&config));
    writer.persist().await.expect("persist v1 metadata");
    drop(writer);

    let reader = JsonMetadataStore::new(Arc::clone(&config));
    reader.load().await.expect("read current metadata");
    drop(reader);

    let path = temp.path().join("config/tieredStoreMetadata.json");
    let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path).expect("read metadata")).unwrap();
    value["version"] = serde_json::json!(99);
    let incompatible = serde_json::to_vec_pretty(&value).expect("encode future fixture");
    fs::write(&path, &incompatible).expect("write future fixture");

    JsonMetadataStore::new(config)
        .load()
        .await
        .expect_err("future metadata must fail closed");
    assert_eq!(fs::read(path).expect("metadata remains readable"), incompatible);
}
