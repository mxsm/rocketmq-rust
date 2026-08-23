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

use super::SqlPersistence;
use crate::config::SqlPoolConfig;
use crate::config::StorageConfig;
use crate::model::EnvironmentId;
use crate::model::MetricSample;
use crate::model::StorageBackend;
use crate::persistence::TimeRange;
use crate::persistence::history_repository::HistoryQuery;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

#[test]
#[ignore = "requires docker-compose.storage-test.yml"]
fn docker_sqlite_history_reopens_a_mounted_database() {
    let data_path = std::env::var("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH")
        .expect("ROCKETMQ_DASHBOARD_STORAGE_TEST_SQLITE_PATH must be set by the storage test runner");
    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    owner.block_on(async {
        let config = StorageConfig {
            backend: StorageBackend::Sqlite,
            data_path: data_path.into(),
            database_url: None,
            pool: SqlPoolConfig::default(),
        };
        let environment_id = EnvironmentId::new();
        let sample = MetricSample {
            environment_id: environment_id.clone(),
            metric: "broker-count".to_string(),
            bucket_ms: 1_000,
            dimensions: Vec::new(),
            value: 3.0,
        };
        let store = SqlPersistence::initialize(&config, owner.root_context().component("docker-history-sqlite"))
            .await
            .expect("SQLite persistence");
        let lease = store
            .acquire_history_lease(&environment_id, "test-holder", 30_000)
            .await
            .expect("lease query")
            .expect("lease acquired");
        store
            .append_history(vec![sample.clone()], Some(&lease))
            .await
            .expect("append history");
        drop(store);

        let reopened =
            SqlPersistence::initialize(&config, owner.root_context().component("docker-history-sqlite-reopen"))
                .await
                .expect("reopen mounted SQLite persistence");
        let page = reopened
            .query_history(HistoryQuery {
                environment_id,
                metric: "broker-count".to_string(),
                range: TimeRange {
                    start_ms: 0,
                    end_ms: 2_000,
                },
                dimensions: Vec::new(),
                limit: 10,
                cursor: None,
            })
            .await
            .expect("read reopened history");
        assert_eq!(page.samples, vec![sample]);
    });
    owner.shutdown_runtime_blocking().expect("runtime shutdown");
}
