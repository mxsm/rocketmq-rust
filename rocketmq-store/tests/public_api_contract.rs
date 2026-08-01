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

use rocketmq_store::LocalFileMessageStore;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::StoreError;
use rocketmq_store::StoreFactory;
use rocketmq_store::StoreFactoryError;

#[test]
fn store_consumers_use_only_intentional_root_exports() {
    let source = include_str!("../src/lib.rs").replace("\r\n", "\n");
    assert!(
        source.contains("#[cfg(any(test, feature = \"test-support\"))]\npub mod test_support;"),
        "Store test fixtures must require the explicit test-support feature"
    );
    assert!(
        !source.contains("bench_support"),
        "the retired production bench_support path must stay deleted"
    );

    for module in [
        "base",
        "capability",
        "config",
        "consume_queue",
        "factory",
        "filter",
        "ha",
        "hook",
        "index",
        "inspection",
        "kv",
        "log_file",
        "message_store",
        "platform",
        "pop",
        "queue",
        "rocksdb",
        "stats",
        "store",
        "store_error",
        "store_path_config_helper",
        "tieredstore",
        "timer",
        "transfer",
        "utils",
    ] {
        assert!(
            !source.contains(&format!("pub mod {module};")),
            "`rocketmq-store` implementation module `{module}` must remain private"
        );
    }

    let _ = MessageStoreConfig::default();
    let _: Option<LocalFileMessageStore> = None;
    let _: Option<StoreError> = None;
    let _: Option<StoreFactoryError> = None;
    let _ = std::mem::size_of::<StoreFactory>();
}
