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

use rocketmq_error::DomainError;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreErrorKind;
use rocketmq_store_api::StoreOperation;

#[test]
fn storage_api_is_consumed_only_through_root_exports() {
    let source = include_str!("../src/lib.rs");
    for module in ["capability", "error", "progress"] {
        assert!(
            !source.contains(&format!("pub mod {module};")),
            "`rocketmq-store-api` implementation module `{module}` must remain private"
        );
    }

    let error =
        StoreError::new(StoreErrorKind::Storage, StoreOperation::Append).in_component(StoreComponent::CommitLog);
    assert_eq!("STORE_STORAGE_FAILED", DomainError::code(&error).as_str());
}
