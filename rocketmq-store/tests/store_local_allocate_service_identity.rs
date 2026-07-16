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

use rocketmq_store::base::allocate_mapped_file_service::AllocateMappedFileService as LegacyAllocateService;
use rocketmq_store::config::message_store_config::MessageStoreConfig;
use rocketmq_store_local::base::allocate_mapped_file_service::AllocateMappedFileService as CanonicalAllocateService;

fn canonical_service(service: LegacyAllocateService) -> CanonicalAllocateService {
    service
}

#[test]
fn legacy_allocation_service_path_is_the_local_canonical_type() {
    let config = MessageStoreConfig {
        warm_mapped_file_enable: true,
        mapped_file_size_commit_log: 1024,
        ..MessageStoreConfig::default()
    };

    let service = LegacyAllocateService::new_with_message_store_config(None, false, false, &config);
    let service = canonical_service(service);

    assert_eq!(service.get_service_name(), "AllocateMappedFileService");
    assert!(!service.is_started());
}
