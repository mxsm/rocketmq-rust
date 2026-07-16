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

use rocketmq_store_local::mapped_file::queue_storage::MappedFileQueueStorage;

#[test]
fn queue_storage_preserves_path_size_and_collection_identity() {
    let storage = MappedFileQueueStorage::new("root/commitlog".to_owned(), 1024, vec![1_u64, 2]);

    assert_eq!(storage.store_path(), "root/commitlog");
    assert_eq!(storage.mapped_file_size(), 1024);
    assert_eq!(storage.mapped_files(), &[1, 2]);
}

#[test]
fn queue_storage_supports_backend_owned_interior_mutability() {
    let storage = MappedFileQueueStorage::new("root/cq".to_owned(), 20, std::cell::Cell::new(1_u64));

    storage.mapped_files().set(2);
    assert_eq!(storage.mapped_files().get(), 2);
}
