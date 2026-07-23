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

use super::local_file_message_store::LocalFileMessageStore;
#[cfg(feature = "rocksdb_store")]
use super::rocksdb_message_store::RocksDBMessageStore;

/// Application composition root that exclusively owns the selected Store backend.
///
/// Unlike [`super::GenericMessageStore`], this type does not add an unsafe
/// shared-mutation wrapper around the concrete backend. Shared consumers should
/// receive narrow Store capabilities while the application lifecycle retains the
/// only mutable root owner.
#[doc(hidden)]
#[non_exhaustive]
pub enum OwnedMessageStore {
    LocalFileStore(Box<LocalFileMessageStore>),

    #[cfg(feature = "rocksdb_store")]
    RocksDBStore(Box<RocksDBMessageStore>),
}

impl OwnedMessageStore {
    pub fn local_file(store: LocalFileMessageStore) -> Self {
        Self::LocalFileStore(Box::new(store))
    }

    #[cfg(feature = "rocksdb_store")]
    pub fn rocksdb(store: RocksDBMessageStore) -> Self {
        Self::RocksDBStore(Box::new(store))
    }
}
