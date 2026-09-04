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

use bytes::Bytes;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;

use crate::error::rocksdb_contract_error;
use crate::error::RocksDbStoreResultExt;

pub struct RocksDbSnapshot<'a> {
    db: &'a ::rocksdb::DB,
    snapshot: ::rocksdb::Snapshot<'a>,
}

impl<'a> RocksDbSnapshot<'a> {
    pub(crate) fn new(db: &'a ::rocksdb::DB) -> Self {
        Self {
            db,
            snapshot: db.snapshot(),
        }
    }

    pub fn get_cf(&self, operation: StoreOperation, cf: &str, key: &[u8]) -> Result<Option<Bytes>, StoreError> {
        let handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| rocksdb_contract_error(&rocketmq_error::STORAGE_STATE_CORRUPTED, operation))?;
        let mut read_options = ::rocksdb::ReadOptions::default();
        read_options.set_snapshot(&self.snapshot);
        self.db
            .get_cf_opt(&handle, key, &read_options)
            .map(|value| value.map(Bytes::from))
            .map_store(&rocketmq_error::STORAGE_READ_FAILED, operation)
    }
}
