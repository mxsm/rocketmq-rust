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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RocksDbBatchOperation {
    Put { cf: String, key: Vec<u8>, value: Vec<u8> },
    Delete { cf: String, key: Vec<u8> },
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RocksDbWriteBatch {
    operations: Vec<RocksDbBatchOperation>,
}

impl RocksDbWriteBatch {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            operations: Vec::with_capacity(capacity),
        }
    }

    pub fn put_cf(&mut self, cf: impl Into<String>, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.operations.push(RocksDbBatchOperation::Put {
            cf: cf.into(),
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete_cf(&mut self, cf: impl Into<String>, key: impl Into<Vec<u8>>) {
        self.operations.push(RocksDbBatchOperation::Delete {
            cf: cf.into(),
            key: key.into(),
        });
    }

    pub fn operations(&self) -> &[RocksDbBatchOperation] {
        &self.operations
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}
