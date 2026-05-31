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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RocksDbScanItem {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RocksDbScanOptions {
    pub cf: String,
    pub prefix: Vec<u8>,
    pub limit: usize,
}

impl RocksDbScanOptions {
    pub fn prefix(cf: impl Into<String>, prefix: impl Into<Vec<u8>>, limit: usize) -> Self {
        Self {
            cf: cf.into(),
            prefix: prefix.into(),
            limit,
        }
    }
}
