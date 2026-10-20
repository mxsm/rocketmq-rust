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
use crate::model::StorageEngine;
use crate::persistence::file_store::FileDashboardStore;
use crate::persistence::sql_store::SqlDashboardStore;

/// Owns the initialized File or SQL store for durable Dashboard data.
pub enum DashboardDataStore {
    File(Box<FileDashboardStore>),
    Sql(Box<SqlDashboardStore>),
}

impl DashboardDataStore {
    pub const fn storage_backend(&self) -> StorageEngine {
        match self {
            Self::File(_) => StorageEngine::File,
            Self::Sql(store) => store.storage_backend(),
        }
    }
}
