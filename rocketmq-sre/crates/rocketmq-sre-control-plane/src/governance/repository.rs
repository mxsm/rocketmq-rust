// Copyright 2026 The RocketMQ Rust Authors
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

mod registry;
mod reporting;
mod support;

use sqlx::PgPool;

pub(super) use registry::human_event;

#[derive(Clone)]
pub(super) struct GovernanceRepository {
    pub(super) pool: PgPool,
}

impl GovernanceRepository {
    pub(super) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
