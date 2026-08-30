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

use chrono::Utc;

use crate::ControlPlaneError;
use crate::PostgresRepository;

/// Materializes idempotent operator todos without mutating knowledge or
/// RocketMQ resources.
pub(crate) async fn materialize_due_operator_todos(repository: &PostgresRepository) -> Result<u64, ControlPlaneError> {
    repository.materialize_due_todos(Utc::now()).await
}
