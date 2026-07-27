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

use chrono::DateTime;
use chrono::Utc;
use std::future::Future;
use std::pin::Pin;

use super::AgentActionHandler;
use crate::ExecutionAgentError;

/// Closed logger-level mutation accepted by the configuration client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoggerLevelTtlWrite {
    pub component: String,
    pub level: String,
    pub expires_at: DateTime<Utc>,
    pub operation_id: String,
}

/// Narrow configuration writer. It has no generic key/value method.
pub trait ConfigWriteClient: Send + Sync {
    fn set_logger_level_ttl<'a>(
        &'a self,
        request: &'a LoggerLevelTtlWrite,
    ) -> Pin<Box<dyn Future<Output = Result<(), ExecutionAgentError>> + Send + 'a>>;
}

/// Typed configuration-system driver.
pub trait ConfigDriver: AgentActionHandler {}
