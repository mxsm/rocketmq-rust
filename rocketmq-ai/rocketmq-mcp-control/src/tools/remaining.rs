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

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::Serialize;

use super::validate_user_name;
use super::NameKind;
use crate::error::ControlError;

macro_rules! operation_schema {
    ($type:ty, $name:literal, $value:literal) => {
        impl schemars::JsonSchema for $type {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                $name.into()
            }

            fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({"type": "string", "const": $value})
            }
        }
    };
}

#[path = "remaining/broker.rs"]
mod broker;
#[path = "remaining/log_filter.rs"]
mod log_filter;
#[path = "remaining/offset.rs"]
mod offset;
#[path = "remaining/proxy_drain.rs"]
mod proxy_drain;
#[path = "remaining/request_mode.rs"]
mod request_mode;

pub use broker::*;
pub use log_filter::*;
pub use offset::*;
pub use proxy_drain::*;
pub use request_mode::*;

/// Which operation holds a target, told without its identifier.
///
/// The identifier of an operation that another call started is that caller's text, so a response
/// never repeats it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperationHolder {
    /// No operation holds the target.
    None,
    /// The operation that this call names by its `operation_id`.
    ThisOperation,
    /// An operation with another identifier.
    OtherOperation,
}

fn nullable_schema<T: JsonSchema>(generator: &mut SchemaGenerator) -> Schema {
    schemars::json_schema!({
        "anyOf": [
            generator.subschema_for::<T>(),
            { "type": "null" }
        ]
    })
}

fn validate_consumer_group(group: &str) -> Result<(), ControlError> {
    validate_user_name(group, NameKind::ConsumerGroup)?;
    #[cfg(feature = "write-tools")]
    if rocketmq_admin_core::core::consumer::is_protected_consumer_group(group) {
        return Err(ControlError::invalid_argument());
    }
    Ok(())
}
