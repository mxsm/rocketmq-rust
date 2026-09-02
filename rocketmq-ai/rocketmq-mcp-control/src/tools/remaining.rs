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
#[path = "remaining/offset.rs"]
mod offset;
#[path = "remaining/request_mode.rs"]
mod request_mode;

pub use broker::*;
pub use offset::*;
pub use request_mode::*;

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
        return Err(ControlError::invalid_arguments());
    }
    Ok(())
}
