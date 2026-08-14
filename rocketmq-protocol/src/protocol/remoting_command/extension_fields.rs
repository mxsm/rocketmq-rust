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

use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;

use super::BinaryHeaderFields;
use crate::protocol::header_codec::HeaderFieldSource;
use crate::protocol::header_codec::JsonHeaderFields;
use crate::HeaderMap;

#[derive(Clone, Default)]
pub(super) enum ExtensionFields {
    #[default]
    Absent,
    Materialized(HeaderMap),
    RocketMqRaw {
        fields: BinaryHeaderFields,
        materialized: OnceLock<Arc<HeaderMap>>,
    },
    JsonRaw {
        fields: JsonHeaderFields,
        materialized: OnceLock<Arc<HeaderMap>>,
    },
}

impl fmt::Debug for ExtensionFields {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => formatter.write_str("Absent"),
            Self::Materialized(fields) => write!(formatter, "Materialized(count={})", fields.len()),
            Self::RocketMqRaw { fields, materialized } => write!(
                formatter,
                "RocketMqRaw(count={}, materialized={})",
                fields.len(),
                materialized.get().is_some()
            ),
            Self::JsonRaw { fields, materialized } => write!(
                formatter,
                "JsonRaw(count={}, materialized={})",
                fields.len(),
                materialized.get().is_some()
            ),
        }
    }
}

impl ExtensionFields {
    pub(super) fn from_rocketmq_raw(fields: BinaryHeaderFields) -> Self {
        Self::RocketMqRaw {
            fields,
            materialized: OnceLock::new(),
        }
    }

    pub(super) fn from_json_raw(fields: JsonHeaderFields) -> Self {
        Self::JsonRaw {
            fields,
            materialized: OnceLock::new(),
        }
    }

    pub(super) fn replace_map(&mut self, fields: HeaderMap) {
        *self = Self::Materialized(fields);
    }

    pub(super) fn as_map(&self) -> Option<&HeaderMap> {
        match self {
            Self::Absent => None,
            Self::Materialized(fields) => Some(fields),
            Self::RocketMqRaw { fields, materialized } => {
                Some(materialized.get_or_init(|| Arc::new(fields.materialize())))
            }
            Self::JsonRaw { fields, materialized } => Some(materialized.get_or_init(|| Arc::new(fields.materialize()))),
        }
    }

    pub(super) fn as_field_source(&self) -> Option<&dyn HeaderFieldSource> {
        match self {
            Self::RocketMqRaw { fields, .. } => Some(fields),
            Self::JsonRaw { fields, .. } => Some(fields),
            Self::Absent | Self::Materialized(_) => None,
        }
    }

    pub(super) fn as_map_mut(&mut self) -> Option<&mut HeaderMap> {
        match self {
            Self::Absent => None,
            Self::Materialized(fields) => Some(fields),
            Self::RocketMqRaw { fields, materialized } => {
                let fields = materialized.take().map_or_else(
                    || fields.materialize(),
                    |cached| Arc::try_unwrap(cached).unwrap_or_else(|shared| (*shared).clone()),
                );
                *self = Self::Materialized(fields);
                self.as_map_mut()
            }
            Self::JsonRaw { fields, materialized } => {
                let fields = materialized.take().map_or_else(
                    || fields.materialize(),
                    |cached| Arc::try_unwrap(cached).unwrap_or_else(|shared| (*shared).clone()),
                );
                *self = Self::Materialized(fields);
                self.as_map_mut()
            }
        }
    }

    pub(super) fn get_or_insert_map(&mut self) -> &mut HeaderMap {
        match self {
            Self::Materialized(fields) => fields,
            Self::Absent => {
                *self = Self::Materialized(HeaderMap::new());
                self.get_or_insert_map()
            }
            Self::RocketMqRaw { fields, materialized } => {
                let fields = materialized.take().map_or_else(
                    || fields.materialize(),
                    |cached| Arc::try_unwrap(cached).unwrap_or_else(|shared| (*shared).clone()),
                );
                *self = Self::Materialized(fields);
                self.get_or_insert_map()
            }
            Self::JsonRaw { fields, materialized } => {
                let fields = materialized.take().map_or_else(
                    || fields.materialize(),
                    |cached| Arc::try_unwrap(cached).unwrap_or_else(|shared| (*shared).clone()),
                );
                *self = Self::Materialized(fields);
                self.get_or_insert_map()
            }
        }
    }

    pub(super) fn is_absent(&self) -> bool {
        matches!(self, Self::Absent)
    }

    #[cfg(test)]
    pub(super) fn is_rocketmq_raw(&self) -> bool {
        matches!(self, Self::RocketMqRaw { .. })
    }

    #[cfg(test)]
    pub(super) fn is_json_raw(&self) -> bool {
        matches!(self, Self::JsonRaw { .. })
    }

    #[cfg(test)]
    pub(super) fn has_materialized_map(&self) -> bool {
        match self {
            Self::Absent => false,
            Self::Materialized(_) => true,
            Self::RocketMqRaw { materialized, .. } => materialized.get().is_some(),
            Self::JsonRaw { materialized, .. } => materialized.get().is_some(),
        }
    }
}
