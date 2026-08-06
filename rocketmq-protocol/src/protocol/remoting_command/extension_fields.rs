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

use std::sync::OnceLock;

use super::BinaryHeaderFields;
use crate::HeaderMap;

#[derive(Default)]
pub(super) enum ExtensionFields {
    #[default]
    Absent,
    Materialized(HeaderMap),
    RocketMqRaw {
        fields: BinaryHeaderFields,
        materialized: OnceLock<HeaderMap>,
    },
}

impl Clone for ExtensionFields {
    fn clone(&self) -> Self {
        match self {
            Self::Absent => Self::Absent,
            Self::Materialized(map) => Self::Materialized(map.clone()),
            Self::RocketMqRaw { fields, materialized } => {
                let cloned_cache = OnceLock::new();
                if let Some(map) = materialized.get() {
                    let _ = cloned_cache.set(map.clone());
                }
                Self::RocketMqRaw {
                    fields: fields.clone(),
                    materialized: cloned_cache,
                }
            }
        }
    }
}

impl ExtensionFields {
    pub(super) fn from_option(fields: Option<HeaderMap>) -> Self {
        fields.map_or(Self::Absent, Self::Materialized)
    }

    pub(super) fn from_rocketmq_raw(fields: BinaryHeaderFields) -> Self {
        Self::RocketMqRaw {
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
            Self::RocketMqRaw { fields, materialized } => Some(materialized.get_or_init(|| fields.materialize())),
        }
    }

    pub(super) fn as_map_mut(&mut self) -> Option<&mut HeaderMap> {
        match self {
            Self::Absent => None,
            Self::Materialized(fields) => Some(fields),
            Self::RocketMqRaw { fields, .. } => {
                let fields = fields.materialize();
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
            Self::RocketMqRaw { fields, .. } => {
                let fields = fields.materialize();
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
    pub(super) fn has_materialized_map(&self) -> bool {
        match self {
            Self::Absent => false,
            Self::Materialized(_) => true,
            Self::RocketMqRaw { materialized, .. } => materialized.get().is_some(),
        }
    }
}
