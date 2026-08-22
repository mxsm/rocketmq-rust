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

use std::sync::Arc;

use super::RemotingCommand;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
use crate::protocol::command_custom_header::HeaderEncodeCapability;
use crate::protocol::header_codec::HeaderCodecError;
use crate::protocol::header_field_merge::merge_header_and_dynamic;

impl RemotingCommand {
    pub fn set_command_custom_header<T>(mut self, command_custom_header: T) -> Self
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.invalidate_materialized_custom_header();
        self.command_custom_header = Some(Arc::new(Box::new(command_custom_header)));
        self.custom_header_to_net = false;
        self
    }

    pub fn set_command_custom_header_boxed(
        mut self,
        command_custom_header: Box<dyn CommandCustomHeader + Send + Sync + 'static>,
    ) -> Self {
        self.invalidate_materialized_custom_header();
        self.command_custom_header = Some(Arc::new(command_custom_header));
        self.custom_header_to_net = false;
        self
    }

    pub fn set_command_custom_header_origin<T>(mut self, command_custom_header: Option<T>) -> Self
    where
        T: std::ops::Deref<Target = Box<dyn CommandCustomHeader + Send + Sync + 'static>>,
    {
        self.invalidate_materialized_custom_header();
        if let Some(header_fields) = command_custom_header.as_ref().and_then(|header| header.to_map()) {
            self.ext_fields.get_or_insert_map().extend(header_fields);
        }
        self.command_custom_header = None;
        self.custom_header_to_net = true;
        self
    }

    pub fn set_command_custom_header_ref<T>(&mut self, command_custom_header: T)
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.invalidate_materialized_custom_header();
        self.command_custom_header = Some(Arc::new(Box::new(command_custom_header)));
        self.custom_header_to_net = false;
    }

    /// Fallibly merges the custom header into dynamic extension fields.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, conversion, alias, or dynamic-field
    /// collision error without mutating this command.
    pub fn try_make_custom_header_to_net(&mut self) -> Result<(), HeaderCodecError> {
        if self.custom_header_to_net {
            return Ok(());
        }

        if let Some(header) = self.command_custom_header_ref() {
            let merged = merge_header_and_dynamic(header, self.ext_fields.as_map())?;
            self.ext_fields.replace_map(merged);
        }
        self.custom_header_to_net = true;
        Ok(())
    }

    fn invalidate_materialized_custom_header(&mut self) {
        if !self.custom_header_to_net {
            return;
        }

        let owned_keys = match (self.command_custom_header_ref(), self.ext_fields.as_map()) {
            (Some(header), Some(fields)) => {
                let mut keys = fields
                    .keys()
                    .filter(|key| header.contains_wire_key(key.as_str()))
                    .cloned()
                    .collect::<Vec<_>>();
                if let Some(legacy_fields) = header.to_map() {
                    keys.extend(legacy_fields.into_keys());
                }
                keys
            }
            _ => Vec::new(),
        };
        if let Some(fields) = self.ext_fields.as_map_mut() {
            for key in owned_keys {
                fields.remove(&key);
            }
        }
        self.custom_header_to_net = false;
    }

    pub fn decode_command_custom_header<T>(&self) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
    {
        if T::SUPPORTS_HEADER_FIELD_SOURCE {
            if let Some(source) = self.ext_fields.as_field_source() {
                return T::from_field_source(source);
            }
        }
        match self.ext_fields.as_map() {
            None => Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "ExtFields is None".to_string(),
                },
            )),
            Some(header) => T::from(header),
        }
    }

    pub fn decode_command_custom_header_fast<T>(&self) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
        T: Default + CommandCustomHeader,
    {
        if T::SUPPORTS_HEADER_FIELD_SOURCE {
            if let Some(source) = self.ext_fields.as_field_source() {
                return T::from_field_source(source);
            }
        }
        match self.ext_fields.as_map() {
            None => Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: "ExtFields is None".to_string(),
                },
            )),
            Some(header) => {
                let mut target = T::default();
                if target.support_fast_codec() {
                    target.decode_fast(header)?;
                    target.check_fields()?;
                    Ok(target)
                } else {
                    T::from(header)
                }
            }
        }
    }

    /// Decodes a required custom request header and classifies any failure at
    /// the request-header boundary.
    ///
    /// `operation` must be a static, low-cardinality description. It is exposed
    /// as structured error context, while the source retains the decoder cause.
    ///
    /// # Errors
    ///
    /// Returns [`rocketmq_error::RocketMQError::RequestHeaderSource`] when the
    /// extension fields are absent or the header cannot be decoded.
    pub fn decode_required_header<T>(&self, operation: &'static str) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
    {
        self.decode_command_custom_header::<T>()
            .map_err(|source| required_header_decode_error(operation, source))
    }

    /// Decodes a required custom request header through the fast codec when the
    /// header supports it and classifies any failure at the request-header
    /// boundary.
    ///
    /// `operation` must be a static, low-cardinality description. It is exposed
    /// as structured error context, while the source retains the decoder cause.
    ///
    /// # Errors
    ///
    /// Returns [`rocketmq_error::RocketMQError::RequestHeaderSource`] when the
    /// extension fields are absent or the header cannot be decoded.
    pub fn decode_required_header_fast<T>(&self, operation: &'static str) -> rocketmq_error::RocketMQResult<T>
    where
        T: FromMap<Target = T, Error = rocketmq_error::RocketMQError>,
        T: Default + CommandCustomHeader,
    {
        self.decode_command_custom_header_fast::<T>()
            .map_err(|source| required_header_decode_error(operation, source))
    }

    pub fn try_read_custom_header_ref<T>(&self) -> rocketmq_error::RocketMQResult<&T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_ref() {
            None => Err(Self::custom_header_missing_error::<T>()),
            Some(value) => value
                .as_ref()
                .as_any()
                .downcast_ref::<T>()
                .ok_or_else(Self::custom_header_type_mismatch_error::<T>),
        }
    }

    pub fn try_read_custom_header_mut<T>(&mut self) -> rocketmq_error::RocketMQResult<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        match self.command_custom_header.as_ref() {
            None => return Err(Self::custom_header_missing_error::<T>()),
            Some(value) if Arc::strong_count(value) != 1 => return Err(Self::custom_header_shared_error()),
            Some(value) if !value.as_ref().as_any().is::<T>() => {
                return Err(Self::custom_header_type_mismatch_error::<T>());
            }
            Some(_) => {}
        }
        self.invalidate_materialized_custom_header();
        Arc::get_mut(
            self.command_custom_header
                .as_mut()
                .ok_or_else(Self::custom_header_missing_error::<T>)?,
        )
        .ok_or_else(Self::custom_header_shared_error)?
        .as_mut()
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(Self::custom_header_type_mismatch_error::<T>)
    }

    pub fn command_custom_header_ref(&self) -> Option<&dyn CommandCustomHeader> {
        match self.command_custom_header.as_ref() {
            None => None,
            Some(value) => Some(value.as_ref().as_ref()),
        }
    }

    pub(crate) fn custom_header_encode_capability(&self) -> HeaderEncodeCapability {
        if self.custom_header_to_net {
            HeaderEncodeCapability::MapOnly
        } else {
            self.command_custom_header_ref()
                .map_or(HeaderEncodeCapability::MapOnly, CommandCustomHeader::encode_capability)
        }
    }

    pub fn command_custom_header_mut(&mut self) -> Option<&mut dyn CommandCustomHeader> {
        if self
            .command_custom_header
            .as_ref()
            .is_none_or(|header| Arc::strong_count(header) != 1)
        {
            return None;
        }
        self.invalidate_materialized_custom_header();
        match self.command_custom_header.as_mut() {
            None => None,
            Some(value) => Arc::get_mut(value).map(|header| header.as_mut() as &mut dyn CommandCustomHeader),
        }
    }

    fn custom_header_missing_error<T>() -> rocketmq_error::RocketMQError
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
            format: "header",
            message: format!(
                "Command custom header is missing; expected {}.",
                std::any::type_name::<T>()
            ),
        })
    }

    fn custom_header_type_mismatch_error<T>() -> rocketmq_error::RocketMQError
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
            format: "header",
            message: format!(
                "Command custom header type mismatch; expected {}.",
                std::any::type_name::<T>()
            ),
        })
    }

    fn custom_header_shared_error() -> rocketmq_error::RocketMQError {
        rocketmq_error::RocketMQError::Serialization(rocketmq_error::SerializationError::DecodeFailed {
            format: "header",
            message: "Command custom header is shared by a cloned command and cannot be mutated safely.".to_string(),
        })
    }
}

#[inline]
fn required_header_decode_error(
    operation: &'static str,
    source: rocketmq_error::RocketMQError,
) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::request_header_source(operation, source)
}
