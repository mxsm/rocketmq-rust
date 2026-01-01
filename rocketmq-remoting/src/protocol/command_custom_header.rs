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

use std::any::Any;
use std::collections::HashMap;

use cheetah_string::CheetahString;

use crate::rocketmq_serializable::RocketMQSerializable;

pub trait CommandCustomHeader: AsAny {
    /// Checks the fields of the implementing type.  
    ///  
    /// Returns a `Result` indicating whether the fields are valid or not.  
    /// If the fields are valid, the `Ok` variant is returned with an empty `()` value.  
    /// If the fields are invalid, an `Err` variant is returned with an associated `Error` value.  
    fn check_fields(&self) -> anyhow::Result<(), anyhow::Error> {
        Ok(())
    }

    /// Converts the implementing type to a map.  
    ///  
    /// Returns an `Option` that contains a `HashMap` of string keys and string values,  
    /// representing the implementing type's fields.  
    /// If the conversion is successful, a non-empty map is returned.  
    /// If the conversion fails, `None` is returned.  
    fn to_map(&self) -> Option<HashMap<CheetahString, CheetahString>>;

    /// Writes the provided `key` to the `out` buffer if the `value` is not empty.
    ///
    /// # Arguments
    ///
    /// * `out` - A mutable reference to a `BytesMut` buffer where the `key` will be written.
    /// * `key` - A string slice that represents the key to be written.
    /// * `value` - A string slice that represents the value associated with the key.
    ///
    /// # Behavior
    ///
    /// If `value` is not empty, the function will write the `key` to the `out` buffer twice,
    /// first with a short length prefix and then with a long length prefix.
    fn write_if_not_null(&self, out: &mut bytes::BytesMut, key: &str, value: &str) {
        if !value.is_empty() {
            RocketMQSerializable::write_str(out, true, key);
            RocketMQSerializable::write_str(out, false, value);
        }
    }

    /// A placeholder function for fast encoding.
    ///
    /// This function currently does nothing and can be overridden by implementing types.
    fn encode_fast(&mut self, _out: &mut bytes::BytesMut) {}

    /// A placeholder function for fast decoding.
    ///
    /// This function currently does nothing and can be overridden by implementing types.
    ///
    /// # Arguments
    ///
    /// * `_fields` - A reference to a `HashMap` that contains the fields to be decoded.
    fn decode_fast(&mut self, _fields: &HashMap<CheetahString, CheetahString>) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }

    /// Indicates whether the implementing type supports fast codec.
    ///
    /// # Returns
    ///
    /// This function returns `false` by default, indicating that the implementing type does not
    /// support fast codec. This can be overridden by implementing types.
    fn support_fast_codec(&self) -> bool {
        false
    }

    /// Retrieves the value associated with the specified field from the provided map.
    ///
    /// # Arguments
    ///
    /// * `map` - A reference to a `HashMap` containing `CheetahString` keys and values.
    /// * `field` - A reference to a `CheetahString` representing the field to retrieve.
    ///
    /// # Returns
    ///
    /// * `Ok(CheetahString)` - If the field is found in the map, returns the associated value.
    /// * `Err(RocketMQError::Serialization)` - If the field is not found in the map, returns an
    ///   error indicating the field is required.
    ///
    /// # Errors
    ///
    /// This function returns a `SerializationError::DecodeFailed` if the specified field is
    /// not found in the map.
    #[inline(always)]
    fn get_and_check_not_none(
        &self,
        map: &HashMap<CheetahString, CheetahString>,
        field: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        match map.get(field) {
            Some(value) => Ok(value.clone()),
            None => Err(rocketmq_error::RocketMQError::Serialization(
                rocketmq_error::SerializationError::DecodeFailed {
                    format: "header",
                    message: format!("The field {field} is required."),
                },
            )),
        }
    }
}

pub trait AsAny: Any {
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn as_any(&self) -> &dyn Any;
}

impl<T: CommandCustomHeader> AsAny for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub trait FromMap {
    type Error: From<rocketmq_error::RocketMQError>;

    type Target;
    /// Converts the implementing type from a map.
    ///
    /// Returns an instance of `Self::Target` that is created from the provided map.
    fn from(map: &HashMap<CheetahString, CheetahString>) -> Result<Self::Target, Self::Error>;
}
