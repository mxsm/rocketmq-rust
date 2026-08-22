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

use std::collections::HashMap;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::version::RocketMqVersion;

use super::RemotingCommand;
use super::SerializeType;
use crate::code::request_code::RequestCode;
use crate::protocol::LanguageCode;

#[cfg(test)]
use super::ExtensionFields;
#[cfg(test)]
use crate::protocol::header_codec::BinaryHeaderFields;

impl RemotingCommand {
    pub fn set_code(mut self, code: impl Into<i32>) -> Self {
        self.code = code.into();
        self
    }

    pub fn set_code_ref(&mut self, code: impl Into<i32>) {
        self.code = code.into();
    }

    pub fn set_code_mut(&mut self, code: impl Into<i32>) -> &mut Self {
        self.code = code.into();
        self
    }

    pub fn set_language(mut self, language: LanguageCode) -> Self {
        self.language = language;
        self
    }

    pub fn set_version_ref(&mut self, version: i32) {
        self.version = version;
    }

    pub fn set_version(mut self, version: i32) -> Self {
        self.version = version;
        self
    }

    #[inline]
    pub fn set_opaque(mut self, opaque: i32) -> Self {
        self.opaque = opaque;
        self
    }

    #[inline]
    pub fn set_opaque_mut(&mut self, opaque: i32) {
        self.opaque = opaque;
    }

    #[inline]
    pub fn set_remark_option(mut self, remark: Option<impl Into<CheetahString>>) -> Self {
        self.remark = remark.map(|item| item.into());
        self
    }

    #[inline]
    pub fn set_remark(mut self, remark: impl Into<CheetahString>) -> Self {
        self.remark = Some(remark.into());
        self
    }

    #[inline]
    pub fn set_remark_option_mut(&mut self, remark: Option<impl Into<CheetahString>>) {
        self.remark = remark.map(|item| item.into());
    }

    #[inline]
    pub fn set_remark_mut(&mut self, remark: impl Into<CheetahString>) {
        self.remark = Some(remark.into());
    }

    #[inline]
    pub fn set_ext_fields(mut self, ext_fields: HashMap<CheetahString, CheetahString>) -> Self {
        self.ext_fields.replace_map(ext_fields);
        self.custom_header_to_net = false;
        self
    }

    #[cfg(test)]
    pub(super) fn set_binary_ext_fields(mut self, ext_fields: BinaryHeaderFields) -> Self {
        self.ext_fields = ExtensionFields::from_rocketmq_raw(ext_fields);
        self.custom_header_to_net = false;
        self
    }

    #[inline]
    pub fn set_body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = Some(body.into());
        self
    }

    #[inline]
    pub fn set_body_mut_ref(&mut self, body: impl Into<Bytes>) {
        self.body = Some(body.into());
    }

    #[inline]
    pub fn set_suspended(mut self, suspended: bool) -> Self {
        self.suspended = suspended;
        self
    }

    #[inline]
    pub fn set_suspended_ref(&mut self, suspended: bool) {
        self.suspended = suspended;
    }

    #[inline]
    pub fn set_serialize_type(mut self, serialize_type: SerializeType) -> Self {
        self.serialize_type = serialize_type;
        self
    }

    #[inline]
    pub(crate) fn set_serialize_type_ref(&mut self, serialize_type: SerializeType) {
        self.serialize_type = serialize_type;
    }

    #[inline]
    pub fn get_body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    #[inline]
    pub fn get_body_mut(&mut self) -> Option<&mut Bytes> {
        self.body.as_mut()
    }

    #[inline]
    pub fn code(&self) -> i32 {
        self.code
    }

    #[inline]
    pub fn request_code(&self) -> RequestCode {
        RequestCode::from(self.code)
    }

    #[inline]
    pub fn code_ref(&self) -> &i32 {
        &self.code
    }

    #[inline]
    pub fn language(&self) -> LanguageCode {
        self.language
    }

    #[inline]
    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn rocketmq_version(&self) -> RocketMqVersion {
        RocketMqVersion::from_ordinal(self.version as u32)
    }

    #[inline]
    pub fn opaque(&self) -> i32 {
        self.opaque
    }

    #[inline]
    pub fn remark(&self) -> Option<&CheetahString> {
        self.remark.as_ref()
    }

    #[inline]
    pub fn ext_fields(&self) -> Option<&HashMap<CheetahString, CheetahString>> {
        self.ext_fields.as_map()
    }

    #[inline]
    pub fn body(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    #[inline]
    pub fn take_body(&mut self) -> Option<Bytes> {
        self.body.take()
    }

    #[inline]
    pub fn suspended(&self) -> bool {
        self.suspended
    }

    #[inline]
    pub fn serialize_type(&self) -> SerializeType {
        self.serialize_type
    }

    #[inline]
    pub fn with_opaque(&mut self, opaque: i32) -> &mut Self {
        self.opaque = opaque;
        self
    }

    pub fn add_ext_field(&mut self, key: impl Into<CheetahString>, value: impl Into<CheetahString>) -> &mut Self {
        self.ext_fields.get_or_insert_map().insert(key.into(), value.into());
        self
    }

    #[inline]
    pub fn with_code(&mut self, code: impl Into<i32>) -> &mut Self {
        self.code = code.into();
        self
    }

    #[inline]
    pub fn with_remark(&mut self, remark: impl Into<CheetahString>) -> &mut Self {
        self.remark = Some(remark.into());
        self
    }

    #[inline]
    pub fn get_ext_fields(&self) -> Option<&HashMap<CheetahString, CheetahString>> {
        self.ext_fields.as_map()
    }

    #[inline]
    pub fn add_ext_field_if_not_exist(&mut self, key: impl Into<CheetahString>, value: impl Into<CheetahString>) {
        self.ext_fields
            .get_or_insert_map()
            .entry(key.into())
            .or_insert(value.into());
    }

    /// Ensures the extension fields map is initialized.
    ///
    /// If `ext_fields` is `None`, initializes it with an empty `HashMap`.
    /// This method is idempotent and safe to call multiple times.
    #[inline]
    pub fn ensure_ext_fields_initialized(&mut self) {
        if self.ext_fields.is_absent() {
            let _ = self.ext_fields.get_or_insert_map();
        }
    }
}

impl AsRef<RemotingCommand> for RemotingCommand {
    #[inline]
    fn as_ref(&self) -> &RemotingCommand {
        self
    }
}

impl AsMut<RemotingCommand> for RemotingCommand {
    #[inline]
    fn as_mut(&mut self) -> &mut RemotingCommand {
        self
    }
}
