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

use bytes::Bytes;
use bytes::BytesMut;

use super::RemotingCommand;
use crate::protocol::command_custom_header::CommandCustomHeader;

impl RemotingCommand {
    /// Legacy ambiguous-success response factory.
    ///
    /// New code should call [`Self::create_success_response_command`] so the
    /// response intent is visible during review. Call
    /// [`Self::create_java_default_error_response_command`] when matching
    /// Java's unset-response behavior instead.
    #[deprecated(
        note = "use create_success_response_command for SUCCESS or create_java_default_error_response_command for Java-compatible unset errors"
    )]
    pub fn create_response_command() -> Self {
        Self::create_success_response_command()
    }

    /// Legacy ambiguous-success typed-header response factory.
    ///
    /// New code should call [`Self::create_success_response_command_with_header`].
    /// Call [`Self::create_java_default_error_response_command_with_header`]
    /// when matching Java's unset-response behavior instead.
    #[deprecated(
        note = "use create_success_response_command_with_header for SUCCESS or create_java_default_error_response_command_with_header for Java-compatible unset errors"
    )]
    pub fn create_response_command_with_header(header: impl CommandCustomHeader + Sync + Send + 'static) -> Self {
        Self::create_success_response_command_with_header(header)
    }

    /// Convert custom header to network format (merge into ext_fields)
    #[inline]
    pub fn make_custom_header_to_net(&mut self) {
        let _ = self.try_make_custom_header_to_net();
    }

    #[inline]
    pub fn materialize_custom_header_to_ext_fields(&mut self) {
        let _ = self.try_make_custom_header_to_net();
    }

    #[deprecated(
        since = "1.0.0",
        note = "use try_read_custom_header_ref; this compatibility alias is fallible despite its historical unchecked name"
    )]
    pub fn read_custom_header_ref_unchecked<T>(&self) -> rocketmq_error::RocketMQResult<&T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_ref::<T>()
    }

    /// Compatibility name for the former shared-reference mutation escape.
    ///
    /// Mutation now requires exclusive access to this command and succeeds only
    /// when the safely shared header is uniquely owned.
    #[deprecated(note = "use read_custom_header_mut; shared-reference mutation is no longer supported")]
    pub fn read_custom_header_mut_from_ref<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_mut::<T>().ok()
    }

    #[deprecated(
        since = "1.0.0",
        note = "use try_read_custom_header_mut; this compatibility alias is fallible despite its historical unchecked name"
    )]
    pub fn read_custom_header_mut_unchecked<T>(&mut self) -> rocketmq_error::RocketMQResult<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_mut::<T>()
    }

    pub fn read_custom_header_ref<T>(&self) -> Option<&T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_ref::<T>().ok()
    }

    pub fn read_custom_header_mut<T>(&mut self) -> Option<&mut T>
    where
        T: CommandCustomHeader + Sync + Send + 'static,
    {
        self.try_read_custom_header_mut::<T>().ok()
    }

    /// Encode header with optimized path selection
    #[inline]
    pub fn header_encode(&mut self) -> Option<Bytes> {
        self.try_header_encode().ok()
    }

    #[inline]
    pub fn fast_header_encode(&mut self, dst: &mut BytesMut) {
        let _ = self.try_fast_header_encode(dst);
    }

    /// Encode header with body length information
    #[inline]
    pub fn encode_header(&mut self) -> Option<Bytes> {
        let body_length = self.body.as_ref().map_or(0, |b| b.len());
        self.try_encode_header_with_body_length(body_length).ok()
    }

    /// Optimized header encoding with pre-calculated capacity
    #[inline]
    pub fn encode_header_with_body_length(&mut self, body_length: usize) -> Option<Bytes> {
        self.try_encode_header_with_body_length(body_length).ok()
    }
}
