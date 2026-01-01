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

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use rocketmq_error::RocketmqError;
use tokio_util::codec::BytesCodec;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use crate::protocol::remoting_command::RemotingCommand;

/// Encodes a `RemotingCommand` into a `BytesMut` buffer.
///
/// This method takes a `RemotingCommand` and a mutable reference to a `BytesMut` buffer as
/// parameters. It first encodes the header of the `RemotingCommand` and calculates the lengths of
/// the header and body. It then reserves the necessary space in the `BytesMut` buffer and writes
/// the total length, serialize type, header, and body to the buffer.
///
/// # Arguments
///
/// * `item` - A `RemotingCommand` that is to be encoded.
/// * `dst` - A mutable reference to a `BytesMut` buffer where the encoded command will be written.
///
/// # Returns
///
/// * `Result<(), Self::Error>` - Returns `Ok(())` if the encoding is successful, otherwise returns
///   an `Err` with a `RemotingError`.
///
/// # Errors
///
/// This function will return an error if the encoding process fails.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RemotingCommandCodec(());

impl Default for RemotingCommandCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RemotingCommandCodec {
    pub fn new() -> Self {
        RemotingCommandCodec(())
    }
}

impl Decoder for RemotingCommandCodec {
    type Error = rocketmq_error::RocketMQError;
    type Item = RemotingCommand;

    /// Decodes a `RemotingCommand` from a `BytesMut` buffer.
    ///
    /// This method takes a mutable reference to a `BytesMut` buffer as a parameter.
    /// It first checks if there are at least 4 bytes in the buffer, if not, it returns `Ok(None)`.
    /// Then it reads the total size of the incoming data as a big-endian i32 from the first 4
    /// bytes. If the available data is less than the total size, it returns `Ok(None)`.
    /// It then splits the `BytesMut` buffer to get the command data including the total size and
    /// discards the first i32 (total size). It reads the header length as a big-endian i32 and
    /// checks if the header length is greater than the total size minus 4. If it is, it returns
    /// an error. It then splits the buffer again to get the header data and deserializes it
    /// into a `RemotingCommand`. If the total size minus 4 is greater than the header length,
    /// it sets the body of the `RemotingCommand`.
    ///
    /// # Arguments
    ///
    /// * `src` - A mutable reference to a `BytesMut` buffer from which the `RemotingCommand` will
    ///   be decoded.
    ///
    /// # Returns
    ///
    /// * `Result<Option<Self::Item>, Self::Error>` - Returns `Ok(Some(cmd))` if the decoding is
    ///   successful, otherwise returns an `Err` with a `RemotingError`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the decoding process fails.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, rocketmq_error::RocketMQError> {
        RemotingCommand::decode(src)
    }
}

impl Encoder<RemotingCommand> for RemotingCommandCodec {
    type Error = rocketmq_error::RocketMQError;

    /// Encodes a `RemotingCommand` into a `BytesMut` buffer.
    ///
    /// This method takes a `RemotingCommand` and a mutable reference to a `BytesMut` buffer as
    /// parameters. It first encodes the header of the `RemotingCommand` and calculates the
    /// lengths of the header and body. It then reserves the necessary space in the `BytesMut`
    /// buffer and writes the total length, serialize type, header, and body to the buffer.
    ///
    /// # Arguments
    ///
    /// * `item` - A `RemotingCommand` that is to be encoded.
    /// * `dst` - A mutable reference to a `BytesMut` buffer where the encoded command will be
    ///   written.
    ///
    /// # Returns
    ///
    /// * `Result<(), Self::Error>` - Returns `Ok(())` if the encoding is successful, otherwise
    ///   returns an `Err` with a `RemotingError`.
    ///
    /// # Errors
    ///
    /// This function will return an error if the encoding process fails.
    fn encode(&mut self, item: RemotingCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut item = item;
        item.fast_header_encode(dst);
        if let Some(body_inner) = item.take_body() {
            dst.put(body_inner);
        }
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct CompositeCodec {
    bytes_codec: BytesCodec,
    remoting_command_codec: RemotingCommandCodec,
}

impl CompositeCodec {
    pub fn new() -> Self {
        Self {
            bytes_codec: BytesCodec::new(),
            remoting_command_codec: RemotingCommandCodec::new(),
        }
    }
}

impl Decoder for CompositeCodec {
    type Error = rocketmq_error::RocketMQError;
    type Item = RemotingCommand;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, rocketmq_error::RocketMQError> {
        self.remoting_command_codec.decode(src)
    }
}

impl Encoder<Bytes> for CompositeCodec {
    type Error = rocketmq_error::RocketMQError;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.bytes_codec.encode(item, dst).map_err(|error| {
            RocketmqError::RemotingCommandEncoderError(format!("Error encoding bytes: {error}")).into()
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use crate::protocol::LanguageCode;

    #[tokio::test]
    async fn decode_handles_insufficient_data() {
        let mut decoder = RemotingCommandCodec::new();
        let mut src = BytesMut::from(&[0, 0, 0, 1][..]);
        assert!(matches!(decoder.decode(&mut src), Ok(None)));
    }

    #[tokio::test]
    async fn decode_handles_invalid_total_size() {
        let mut decoder = RemotingCommandCodec::new();
        // total_size = 1, which is less than minimum required (4 bytes for serialize_type)
        let mut src = BytesMut::from(&[0, 0, 0, 1, 0, 0, 0, 0][..]);
        assert!(decoder.decode(&mut src).is_err());
    }

    #[tokio::test]
    async fn encode_handles_empty_body() {
        let mut encoder = RemotingCommandCodec::new();
        let mut dst = BytesMut::new();
        let command = RemotingCommand::create_remoting_command(1)
            .set_code(1)
            .set_language(LanguageCode::JAVA)
            .set_opaque(1)
            .set_flag(1)
            .set_command_custom_header(GetRouteInfoRequestHeader::new("1111", Some(true)))
            .set_remark_option(Some("remark".to_string()));
        assert!(encoder.encode(command, &mut dst).is_ok());
    }

    #[tokio::test]
    async fn encode_handles_non_empty_body() {
        let mut encoder = RemotingCommandCodec::new();
        let mut dst = BytesMut::new();
        let command = RemotingCommand::create_remoting_command(1)
            .set_code(1)
            .set_language(LanguageCode::JAVA)
            .set_opaque(1)
            .set_flag(1)
            .set_body(Bytes::from("body"))
            .set_command_custom_header(GetRouteInfoRequestHeader::new("1111", Some(true)))
            .set_remark_option(Some("remark".to_string()));
        assert!(encoder.encode(command, &mut dst).is_ok());
    }
}
