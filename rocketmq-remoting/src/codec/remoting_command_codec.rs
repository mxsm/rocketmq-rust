/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use crate::error::RemotingError;
use crate::error::RemotingError::RemotingCommandDecoderError;
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
#[derive(Debug, Clone)]
pub struct RemotingCommandCodec;

impl Default for RemotingCommandCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RemotingCommandCodec {
    pub fn new() -> Self {
        Self {}
    }
}

impl Decoder for RemotingCommandCodec {
    type Error = RemotingError;
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
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let read_to = src.len();
        if read_to < 4 {
            // Wait for more data when there are less than 4 bytes.
            return Ok(None);
        }
        //Read the total size as a big-endian i32 from the first 4 bytes.
        let total_size = i32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if read_to < total_size + 4 {
            // Wait for more data when the available data is less than the total size.
            return Ok(None);
        }
        // Split the BytesMut to get the command data including the total size.
        let mut cmd_data = src.split_to(total_size + 4);
        // Discard the first i32 (total size).
        cmd_data.advance(4);
        if cmd_data.remaining() < 4 {
            return Ok(None);
        }
        // Read the header length as a big-endian i32.
        let header_length = cmd_data.get_i32() as usize;
        if header_length > total_size - 4 {
            return Err(RemotingCommandDecoderError(format!(
                "Header length {} is greater than total size {}",
                header_length, total_size
            )));
        }

        // Assume the header is of i32 type and directly get it from the data.
        let header_data = cmd_data.split_to(header_length);

        let mut cmd = serde_json::from_slice::<RemotingCommand>(&header_data).map_err(|error| {
            // Handle deserialization error gracefully
            RemotingCommandDecoderError(format!("Deserialization error: {}", error))
        })?;
        if total_size - 4 > header_length {
            cmd.set_body_mut_ref(Some(
                cmd_data.split_to(total_size - 4 - header_length).freeze(),
            ));
        }

        Ok(Some(cmd))
    }
}

impl Encoder<RemotingCommand> for RemotingCommandCodec {
    type Error = RemotingError;

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
        let header = item.fast_header_encode();
        let header_length = header.as_ref().map_or(0, |h| h.len()) as i32;
        let body_length = item.get_body().map_or(0, |b| b.len()) as i32;
        let total_length = 4 + header_length + body_length;

        dst.reserve((total_length + 4) as usize);
        dst.put_i32(total_length);
        let serialize_type =
            RemotingCommand::mark_serialize_type(header_length, item.get_serialize_type());
        dst.put_i32(serialize_type);

        if let Some(header_inner) = header {
            dst.put(header_inner);
        }
        if let Some(body_inner) = item.get_body() {
            dst.put(body_inner);
        }
        Ok(())
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
    async fn decode_handles_sufficient_data() {
        let mut decoder = RemotingCommandCodec::new();
        let mut src = BytesMut::from(&[0, 0, 0, 1, 0, 0, 0, 0][..]);
        assert!(matches!(decoder.decode(&mut src), Ok(None)));
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
            .set_remark(Some("remark".to_string()));
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
            .set_body(Some(Bytes::from("body")))
            .set_command_custom_header(GetRouteInfoRequestHeader::new("1111", Some(true)))
            .set_remark(Some("remark".to_string()));
        assert!(encoder.encode(command, &mut dst).is_ok());
    }
}
