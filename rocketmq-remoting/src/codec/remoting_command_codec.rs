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
use std::i32;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    error::{RemotingError, RemotingError::RemotingCommandDecoderError},
    protocol::remoting_command::RemotingCommand,
};

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
    type Item = RemotingCommand;
    type Error = RemotingError;

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
        let _ = cmd_data.get_i32();

        // Read the header length as a big-endian i32.
        let header_length = cmd_data.get_i32() as usize;

        // Assume the header is of i32 type and directly get it from the data.
        let header_data = cmd_data.split_to(header_length);

        let cmd = serde_json::from_slice::<RemotingCommand>(&header_data).map_err(|error| {
            // Handle deserialization error gracefully
            RemotingCommandDecoderError(format!("Deserialization error: {}", error))
        })?;

        let body_length = total_size - 4 - header_length;
        Ok(Some(if body_length > 0 {
            let body_data = cmd_data.split_to(body_length).to_vec();
            cmd.set_body(Some(Bytes::from(body_data)))
        } else {
            cmd
        }))
    }
}

impl Encoder<RemotingCommand> for RemotingCommandCodec {
    type Error = RemotingError;

    fn encode(&mut self, item: RemotingCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut total_length = 4i32;
        let header = item.fast_header_encode();
        let mut header_length = 0i32;
        if let Some(header) = &header {
            header_length = header.len() as i32;
            total_length += header_length;
        }
        let body = item.get_body();
        if let Some(body) = &body {
            total_length += body.len() as i32;
        }

        dst.reserve(total_length as usize);
        // total length: 8 + header length + body length
        dst.put_i32(total_length);
        let serialize_type =
            RemotingCommand::mark_serialize_type(header_length, item.get_serialize_type());
        dst.put_i32(serialize_type);
        if let Some(header_inner) = header {
            dst.put(header_inner);
        }
        if let Some(body_inner) = body {
            dst.put(body_inner);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{header::client_request_header::GetRouteInfoRequestHeader, LanguageCode};

    #[test]
    fn test_encode() {
        let mut dst = BytesMut::new();
        let command = RemotingCommand::create_remoting_command(1)
            .set_code(1)
            .set_language(LanguageCode::JAVA)
            .set_opaque(1)
            .set_flag(1)
            .set_body(Some(Bytes::from("body")))
            .set_command_custom_header(Some(Box::new(GetRouteInfoRequestHeader::new(
                "1111",
                Some(true),
            ))))
            .set_remark(Some("remark".to_string()));
        println!("{}", serde_json::to_string(&command).unwrap());
        let mut encoder = RemotingCommandCodec::new();
        let _ = encoder.encode(command, &mut dst);

        let _expected_length = 8 + "header".len() as i32 + "body".len() as i32;
        let result = encoder.decode(&mut dst);
        println!("{:?}", result.unwrap().unwrap().get_serialize_type());
    }

    #[test]
    fn tsts() {
        let mut bytes1 = bytes::BytesMut::from("122222");
        let _bytes2 = bytes1.split_to(1);
        println!("{}", bytes1.len());
        bytes1.reserve(1);
        let _bytes2 = bytes1.split_to(1);
        println!("{}", bytes1.len());
    }
}
