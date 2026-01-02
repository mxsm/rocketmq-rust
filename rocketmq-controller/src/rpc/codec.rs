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

use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use serde::Deserialize;
use serde::Serialize;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;
use tracing::debug;
use tracing::trace;

use crate::error::ControllerError;
use crate::error::Result;
use crate::processor::RequestType;

/// RPC request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    /// Request ID for correlation
    pub request_id: u64,

    /// Request type
    pub request_type: RequestType,

    /// Request payload (JSON-encoded)
    pub payload: Vec<u8>,
}

/// RPC response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    /// Request ID for correlation
    pub request_id: u64,

    /// Success flag
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Response payload (JSON-encoded)
    pub payload: Vec<u8>,
}

/// RPC message codec
///
/// Protocol format:
/// ```text
/// +--------+--------+--------+--------+
/// | Length (4 bytes, big-endian)      |
/// +--------+--------+--------+--------+
/// | JSON-encoded message              |
/// | ...                               |
/// +-----------------------------------+
/// ```
pub struct RpcCodec;

impl RpcCodec {
    /// Maximum frame size (16MB)
    const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

    /// Create a new RPC codec
    pub fn new() -> Self {
        Self
    }
}

impl Default for RpcCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for RpcCodec {
    type Item = RpcRequest;
    type Error = ControllerError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        // Check if we have enough bytes for the length prefix
        if src.len() < 4 {
            trace!("Not enough bytes for length prefix: {}", src.len());
            return Ok(None);
        }

        // Read the length prefix
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        trace!("RPC request length: {}", length);

        // Validate length
        if length > Self::MAX_FRAME_SIZE {
            return Err(ControllerError::InvalidRequest(format!(
                "Frame size {} exceeds maximum {}",
                length,
                Self::MAX_FRAME_SIZE
            )));
        }

        // Check if we have the complete frame
        if src.len() < 4 + length {
            trace!("Incomplete frame: have {}, need {}", src.len(), 4 + length);
            // Reserve space for the rest of the frame
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Skip the length prefix
        src.advance(4);

        // Read the frame data
        let data = src.split_to(length);

        // Deserialize the request
        let request: RpcRequest =
            serde_json::from_slice(&data).map_err(|e| ControllerError::InvalidRequest(e.to_string()))?;

        debug!(
            "Decoded RPC request: id={}, type={:?}",
            request.request_id, request.request_type
        );

        Ok(Some(request))
    }
}

impl Encoder<RpcResponse> for RpcCodec {
    type Error = ControllerError;

    fn encode(&mut self, item: RpcResponse, dst: &mut BytesMut) -> Result<()> {
        debug!(
            "Encoding RPC response: id={}, success={}",
            item.request_id, item.success
        );

        // Serialize the response
        let data = serde_json::to_vec(&item).map_err(|e| ControllerError::SerializationError(e.to_string()))?;

        // Check size
        if data.len() > Self::MAX_FRAME_SIZE {
            return Err(ControllerError::SerializationError(format!(
                "Response size {} exceeds maximum {}",
                data.len(),
                Self::MAX_FRAME_SIZE
            )));
        }

        // Write length prefix
        let length = data.len() as u32;
        dst.reserve(4 + data.len());
        dst.put_u32(length);

        // Write data
        dst.put_slice(&data);

        trace!("Encoded RPC response: {} bytes", 4 + data.len());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_request_serialization() {
        let request = RpcRequest {
            request_id: 123,
            request_type: RequestType::RegisterBroker,
            payload: b"test payload".to_vec(),
        };

        let serialized = serde_json::to_vec(&request).unwrap();
        let deserialized: RpcRequest = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.request_id, request.request_id);
        assert_eq!(deserialized.request_type, request.request_type);
        assert_eq!(deserialized.payload, request.payload);
    }

    #[test]
    fn test_rpc_response_serialization() {
        let response = RpcResponse {
            request_id: 456,
            success: true,
            error: None,
            payload: b"response payload".to_vec(),
        };

        let serialized = serde_json::to_vec(&response).unwrap();
        let deserialized: RpcResponse = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.request_id, response.request_id);
        assert_eq!(deserialized.success, response.success);
        assert_eq!(deserialized.error, response.error);
        assert_eq!(deserialized.payload, response.payload);
    }

    #[test]
    fn test_codec_decode_incomplete() {
        let mut codec = RpcCodec::new();
        let mut buf = BytesMut::new();

        // Write only 2 bytes of length prefix
        buf.put_u16(0x00);

        let result = codec.decode(&mut buf);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_codec_encode_decode() {
        let mut codec = RpcCodec::new();

        // Create a request
        let request = RpcRequest {
            request_id: 789,
            request_type: RequestType::BrokerHeartbeat,
            payload: b"heartbeat data".to_vec(),
        };

        // Serialize manually to get the data
        let request_data = serde_json::to_vec(&request).unwrap();

        // Create a buffer with length prefix + data
        let mut encode_buf = BytesMut::new();
        encode_buf.put_u32(request_data.len() as u32);
        encode_buf.put_slice(&request_data);

        // Decode
        let decoded = codec.decode(&mut encode_buf).unwrap();
        assert!(decoded.is_some());

        let decoded_request = decoded.unwrap();
        assert_eq!(decoded_request.request_id, request.request_id);
        assert_eq!(decoded_request.request_type, request.request_type);
        assert_eq!(decoded_request.payload, request.payload);
    }

    #[test]
    fn test_codec_encode_response() {
        let mut codec = RpcCodec::new();
        let mut buf = BytesMut::new();

        let response = RpcResponse {
            request_id: 999,
            success: true,
            error: None,
            payload: b"success response".to_vec(),
        };

        let result = codec.encode(response.clone(), &mut buf);
        assert!(result.is_ok());

        // Check that length prefix is present
        assert!(buf.len() >= 4);

        // Read length
        let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(buf.len(), 4 + length);
    }
}
