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

use std::io::Read;
use std::io::Write;

use bytes::Buf;
use bytes::Bytes;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use lz4_flex::block::compress_prepend_size;
use lz4_flex::block::decompress_size_prepended;
use rocketmq_error::Result;

use crate::common::sys_flag::message_sys_flag::MessageSysFlag;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum CompressionType {
    LZ4,
    Zstd,
    Zlib,
}

impl CompressionType {
    pub fn try_of(name: &str) -> Result<Self> {
        let name = name.trim();
        if name.eq_ignore_ascii_case("LZ4") {
            Ok(Self::LZ4)
        } else if name.eq_ignore_ascii_case("ZSTD") {
            Ok(Self::Zstd)
        } else if name.eq_ignore_ascii_case("ZLIB") {
            Ok(Self::Zlib)
        } else {
            Err(crate::error::invalid_configuration("rocketmq.message.compressType"))
        }
    }

    pub fn of(name: &str) -> Result<Self> {
        Self::try_of(name)
    }

    pub fn try_find_by_value(value: i32) -> Result<Self> {
        match value {
            1 => Ok(Self::LZ4),
            2 => Ok(Self::Zstd),
            0 | 3 => Ok(Self::Zlib),
            _ => Err(crate::error::serialization_decode_failed(
                "compression",
                format!("unknown compression type value: {value}"),
            )),
        }
    }

    pub fn find_by_value(value: i32) -> Result<Self> {
        Self::try_find_by_value(value)
    }

    pub fn get_compression_flag(&self) -> i32 {
        match self {
            Self::LZ4 => MessageSysFlag::COMPRESSION_LZ4_TYPE,
            Self::Zstd => MessageSysFlag::COMPRESSION_ZSTD_TYPE,
            Self::Zlib => MessageSysFlag::COMPRESSION_ZLIB_TYPE,
        }
    }

    pub fn compression(&self, data: &[u8]) -> Result<Bytes> {
        self.try_compression(data)
    }

    pub fn try_compression(&self, data: &[u8]) -> Result<Bytes> {
        match self {
            CompressionType::LZ4 => {
                let compressed = compress_prepend_size(data);
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zstd => {
                let compressed = zstd::encode_all(data.reader(), 5)
                    .map_err(|error| crate::error::serialization_source("encode", "compression", error))?;
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zlib => {
                let mut zlib_encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                zlib_encoder
                    .write_all(data)
                    .map_err(|error| crate::error::serialization_source("encode", "compression", error))?;
                let result = zlib_encoder
                    .finish()
                    .map_err(|error| crate::error::serialization_source("encode", "compression", error))?;
                Ok(Bytes::from(result))
            }
        }
    }

    pub fn decompression(&self, data: &[u8]) -> Result<Bytes> {
        self.try_decompression(data)
    }

    pub fn try_decompression(&self, data: &[u8]) -> Result<Bytes> {
        match self {
            CompressionType::LZ4 => {
                let compressed = decompress_size_prepended(data)
                    .map_err(|error| crate::error::serialization_source("decode", "compression", error))?;
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zstd => {
                let compressed = zstd::decode_all(data.reader())
                    .map_err(|error| crate::error::serialization_source("decode", "compression", error))?;
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zlib => {
                let mut zlib_encoder = ZlibDecoder::new(data.reader());
                let mut decompressed_data = Vec::new();
                zlib_encoder
                    .read_to_end(&mut decompressed_data)
                    .map_err(|error| crate::error::serialization_source("decode", "compression", error))?;
                Ok(Bytes::from(decompressed_data))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn try_find_by_value_rejects_unknown_type() {
        let error = CompressionType::try_find_by_value(7).expect_err("unknown compression type should error");

        assert_eq!(error.descriptor(), &rocketmq_error::CORE_SERIALIZATION_FAILED);
    }

    #[test]
    fn try_of_accepts_java_names_case_insensitive() {
        assert_eq!(
            CompressionType::try_of(" lz4 ").expect("LZ4 should parse"),
            CompressionType::LZ4
        );
        assert_eq!(
            CompressionType::try_of("zstd").expect("ZSTD should parse"),
            CompressionType::Zstd
        );
        assert_eq!(
            CompressionType::try_of("ZLIB").expect("ZLIB should parse"),
            CompressionType::Zlib
        );
    }

    #[test]
    fn try_of_rejects_unknown_name_without_panic() {
        let error = CompressionType::try_of("snappy").expect_err("unknown compression name should error");

        assert_eq!(error.descriptor(), &rocketmq_error::CORE_CONFIGURATION_INVALID);
    }

    #[test]
    fn legacy_entrypoints_return_typed_errors_without_panic() {
        let name_error = CompressionType::of("snappy").expect_err("unknown compression name should error");
        assert_eq!(name_error.descriptor(), &rocketmq_error::CORE_CONFIGURATION_INVALID);

        let value_error = CompressionType::find_by_value(7).expect_err("unknown compression value should error");
        assert_eq!(value_error.descriptor(), &rocketmq_error::CORE_SERIALIZATION_FAILED);

        let body_error = CompressionType::Zstd
            .decompression(b"not-zstd")
            .expect_err("invalid compressed body should error");
        assert_eq!(body_error.descriptor(), &rocketmq_error::CORE_SERIALIZATION_FAILED);
        assert!(std::error::Error::source(&body_error).is_some());
    }

    #[test]
    fn try_decompression_rejects_invalid_lz4_payload() {
        let error = CompressionType::LZ4
            .try_decompression(b"not-lz4")
            .expect_err("invalid lz4 body should error");

        assert_eq!(error.descriptor(), &rocketmq_error::CORE_SERIALIZATION_FAILED);
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn try_compression_round_trips_supported_types() {
        let body = Bytes::from_static(b"rocketmq compression round trip");

        for compression_type in [CompressionType::LZ4, CompressionType::Zstd, CompressionType::Zlib] {
            let compressed = compression_type
                .try_compression(&body)
                .expect("supported compressor should compress");
            let decompressed = compression_type
                .try_decompression(&compressed)
                .expect("supported compressor should decompress");

            assert_eq!(decompressed, body);
        }
    }
}
