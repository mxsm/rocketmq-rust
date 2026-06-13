use std::io::Read;
use std::io::Write;
use std::io::{self};

use bytes::buf::Reader;
use bytes::buf::Writer;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use lz4_flex::compress_prepend_size;
use lz4_flex::decompress_size_prepended;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::SerializationError;

use crate::common::sys_flag::message_sys_flag::MessageSysFlag;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum CompressionType {
    LZ4,
    Zstd,
    Zlib,
}

impl CompressionType {
    pub fn try_of(name: &str) -> RocketMQResult<Self> {
        let name = name.trim();
        if name.eq_ignore_ascii_case("LZ4") {
            Ok(Self::LZ4)
        } else if name.eq_ignore_ascii_case("ZSTD") {
            Ok(Self::Zstd)
        } else if name.eq_ignore_ascii_case("ZLIB") {
            Ok(Self::Zlib)
        } else {
            Err(RocketMQError::ConfigInvalidValue {
                key: "rocketmq.message.compressType",
                value: name.to_string(),
                reason: "supported values are LZ4, ZSTD, and ZLIB".to_string(),
            })
        }
    }

    pub fn of(name: &str) -> RocketMQResult<Self> {
        Self::try_of(name)
    }

    pub fn try_find_by_value(value: i32) -> RocketMQResult<Self> {
        match value {
            1 => Ok(Self::LZ4),
            2 => Ok(Self::Zstd),
            0 | 3 => Ok(Self::Zlib),
            _ => Err(RocketMQError::deserialization_failed(
                "compression",
                format!("unknown compression type value: {value}"),
            )),
        }
    }

    pub fn find_by_value(value: i32) -> RocketMQResult<Self> {
        Self::try_find_by_value(value)
    }

    pub fn get_compression_flag(&self) -> i32 {
        match self {
            Self::LZ4 => MessageSysFlag::COMPRESSION_LZ4_TYPE,
            Self::Zstd => MessageSysFlag::COMPRESSION_ZSTD_TYPE,
            Self::Zlib => MessageSysFlag::COMPRESSION_ZLIB_TYPE,
        }
    }

    pub fn compression(&self, data: &[u8]) -> RocketMQResult<Bytes> {
        self.try_compression(data)
    }

    pub fn try_compression(&self, data: &[u8]) -> RocketMQResult<Bytes> {
        match self {
            CompressionType::LZ4 => {
                let compressed = compress_prepend_size(data);
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zstd => {
                let compressed = zstd::encode_all(data.reader(), 5).map_err(|e| {
                    RocketMQError::Serialization(SerializationError::encode_failed(
                        "compression",
                        format!("zstd compression failed: {e}"),
                    ))
                })?;
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zlib => {
                let mut zlib_encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                zlib_encoder.write_all(data).map_err(|e| {
                    RocketMQError::Serialization(SerializationError::encode_failed(
                        "compression",
                        format!("zlib compression write failed: {e}"),
                    ))
                })?;
                let result = zlib_encoder.finish().map_err(|e| {
                    RocketMQError::Serialization(SerializationError::encode_failed(
                        "compression",
                        format!("zlib compression finish failed: {e}"),
                    ))
                })?;
                Ok(Bytes::from(result))
            }
        }
    }

    pub fn decompression(&self, data: &[u8]) -> RocketMQResult<Bytes> {
        self.try_decompression(data)
    }

    pub fn try_decompression(&self, data: &[u8]) -> RocketMQResult<Bytes> {
        match self {
            CompressionType::LZ4 => {
                let compressed = decompress_size_prepended(data).map_err(|e| {
                    RocketMQError::deserialization_failed("compression", format!("lz4 decompression failed: {e}"))
                })?;
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zstd => {
                let compressed = zstd::decode_all(data.reader()).map_err(|e| {
                    RocketMQError::deserialization_failed("compression", format!("zstd decompression failed: {e}"))
                })?;
                Ok(Bytes::from(compressed))
            }
            CompressionType::Zlib => {
                let mut zlib_encoder = ZlibDecoder::new(data.reader());
                let mut decompressed_data = Vec::new();
                zlib_encoder.read_to_end(&mut decompressed_data).map_err(|e| {
                    RocketMQError::deserialization_failed("compression", format!("zlib decompression failed: {e}"))
                })?;
                Ok(Bytes::from(decompressed_data))
            }
        }
    }
}

struct BytesWriter {
    bytes: Bytes,
}

impl BytesWriter {
    fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

impl Write for BytesWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut bytes_mut = self.bytes.to_vec();
        bytes_mut.extend_from_slice(buf);
        self.bytes = Bytes::from(bytes_mut);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct BytesReader {
    bytes: Bytes,
    position: usize,
}

impl BytesReader {
    fn new(bytes: Bytes) -> Self {
        Self { bytes, position: 0 }
    }
}

impl Read for BytesReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining_bytes = &self.bytes[self.position..];
        let bytes_to_copy = std::cmp::min(buf.len(), remaining_bytes.len());
        buf[..bytes_to_copy].copy_from_slice(&remaining_bytes[..bytes_to_copy]);
        self.position += bytes_to_copy;
        Ok(bytes_to_copy)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn try_find_by_value_rejects_unknown_type() {
        let error = CompressionType::try_find_by_value(7).expect_err("unknown compression type should error");

        assert!(error.to_string().contains("unknown compression type value: 7"));
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

        assert!(error.to_string().contains("rocketmq.message.compressType"));
        assert!(error.to_string().contains("snappy"));
    }

    #[test]
    fn legacy_entrypoints_return_typed_errors_without_panic() {
        let name_error = CompressionType::of("snappy").expect_err("unknown compression name should error");
        assert!(name_error.to_string().contains("rocketmq.message.compressType"));

        let value_error = CompressionType::find_by_value(7).expect_err("unknown compression value should error");
        assert!(value_error.to_string().contains("unknown compression type value: 7"));

        let body_error = CompressionType::Zstd
            .decompression(b"not-zstd")
            .expect_err("invalid compressed body should error");
        assert!(body_error.to_string().contains("zstd decompression failed"));
    }

    #[test]
    fn try_decompression_rejects_invalid_lz4_payload() {
        let error = CompressionType::LZ4
            .try_decompression(b"not-lz4")
            .expect_err("invalid lz4 body should error");

        assert!(error.to_string().contains("lz4 decompression failed"));
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
