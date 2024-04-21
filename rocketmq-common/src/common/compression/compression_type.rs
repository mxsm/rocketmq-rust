use std::io::{self, Read, Write};

use bytes::{
    buf::{Reader, Writer},
    Buf, BufMut, Bytes,
};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::common::sys_flag::message_sys_flag::MessageSysFlag;

#[derive(Debug, PartialEq)]
pub enum CompressionType {
    LZ4,
    Zstd,
    Zlib,
}

impl CompressionType {
    pub fn of(name: &str) -> Self {
        match name.trim().to_uppercase().as_str() {
            "LZ4" => Self::LZ4,
            "ZSTD" => Self::Zstd,
            "ZLIB" => Self::Zlib,
            _ => panic!("Unsupported compress type name: {}", name),
        }
    }

    pub fn find_by_value(value: i32) -> Self {
        match value {
            1 => Self::LZ4,
            2 => Self::Zstd,
            0 | 3 => Self::Zlib,
            _ => panic!("Unknown compress type value: {}", value),
        }
    }

    pub fn get_compression_flag(&self) -> i32 {
        match self {
            Self::LZ4 => MessageSysFlag::COMPRESSION_LZ4_TYPE,
            Self::Zstd => MessageSysFlag::COMPRESSION_ZSTD_TYPE,
            Self::Zlib => MessageSysFlag::COMPRESSION_ZLIB_TYPE,
        }
    }

    pub fn compression(&self, data: &Bytes) -> Bytes {
        match self {
            CompressionType::LZ4 => {
                let compressed = compress_prepend_size(data.chunk());
                Bytes::from(compressed)
            }
            CompressionType::Zstd => {
                let compressed = zstd::encode_all(data.clone().reader(), 5).unwrap();
                Bytes::from(compressed)
            }
            CompressionType::Zlib => {
                let mut zlib_encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                let _ = zlib_encoder.write_all(data.chunk());
                let result = zlib_encoder.finish().unwrap();
                Bytes::from(result)
            }
        }
    }

    pub fn decompression(&self, data: &Bytes) -> Bytes {
        match self {
            CompressionType::LZ4 => {
                let compressed = decompress_size_prepended(data.chunk()).unwrap();
                Bytes::from(compressed)
            }
            CompressionType::Zstd => {
                let compressed = zstd::decode_all(data.clone().reader()).unwrap();
                Bytes::from(compressed)
            }
            CompressionType::Zlib => {
                let mut zlib_encoder = ZlibDecoder::new(data.clone().reader());
                let mut decompressed_data = Vec::new();
                zlib_encoder.read_to_end(&mut decompressed_data).unwrap();
                Bytes::from(decompressed_data)
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
