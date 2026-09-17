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

use crate::common::compression::compressor::Compressor;
use crate::common::compression::lz4_compressor::Lz4Compressor;
use crate::common::compression::zlib_compressor::ZlibCompressor;
use crate::common::compression::zstd_compressor::ZstdCompressor;
use rocketmq_model::common::compression::compression_type::CompressionType;

static LZ4_COMPRESSOR: Lz4Compressor = Lz4Compressor;
static ZLIB_COMPRESSOR: ZlibCompressor = ZlibCompressor;
static ZSTD_COMPRESSOR: ZstdCompressor = ZstdCompressor;

pub struct CompressorFactory;

impl CompressorFactory {
    pub fn get_compressor(compressor_type: CompressionType) -> &'static (dyn Compressor + Send + Sync) {
        match compressor_type {
            CompressionType::LZ4 => &LZ4_COMPRESSOR,
            CompressionType::Zlib => &ZLIB_COMPRESSOR,
            CompressionType::Zstd => &ZSTD_COMPRESSOR,
        }
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::fields;
    use rocketmq_error::ViewValueRef;

    use super::*;

    const ALL_TYPES: [CompressionType; 3] = [CompressionType::LZ4, CompressionType::Zlib, CompressionType::Zstd];

    const COMPRESSORS: [(&str, &dyn Compressor); 3] = [
        ("lz4", &Lz4Compressor),
        ("zlib", &ZlibCompressor),
        ("zstd", &ZstdCompressor),
    ];

    const TYPICAL_PAYLOAD: &[u8] =
        b"Hello RocketMQ Rust! {\"topic\":\"TopicTest\",\"tags\":\"TagA\",\"keys\":\"KEY-1\"} 0123456789";

    /// Deterministic pseudo-random bytes (xorshift64), so no compressor can shrink them.
    fn incompressible_payload(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    fn assert_round_trip(payload: &[u8], level: i32) {
        for (name, compressor) in COMPRESSORS {
            let compressed = compressor
                .compress(payload, level)
                .unwrap_or_else(|e| panic!("{name} compress at level {level} failed: {e}"));
            let decompressed = compressor
                .decompress(&compressed)
                .unwrap_or_else(|e| panic!("{name} decompress at level {level} failed: {e}"));
            assert_eq!(decompressed.as_ref(), payload, "{name} round trip at level {level}");
        }
    }

    #[test]
    fn get_compressor_returns_same_singleton_for_same_type() {
        for compression_type in ALL_TYPES {
            let first = CompressorFactory::get_compressor(compression_type);
            let second = CompressorFactory::get_compressor(compression_type);
            assert!(
                std::ptr::addr_eq(first, second),
                "{compression_type:?} compressor should be a process-wide singleton"
            );
        }
    }

    #[test]
    fn get_compressor_maps_each_type_to_matching_algorithm() {
        for compression_type in ALL_TYPES {
            let compressed = CompressorFactory::get_compressor(compression_type)
                .compress(TYPICAL_PAYLOAD, 5)
                .unwrap_or_else(|e| panic!("{compression_type:?} compress failed: {e}"));
            let decompressed = compression_type.try_decompression(&compressed).unwrap_or_else(|e| {
                panic!("{compression_type:?} output should be decodable by its own algorithm: {e}")
            });
            assert_eq!(
                decompressed.as_ref(),
                TYPICAL_PAYLOAD,
                "{compression_type:?} factory mapping"
            );
        }
    }

    #[test]
    fn round_trips_typical_payload() {
        assert_round_trip(TYPICAL_PAYLOAD, 5);
    }

    #[test]
    fn round_trips_empty_payload() {
        assert_round_trip(&[], 5);
    }

    #[test]
    fn round_trips_repetitive_payload_and_shrinks_it() {
        let payload = vec![b'a'; 64 * 1024];
        assert_round_trip(&payload, 5);
        for (name, compressor) in COMPRESSORS {
            let compressed = compressor
                .compress(&payload, 5)
                .expect("repetitive payload should compress");
            assert!(
                compressed.len() < payload.len(),
                "{name} should shrink a repetitive payload, got {} bytes from {}",
                compressed.len(),
                payload.len()
            );
        }
    }

    #[test]
    fn round_trips_incompressible_payload() {
        assert_round_trip(&incompressible_payload(4096), 5);
    }

    #[test]
    fn round_trips_at_level_bounds() {
        for level in [0, 9] {
            assert_round_trip(TYPICAL_PAYLOAD, level);
            assert_round_trip(&incompressible_payload(1024), level);
        }
    }

    #[test]
    fn decompress_rejects_invalid_input_without_panic() {
        for (name, compressor) in COMPRESSORS {
            let error = compressor
                .decompress(b"definitely not compressed data")
                .expect_err("garbage input should be rejected");
            // The old free-form "decompression failed" detail is dropped by canonical error
            // rendering. The stable contract is the serialization descriptor, the decode of the
            // compression format in the diagnostic context, and the retained compressor source.
            assert_eq!(
                error.descriptor(),
                &rocketmq_error::CORE_SERIALIZATION_FAILED,
                "{name} garbage input should be a serialization failure"
            );
            let diagnostic = error.diagnostic_view().expect("valid diagnostic error context");
            let diagnostic_field = |field_name: &str| {
                diagnostic
                    .fields()
                    .find(|field| field.name() == field_name)
                    .map(|field| field.value())
            };
            assert_eq!(
                diagnostic_field(fields::OPERATION_DIAGNOSTIC.schema().name()),
                Some(ViewValueRef::Text("decode")),
                "{name} failure should be classified as a decode operation"
            );
            assert_eq!(
                diagnostic_field(fields::FORMAT.schema().name()),
                Some(ViewValueRef::Text("compression")),
                "{name} failure should be classified against the compression format"
            );
            assert!(
                std::error::Error::source(&error).is_some(),
                "{name} compressor detail should stay available as a typed source"
            );
        }
    }
}
