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

use crate::common::compression::compression_type::CompressionType;
use crate::common::compression::compressor::Compressor;
use crate::common::compression::lz4_compressor::Lz4Compressor;
use crate::common::compression::zlib_compressor::ZlibCompressor;
use crate::common::compression::zstd_compressor::ZstdCompressor;

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
