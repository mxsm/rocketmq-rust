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

use bytes::Bytes;

pub trait Compressor {
    /// Compress message by different compressor.
    ///
    /// # Arguments
    ///
    /// * `src` - Bytes ready to compress.
    /// * `level` - Compression level used to balance compression rate and time consumption.
    ///
    /// # Returns
    ///
    /// Compressed byte data or an `std::io::Error`.
    fn compress(&self, src: &[u8], level: i32) -> rocketmq_error::RocketMQResult<Bytes>;

    /// Decompress message by different compressor.
    ///
    /// # Arguments
    ///
    /// * `src` - Bytes ready to decompress.
    ///
    /// # Returns
    ///
    /// Decompressed byte data or an `std::io::Error`.
    fn decompress(&self, src: &[u8]) -> rocketmq_error::RocketMQResult<Bytes>;
}
