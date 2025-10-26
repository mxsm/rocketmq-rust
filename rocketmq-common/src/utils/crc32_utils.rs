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
use bytes::Bytes;
use crc32fast::Hasher;

// Calculate the CRC32 checksum for the given byte array
pub fn crc32(buf: &[u8]) -> u32 {
    if buf.is_empty() {
        return 0;
    }
    //crc32fast::hash(buf)
    let mut hasher = Hasher::new();
    hasher.update(buf);
    hasher.finalize() & 0x7FFFFFFF
}

// Calculate the CRC32 checksum for the given byte array
pub fn crc32_bytes(buf: Option<&Bytes>) -> u32 {
    if buf.is_none() {
        return 0;
    }
    let buf = buf.unwrap();
    let mut hasher = Hasher::new();
    hasher.update(buf);
    hasher.finalize() & 0x7FFFFFFF
}

pub fn crc32_bytes_offset(array: &[u8], offset: usize, length: usize) -> u32 {
    if !array.is_empty() && offset < array.len() && offset + length <= array.len() {
        let mut hasher = Hasher::new();
        hasher.update(&array[offset..offset + length]);
        return hasher.finalize() & 0x7FFFFFFF;
    }
    0
}

pub fn crc32_bytebuffer(byte_buffer: &mut Vec<u8>) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(byte_buffer.as_slice());
    hasher.finalize() & 0x7FFFFFFF
}

pub fn crc32_bytebuffers(byte_buffers: &mut Vec<Vec<u8>>) -> u32 {
    let mut hasher = Hasher::new();
    for buffer in byte_buffers {
        hasher.update(buffer.as_slice());
    }
    hasher.finalize() & 0x7FFFFFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_calculates_correct_checksum() {
        let buf = [1, 2, 3, 4, 5];
        assert_eq!(crc32(&buf), 1191942644);
    }

    #[test]
    fn crc32_bytes_offset_calculates_correct_checksum() {
        let buf = [1, 2, 3, 4, 5];
        assert_eq!(crc32_bytes_offset(&buf, 1, 3), 1350933158);
    }

    #[test]
    fn crc32_bytes_offset_returns_zero_for_empty_array() {
        let buf: [u8; 0] = [];
        assert_eq!(crc32_bytes_offset(&buf, 0, 0), 0);
    }

    #[test]
    fn crc32_bytebuffer_calculates_correct_checksum() {
        let mut buf = vec![1, 2, 3, 4, 5];
        assert_eq!(crc32_bytebuffer(&mut buf), 1191942644);
    }

    #[test]
    fn crc32_bytebuffers_calculates_correct_checksum() {
        let mut bufs = vec![vec![1, 2, 3], vec![4, 5]];
        assert_eq!(crc32_bytebuffers(&mut bufs), 1191942644);
    }
}
