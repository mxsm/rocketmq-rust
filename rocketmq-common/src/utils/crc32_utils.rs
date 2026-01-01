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
use crc::Crc;
use crc::CRC_32_ISO_HDLC;

/// CRC32 instance using ISO HDLC standard
const CRC32_ALGO: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

/// Calculate CRC32 checksum for the entire byte array.
///
/// Equivalent to Java's `crc32(byte[] array)`
///
/// # Arguments
///
/// * `array` - Reference to byte slice (returns 0 if empty)
///
/// # Returns
///
/// CRC32 checksum as u32 (masked with 0x7FFFFFFF to ensure positive value), or 0 if empty
///
/// # Examples
///
/// ```
/// use rocketmq_common::utils::crc32_utils::crc32;
///
/// let data = b"Hello, World!";
/// let checksum = crc32(data);
/// assert!(checksum > 0);
///
/// // Empty array returns 0
/// assert_eq!(crc32(&[]), 0);
/// ```
#[inline]
pub fn crc32(array: &[u8]) -> u32 {
    if array.is_empty() {
        return 0;
    }
    crc32_range(array, 0, array.len())
}

/// Calculate CRC32 checksum for a range of bytes in the array.
///
/// Equivalent to Java's `crc32(byte[] array, int offset, int length)`
///
/// # Arguments
///
/// * `array` - The byte array
/// * `offset` - Starting offset in the array
/// * `length` - Number of bytes to process
///
/// # Returns
///
/// CRC32 checksum as u32 (masked with 0x7FFFFFFF)
///
/// # Panics
///
/// Returns 0 if offset or length are invalid (instead of panicking)
///
/// # Examples
///
/// ```
/// use rocketmq_common::utils::crc32_utils::crc32_range;
///
/// let data = b"Hello, World!";
/// let checksum = crc32_range(data, 0, 5); // Only "Hello"
/// assert!(checksum > 0);
/// ```
#[inline]
pub fn crc32_range(array: &[u8], offset: usize, length: usize) -> u32 {
    if array.is_empty() || offset >= array.len() || offset + length > array.len() {
        return 0;
    }

    let mut digest = CRC32_ALGO.digest();
    digest.update(&array[offset..offset + length]);
    digest.finalize() & 0x7FFFFFFF
}

/// Calculate CRC32 checksum for Bytes.
///
/// # Arguments
///
/// * `buf` - Optional reference to Bytes
///
/// # Returns
///
/// CRC32 checksum as u32 (masked with 0x7FFFFFFF), or 0 if None/empty
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use rocketmq_common::utils::crc32_utils::crc32_bytes;
///
/// let data = Bytes::from("test");
/// let checksum = crc32_bytes(Some(&data));
/// assert!(checksum > 0);
/// ```
#[inline]
pub fn crc32_bytes(buf: Option<&Bytes>) -> u32 {
    match buf {
        Some(data) if !data.is_empty() => {
            let mut digest = CRC32_ALGO.digest();
            digest.update(data);
            digest.finalize() & 0x7FFFFFFF
        }
        _ => 0,
    }
}

/// Calculate CRC32 checksum for a range of bytes.
///
/// # Arguments
///
/// * `array` - The byte array
/// * `offset` - Starting offset
/// * `length` - Number of bytes to process
///
/// # Returns
///
/// CRC32 checksum as u32 (masked with 0x7FFFFFFF), or 0 if invalid range
#[inline]
pub fn crc32_bytes_offset(array: &[u8], offset: usize, length: usize) -> u32 {
    crc32_range(array, offset, length)
}

/// Calculate CRC32 checksum for a byte buffer (Vec<u8>).
///
/// Equivalent to Java's `crc32(ByteBuffer byteBuffer)`
///
/// # Arguments
///
/// * `byte_buffer` - Reference to Vec<u8>
///
/// # Returns
///
/// CRC32 checksum as u32 (masked with 0x7FFFFFFF)
///
/// # Examples
///
/// ```
/// use rocketmq_common::utils::crc32_utils::crc32_bytebuffer;
///
/// let buffer = vec![1, 2, 3, 4, 5];
/// let checksum = crc32_bytebuffer(&buffer);
/// assert!(checksum > 0);
/// ```
#[inline]
pub fn crc32_bytebuffer(byte_buffer: &[u8]) -> u32 {
    if byte_buffer.is_empty() {
        return 0;
    }

    let mut digest = CRC32_ALGO.digest();
    digest.update(byte_buffer);
    digest.finalize() & 0x7FFFFFFF
}

/// Calculate CRC32 checksum for multiple byte buffers.
///
/// # Arguments
///
/// * `byte_buffers` - Slice of Vec<u8> buffers
///
/// # Returns
///
/// CRC32 checksum as u32 (masked with 0x7FFFFFFF)
///
/// # Examples
///
/// ```
/// use rocketmq_common::utils::crc32_utils::crc32_bytebuffers;
///
/// let buffers = vec![vec![1, 2, 3], vec![4, 5]];
/// let checksum = crc32_bytebuffers(&buffers);
/// assert!(checksum > 0);
/// ```
#[inline]
pub fn crc32_bytebuffers(byte_buffers: &[Vec<u8>]) -> u32 {
    if byte_buffers.is_empty() {
        return 0;
    }

    let mut digest = CRC32_ALGO.digest();
    for buffer in byte_buffers {
        digest.update(buffer.as_slice());
    }
    digest.finalize() & 0x7FFFFFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== crc32() tests ==========

    #[test]
    fn test_crc32_with_data() {
        let buf = [1, 2, 3, 4, 5];
        let checksum = crc32(&buf);
        assert!(checksum > 0);
        assert!(checksum <= 0x7FFFFFFF); // Ensure it's positive (masked)
    }

    #[test]
    fn test_crc32_with_empty_array() {
        let buf: &[u8] = &[];
        assert_eq!(crc32(buf), 0);
    }

    #[test]
    fn test_crc32_consistency() {
        let data1 = b"Hello, World!";
        let data2 = b"Hello, World!";
        assert_eq!(crc32(data1), crc32(data2));
    }

    #[test]
    fn test_crc32_different_data_different_checksum() {
        let data1 = b"test1";
        let data2 = b"test2";
        assert_ne!(crc32(data1), crc32(data2));
    }

    #[test]
    fn test_crc32_always_positive() {
        // Test with various data to ensure result is always positive
        let test_data = vec![
            b"".as_slice(),
            b"a".as_slice(),
            b"test".as_slice(),
            b"RocketMQ".as_slice(),
            &[0xFF, 0xFF, 0xFF, 0xFF],
            &[0x00, 0x01, 0x02, 0x03],
        ];

        for data in test_data {
            if !data.is_empty() {
                let checksum = crc32(data);
                assert!(checksum <= 0x7FFFFFFF, "Checksum should be positive: {}", checksum);
            }
        }
    }

    // ========== crc32_range() tests ==========

    #[test]
    fn test_crc32_range_full_array() {
        let buf = [1, 2, 3, 4, 5];
        let checksum1 = crc32(&buf);
        let checksum2 = crc32_range(&buf, 0, buf.len());
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_crc32_range_partial() {
        let buf = b"Hello, World!";
        let checksum = crc32_range(buf, 0, 5); // Only "Hello"
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_range_middle() {
        let buf = b"Hello, World!";
        let checksum = crc32_range(buf, 7, 5); // Only "World"
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_range_empty_array() {
        let buf: &[u8] = &[];
        assert_eq!(crc32_range(buf, 0, 0), 0);
    }

    #[test]
    fn test_crc32_range_offset_out_of_bounds() {
        let buf = [1, 2, 3, 4, 5];
        assert_eq!(crc32_range(&buf, 10, 1), 0); // offset >= len
    }

    #[test]
    fn test_crc32_range_length_out_of_bounds() {
        let buf = [1, 2, 3, 4, 5];
        assert_eq!(crc32_range(&buf, 2, 10), 0); // offset + length > len
    }

    #[test]
    fn test_crc32_range_zero_length() {
        let buf = [1, 2, 3, 4, 5];
        assert_eq!(crc32_range(&buf, 0, 0), 0);
    }

    // ========== crc32_bytes() tests ==========

    #[test]
    fn test_crc32_bytes_with_data() {
        let data = Bytes::from("test data");
        let checksum = crc32_bytes(Some(&data));
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_bytes_with_none() {
        assert_eq!(crc32_bytes(None), 0);
    }

    #[test]
    fn test_crc32_bytes_with_empty() {
        let data = Bytes::new();
        assert_eq!(crc32_bytes(Some(&data)), 0);
    }

    #[test]
    fn test_crc32_bytes_consistency() {
        let data1 = Bytes::from("RocketMQ");
        let data2 = Bytes::from("RocketMQ");
        assert_eq!(crc32_bytes(Some(&data1)), crc32_bytes(Some(&data2)));
    }

    #[test]
    fn test_crc32_bytes_matches_slice() {
        let data = b"test data";
        let bytes = Bytes::from(&data[..]);
        assert_eq!(crc32(data), crc32_bytes(Some(&bytes)));
    }

    // ========== crc32_bytes_offset() tests ==========

    #[test]
    fn test_crc32_bytes_offset_valid_range() {
        let buf = [1, 2, 3, 4, 5];
        let checksum = crc32_bytes_offset(&buf, 1, 3); // [2, 3, 4]
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_bytes_offset_full_array() {
        let buf = [1, 2, 3, 4, 5];
        let checksum1 = crc32(&buf);
        let checksum2 = crc32_bytes_offset(&buf, 0, buf.len());
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_crc32_bytes_offset_empty_array() {
        let buf: [u8; 0] = [];
        assert_eq!(crc32_bytes_offset(&buf, 0, 0), 0);
    }

    #[test]
    fn test_crc32_bytes_offset_invalid_range() {
        let buf = [1, 2, 3, 4, 5];
        assert_eq!(crc32_bytes_offset(&buf, 0, 10), 0);
        assert_eq!(crc32_bytes_offset(&buf, 10, 1), 0);
    }

    // ========== crc32_bytebuffer() tests ==========

    #[test]
    fn test_crc32_bytebuffer_with_data() {
        let buffer = vec![1, 2, 3, 4, 5];
        let checksum = crc32_bytebuffer(&buffer);
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_bytebuffer_empty() {
        let buffer: Vec<u8> = vec![];
        assert_eq!(crc32_bytebuffer(&buffer), 0);
    }

    #[test]
    fn test_crc32_bytebuffer_matches_slice() {
        let data = vec![1, 2, 3, 4, 5];
        let checksum1 = crc32(&data);
        let checksum2 = crc32_bytebuffer(&data);
        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_crc32_bytebuffer_large_data() {
        let buffer = vec![0x42; 10000]; // 10KB of 'B'
        let checksum = crc32_bytebuffer(&buffer);
        assert!(checksum > 0);
    }

    // ========== crc32_bytebuffers() tests ==========

    #[test]
    fn test_crc32_bytebuffers_single_buffer() {
        let buffers = vec![vec![1, 2, 3, 4, 5]];
        let checksum = crc32_bytebuffers(&buffers);
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_bytebuffers_multiple_buffers() {
        let buffers = vec![vec![1, 2, 3], vec![4, 5]];
        let checksum = crc32_bytebuffers(&buffers);
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_bytebuffers_matches_concatenated() {
        let buffers = vec![vec![1, 2, 3], vec![4, 5]];
        let concatenated = vec![1, 2, 3, 4, 5];

        let checksum1 = crc32_bytebuffers(&buffers);
        let checksum2 = crc32(&concatenated);

        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_crc32_bytebuffers_empty() {
        let buffers: Vec<Vec<u8>> = vec![];
        assert_eq!(crc32_bytebuffers(&buffers), 0);
    }

    #[test]
    fn test_crc32_bytebuffers_with_empty_buffers() {
        let buffers = vec![vec![], vec![1, 2, 3], vec![]];
        let checksum = crc32_bytebuffers(&buffers);
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_bytebuffers_many_small_buffers() {
        let buffers = vec![vec![1], vec![2], vec![3], vec![4], vec![5]];
        let concatenated = vec![1, 2, 3, 4, 5];

        assert_eq!(crc32_bytebuffers(&buffers), crc32(&concatenated));
    }

    // ========== Integration and edge cases ==========

    #[test]
    fn test_all_functions_return_same_for_same_data() {
        let data = b"RocketMQ";
        let bytes = Bytes::from(&data[..]);
        let buffer = data.to_vec();
        let buffers = vec![data.to_vec()];

        let c1 = crc32(data);
        let c2 = crc32_range(data, 0, data.len());
        let c3 = crc32_bytes(Some(&bytes));
        let c4 = crc32_bytes_offset(data, 0, data.len());
        let c5 = crc32_bytebuffer(&buffer);
        let c6 = crc32_bytebuffers(&buffers);

        assert_eq!(c1, c2);
        assert_eq!(c1, c3);
        assert_eq!(c1, c4);
        assert_eq!(c1, c5);
        assert_eq!(c1, c6);
    }

    #[test]
    fn test_crc32_with_special_characters() {
        let data = b"!@#$%^&*()_+-=[]{}|;':\",./<>?";
        let checksum = crc32(data);
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_with_unicode_bytes() {
        let data = "你好，世界！".as_bytes();
        let checksum = crc32(data);
        assert!(checksum > 0);
    }

    #[test]
    fn test_crc32_with_binary_data() {
        let data = [0x00, 0xFF, 0xAA, 0x55, 0x12, 0x34, 0x56, 0x78];
        let checksum = crc32(&data);
        assert!(checksum > 0);
    }
}
