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

use std::fmt;

use rocketmq_error::FilterError;
use rocketmq_error::RocketMQResult;

/// Wrapper of bytes arrays, in order to operate single bit easily.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BitsArray {
    bytes: Vec<u8>,
    bit_length: usize,
}

impl BitsArray {
    /// Create a new BitsArray with the specified bit length
    #[must_use]
    pub fn create(bit_length: usize) -> Self {
        let byte_length = bit_length.div_ceil(8);
        BitsArray {
            bytes: vec![0u8; byte_length],
            bit_length,
        }
    }

    /// Create a BitsArray from bytes with specified bit length
    pub fn from_bytes_with_length(bytes: &[u8], bit_length: usize) -> RocketMQResult<Self> {
        if bytes.is_empty() {
            return Err(FilterError::empty_bytes().into());
        }
        if bit_length < 1 {
            return Err(FilterError::invalid_bit_length().into());
        }
        if bit_length < bytes.len() * 8 {
            return Err(FilterError::bit_length_too_small().into());
        }
        Ok(BitsArray {
            bytes: bytes.to_vec(),
            bit_length,
        })
    }

    /// Create a BitsArray from bytes, using bytes.len() * 8 as bit length
    pub fn from_bytes(bytes: &[u8]) -> RocketMQResult<Self> {
        if bytes.is_empty() {
            return Err(FilterError::empty_bytes().into());
        }
        let bit_length = bytes.len() * 8;
        Ok(BitsArray {
            bytes: bytes.to_vec(),
            bit_length,
        })
    }

    #[inline]
    pub fn bit_length(&self) -> usize {
        self.bit_length
    }

    #[inline]
    pub fn byte_length(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// XOR operation with another BitsArray
    pub fn xor(&mut self, other: &BitsArray) -> RocketMQResult<()> {
        self.check_initialized()?;
        other.check_initialized()?;
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] ^= other.bytes[i];
        }
        Ok(())
    }

    /// XOR operation on a single bit
    pub fn xor_bit(&mut self, bit_pos: usize, set: bool) -> RocketMQResult<()> {
        self.check_bit_position(bit_pos)?;
        let value = self.get_bit(bit_pos)?;
        self.set_bit(bit_pos, value ^ set)
    }

    /// OR operation with another BitsArray
    pub fn or(&mut self, other: &BitsArray) -> RocketMQResult<()> {
        self.check_initialized()?;
        other.check_initialized()?;
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] |= other.bytes[i];
        }
        Ok(())
    }

    /// OR operation on a single bit
    pub fn or_bit(&mut self, bit_pos: usize, set: bool) -> RocketMQResult<()> {
        self.check_bit_position(bit_pos)?;
        if set {
            self.set_bit(bit_pos, true)?;
        }
        Ok(())
    }

    /// AND operation with another BitsArray
    pub fn and(&mut self, other: &BitsArray) -> RocketMQResult<()> {
        self.check_initialized()?;
        other.check_initialized()?;
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] &= other.bytes[i];
        }
        Ok(())
    }

    /// AND operation on a single bit
    pub fn and_bit(&mut self, bit_pos: usize, set: bool) -> RocketMQResult<()> {
        self.check_bit_position(bit_pos)?;
        if !set {
            self.set_bit(bit_pos, false)?;
        }
        Ok(())
    }

    /// NOT operation on a single bit
    pub fn not(&mut self, bit_pos: usize) -> RocketMQResult<()> {
        self.check_bit_position(bit_pos)?;
        let value = self.get_bit(bit_pos)?;
        self.set_bit(bit_pos, !value)
    }

    /// Set a bit at the specified position
    pub fn set_bit(&mut self, bit_pos: usize, set: bool) -> RocketMQResult<()> {
        self.check_bit_position(bit_pos)?;
        let sub = self.subscript(bit_pos);
        let pos = self.position(bit_pos);
        if set {
            self.bytes[sub] |= pos;
        } else {
            self.bytes[sub] &= !pos;
        }
        Ok(())
    }

    /// Set a byte at the specified position
    pub fn set_byte(&mut self, byte_pos: usize, set: u8) -> RocketMQResult<()> {
        self.check_byte_position(byte_pos)?;
        self.bytes[byte_pos] = set;
        Ok(())
    }

    /// Get a bit at the specified position
    pub fn get_bit(&self, bit_pos: usize) -> RocketMQResult<bool> {
        self.check_bit_position(bit_pos)?;
        Ok((self.bytes[self.subscript(bit_pos)] & self.position(bit_pos)) != 0)
    }

    /// Get a byte at the specified position
    pub fn get_byte(&self, byte_pos: usize) -> RocketMQResult<u8> {
        self.check_byte_position(byte_pos)?;
        Ok(self.bytes[byte_pos])
    }

    #[inline]
    fn subscript(&self, bit_pos: usize) -> usize {
        bit_pos / 8
    }

    #[inline]
    fn position(&self, bit_pos: usize) -> u8 {
        1 << (bit_pos % 8)
    }

    fn check_byte_position(&self, byte_pos: usize) -> RocketMQResult<()> {
        self.check_initialized()?;
        if byte_pos >= self.byte_length() {
            return Err(FilterError::byte_position_out_of_bounds(byte_pos, self.bytes.len()).into());
        }
        Ok(())
    }

    fn check_bit_position(&self, bit_pos: usize) -> RocketMQResult<()> {
        self.check_initialized()?;
        if bit_pos >= self.bit_length() {
            return Err(FilterError::bit_position_out_of_bounds(bit_pos, self.bit_length).into());
        }
        Ok(())
    }

    fn check_initialized(&self) -> RocketMQResult<()> {
        if self.bytes.is_empty() {
            return Err(FilterError::uninitialized().into());
        }
        Ok(())
    }
}

impl fmt::Display for BitsArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.bytes.is_empty() {
            return write!(f, "null");
        }
        let mut s = String::with_capacity(self.bytes.len() * 8);
        for i in (0..self.bytes.len()).rev() {
            let mut j = 7;
            if i == self.bytes.len() - 1 && self.bit_length % 8 > 0 {
                j = self.bit_length % 8 - 1;
            }
            for k in (0..=j).rev() {
                let mask = 1 << k;
                if (self.bytes[i] & mask) == mask {
                    s.push('1');
                } else {
                    s.push('0');
                }
            }
            if i % 8 == 0 {
                s.push('\n');
            }
        }
        write!(f, "{s}")
    }
}

impl AsRef<[u8]> for BitsArray {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::bits_array::BitsArray;

    #[test]
    fn validate_create() {
        let i: usize = 50;
        let array = BitsArray::create(i);
        assert_eq!(array.bytes.len(), i.div_ceil(8));
        assert_eq!(array.bit_length(), i);
    }

    #[test]
    fn validate_from_bytes_with_length() {
        let empty_bytes: &[u8] = &[];
        assert!(BitsArray::from_bytes_with_length(empty_bytes, 1).is_err());
        let non_empty_bytes: &[u8] = &[255];
        assert!(BitsArray::from_bytes_with_length(non_empty_bytes, 0).is_err());
        assert!(BitsArray::from_bytes_with_length(non_empty_bytes, 7).is_err());
        assert!(BitsArray::from_bytes_with_length(non_empty_bytes, 8).is_ok());
    }

    #[test]
    fn validate_from_bytes() {
        let empty_bytes: &[u8] = &[];
        assert!(BitsArray::from_bytes(empty_bytes).is_err());
        let non_empty_bytes: &[u8] = &[255, 255];
        assert!(BitsArray::from_bytes(non_empty_bytes).is_ok());
    }

    #[test]
    fn validate_xor() {
        let min_byte: &[u8] = &[0];
        let max_byte: &[u8] = &[255];
        let min_array = BitsArray::from_bytes(min_byte).unwrap();
        let mut max_array = BitsArray::from_bytes(max_byte).unwrap();
        // 255 ^ 0 = 255
        assert!(max_array.xor(&min_array).is_ok());
        assert_eq!(max_array.bytes(), max_byte);
        // 255 ^ 255 = 0
        let max_array_clone = BitsArray::from_bytes(max_byte).unwrap();
        max_array.xor(&max_array_clone).unwrap();
        assert_eq!(max_array.bytes(), min_byte);
    }

    #[test]
    fn validate_xor_bit() {
        let bytes: &[u8] = &[0b01];
        let mut byte_array = BitsArray::from_bytes(bytes).unwrap();
        assert!(byte_array.xor_bit(0, true).is_ok());
        assert!(byte_array.xor_bit(1, true).is_ok());
        assert_eq!(byte_array.bytes(), &[0b10]);
    }

    #[test]
    fn validate_or() {
        let byte_seq1: &[u8] = &[0b0011];
        let byte_seq2: &[u8] = &[0b0101];
        let mut byte_array1 = BitsArray::from_bytes(byte_seq1).unwrap();
        let byte_array2 = BitsArray::from_bytes(byte_seq2).unwrap();
        assert!(byte_array1.or(&byte_array2).is_ok());
        assert_eq!(byte_array1.bytes(), &[0b0111]);
    }

    #[test]
    fn validate_or_bit() {
        let byte_seq: &[u8] = &[0b00];
        let mut byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.or_bit(0, true).is_ok());
        assert!(byte_array.or_bit(1, false).is_ok());
        assert_eq!(byte_array.bytes(), &[0b01]);
    }

    #[test]
    fn validate_and() {
        let byte_seq1: &[u8] = &[0b0011];
        let byte_seq2: &[u8] = &[0b0101];
        let byte_array1 = BitsArray::from_bytes(byte_seq1).unwrap();
        let mut byte_array2 = BitsArray::from_bytes(byte_seq2).unwrap();
        assert!(byte_array2.and(&byte_array1).is_ok());
        assert_eq!(byte_array2.bytes(), &[0b0001]);
    }

    #[test]
    fn validate_and_bit() {
        let byte_seq: &[u8] = &[0b0011];
        let mut byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.and_bit(0, true).is_ok());
        assert!(byte_array.and_bit(1, false).is_ok());
        assert!(byte_array.and_bit(2, true).is_ok());
        assert!(byte_array.and_bit(3, false).is_ok());
        assert_eq!(byte_array.bytes(), &[0b0001]);
    }

    #[test]
    fn validate_not() {
        let byte_seq: &[u8] = &[0b01];
        let mut byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.not(0).is_ok());
        assert!(byte_array.not(1).is_ok());
        assert_eq!(byte_array.bytes(), &[0b10]);
    }

    #[test]
    fn validate_set_bit() {
        let byte_seq: &[u8] = &[0b1];
        let mut byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.set_bit(0, false).is_ok());
        assert_eq!(byte_array.bytes(), &[0b0]);
        assert!(byte_array.set_bit(0, true).is_ok());
        assert_eq!(byte_array.bytes(), &[0b1]);
    }

    #[test]
    fn validate_set_byte() {
        let byte_seq: &[u8] = &[0, 255];
        let mut byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.set_byte(0, 255).is_ok());
        assert_eq!(byte_array.bytes(), &[255, 255]);
    }

    #[test]
    fn validate_get_bit() {
        let byte_seq: &[u8] = &[0b00000010];
        let byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.get_bit(1).unwrap());
    }

    #[test]
    fn validate_get_byte() {
        let byte_seq: &[u8] = &[0, 127, 255];
        let byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert_eq!(byte_array.get_byte(1).unwrap(), 127);
    }

    #[test]
    fn validate_check_byte_position() {
        let byte_seq: &[u8] = &[0, 127, 255];
        let byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.check_byte_position(1).is_ok());
        assert!(byte_array.check_byte_position(3).is_err());
    }

    #[test]
    fn validate_check_bit_position() {
        let byte_seq: &[u8] = &[0, 127];
        let byte_array = BitsArray::from_bytes(byte_seq).unwrap();
        assert!(byte_array.check_bit_position(15).is_ok());
        assert!(byte_array.check_bit_position(16).is_err());
    }

    #[test]
    fn validate_check_initialized() {
        let uninitialized = BitsArray::create(0);
        assert!(uninitialized.check_initialized().is_err());
        let initialized = BitsArray::from_bytes(&[255]).unwrap();
        assert!(initialized.check_initialized().is_ok());
    }

    #[test]
    fn validate_display() {
        let empty_array = BitsArray::create(0);
        assert_eq!(format!("{}", empty_array), "null");
        let bit_array = BitsArray::from_bytes(&[0b00011101, 255, 255, 255, 255, 255, 255, 255, 0]).unwrap();
        assert_eq!(
            format!("{}", bit_array),
            "00000000\n1111111111111111111111111111111111111111111111111111111100011101\n"
        );
        let bits = BitsArray {
            bytes: vec![0b0001_0110],
            bit_length: 5,
        };
        assert_eq!(format!("{}", bits), "10110\n");
    }

    #[test]
    fn validate_as_ref() {
        let bits = BitsArray {
            bytes: vec![0xAA, 0x55],
            bit_length: 16,
        };
        let slice: &[u8] = bits.as_ref();
        assert_eq!(slice, &[0xAA, 0x55]);
    }
}
