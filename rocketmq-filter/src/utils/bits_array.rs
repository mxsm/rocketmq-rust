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
    pub fn from_bytes_with_length(bytes: &[u8], bit_length: usize) -> Result<Self, FilterError> {
        if bytes.is_empty() {
            return Err(FilterError::empty_bytes());
        }
        if bit_length < 1 {
            return Err(FilterError::invalid_bit_length());
        }
        if bit_length < bytes.len() * 8 {
            return Err(FilterError::bit_length_too_small());
        }
        Ok(BitsArray {
            bytes: bytes.to_vec(),
            bit_length,
        })
    }

    /// Create a BitsArray from bytes, using bytes.len() * 8 as bit length
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FilterError> {
        if bytes.is_empty() {
            return Err(FilterError::empty_bytes());
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
    pub fn xor(&mut self, other: &BitsArray) -> Result<(), FilterError> {
        self.check_initialized()?;
        other.check_initialized()?;
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] ^= other.bytes[i];
        }
        Ok(())
    }

    /// XOR operation on a single bit
    pub fn xor_bit(&mut self, bit_pos: usize, set: bool) -> Result<(), FilterError> {
        self.check_bit_position(bit_pos)?;
        let value = self.get_bit(bit_pos)?;
        self.set_bit(bit_pos, value ^ set)
    }

    /// OR operation with another BitsArray
    pub fn or(&mut self, other: &BitsArray) -> Result<(), FilterError> {
        self.check_initialized()?;
        other.check_initialized()?;
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] |= other.bytes[i];
        }
        Ok(())
    }

    /// OR operation on a single bit
    pub fn or_bit(&mut self, bit_pos: usize, set: bool) -> Result<(), FilterError> {
        self.check_bit_position(bit_pos)?;
        if set {
            self.set_bit(bit_pos, true)?;
        }
        Ok(())
    }

    /// AND operation with another BitsArray
    pub fn and(&mut self, other: &BitsArray) -> Result<(), FilterError> {
        self.check_initialized()?;
        other.check_initialized()?;
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] &= other.bytes[i];
        }
        Ok(())
    }

    /// AND operation on a single bit
    pub fn and_bit(&mut self, bit_pos: usize, set: bool) -> Result<(), FilterError> {
        self.check_bit_position(bit_pos)?;
        if !set {
            self.set_bit(bit_pos, false)?;
        }
        Ok(())
    }

    /// NOT operation on a single bit
    pub fn not(&mut self, bit_pos: usize) -> Result<(), FilterError> {
        self.check_bit_position(bit_pos)?;
        let value = self.get_bit(bit_pos)?;
        self.set_bit(bit_pos, !value)
    }

    /// Set a bit at the specified position
    pub fn set_bit(&mut self, bit_pos: usize, set: bool) -> Result<(), FilterError> {
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
    pub fn set_byte(&mut self, byte_pos: usize, set: u8) -> Result<(), FilterError> {
        self.check_byte_position(byte_pos)?;
        self.bytes[byte_pos] = set;
        Ok(())
    }

    /// Get a bit at the specified position
    pub fn get_bit(&self, bit_pos: usize) -> Result<bool, FilterError> {
        self.check_bit_position(bit_pos)?;
        Ok((self.bytes[self.subscript(bit_pos)] & self.position(bit_pos)) != 0)
    }

    /// Get a byte at the specified position
    pub fn get_byte(&self, byte_pos: usize) -> Result<u8, FilterError> {
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

    fn check_byte_position(&self, byte_pos: usize) -> Result<(), FilterError> {
        self.check_initialized()?;
        if byte_pos >= self.byte_length() {
            return Err(FilterError::byte_position_out_of_bounds(byte_pos, self.bytes.len()));
        }
        Ok(())
    }

    fn check_bit_position(&self, bit_pos: usize) -> Result<(), FilterError> {
        self.check_initialized()?;
        if bit_pos >= self.bit_length() {
            return Err(FilterError::bit_position_out_of_bounds(bit_pos, self.bit_length));
        }
        Ok(())
    }

    fn check_initialized(&self) -> Result<(), FilterError> {
        if self.bytes.is_empty() {
            return Err(FilterError::uninitialized());
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
