// Copyright 2025-2026 The RocketMQ Rust Authors
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

#[derive(Clone, Debug)]
pub struct BitsArray {
    bytes: Vec<u8>,
    bit_length: usize,
}

impl BitsArray {
    pub fn create(bit_length: usize) -> Self {
        let bytes = vec![0u8; bit_length.div_ceil(8)];
        BitsArray { bytes, bit_length }
    }

    pub fn from_bytes(bytes: &[u8], bit_length: Option<usize>) -> Self {
        if bytes.is_empty() {
            panic!("Bytes is empty!");
        }
        let bit_length = bit_length.unwrap_or(bytes.len() * 8);
        if bit_length < 1 {
            panic!("Bit is less than 1.");
        }
        if bit_length < bytes.len() * 8 {
            panic!("BitLength is less than bytes.len() * 8");
        }
        BitsArray {
            bytes: bytes.to_vec(),
            bit_length,
        }
    }

    pub fn bit_length(&self) -> usize {
        self.bit_length
    }

    pub fn byte_length(&self) -> usize {
        self.bytes.len()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn xor(&mut self, other: &BitsArray) {
        self.check_initialized();
        other.check_initialized();
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] ^= other.get_byte(i);
        }
    }

    pub fn xor_bit(&mut self, bit_pos: usize, set: bool) {
        self.check_bit_position(bit_pos);
        let value = self.get_bit(bit_pos);
        self.set_bit(bit_pos, value ^ set);
    }

    pub fn or(&mut self, other: &BitsArray) {
        self.check_initialized();
        other.check_initialized();
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] |= other.get_byte(i);
        }
    }

    pub fn or_bit(&mut self, bit_pos: usize, set: bool) {
        self.check_bit_position(bit_pos);
        if set {
            self.set_bit(bit_pos, true);
        }
    }

    pub fn and(&mut self, other: &BitsArray) {
        self.check_initialized();
        other.check_initialized();
        let min_len = self.byte_length().min(other.byte_length());
        for i in 0..min_len {
            self.bytes[i] &= other.get_byte(i);
        }
    }

    pub fn and_bit(&mut self, bit_pos: usize, set: bool) {
        self.check_bit_position(bit_pos);
        if !set {
            self.set_bit(bit_pos, false);
        }
    }

    pub fn not(&mut self, bit_pos: usize) {
        self.check_bit_position(bit_pos);
        let value = self.get_bit(bit_pos);
        self.set_bit(bit_pos, !value);
    }

    pub fn set_bit(&mut self, bit_pos: usize, set: bool) {
        self.check_bit_position(bit_pos);
        let sub = self.subscript(bit_pos);
        let pos = self.position(bit_pos);
        if set {
            self.bytes[sub] |= pos;
        } else {
            self.bytes[sub] &= !pos;
        }
    }

    pub fn set_byte(&mut self, byte_pos: usize, set: u8) {
        self.check_byte_position(byte_pos);
        self.bytes[byte_pos] = set;
    }

    pub fn get_bit(&self, bit_pos: usize) -> bool {
        self.check_bit_position(bit_pos);
        (self.bytes[self.subscript(bit_pos)] & self.position(bit_pos)) != 0
    }

    pub fn get_byte(&self, byte_pos: usize) -> u8 {
        self.check_byte_position(byte_pos);
        self.bytes[byte_pos]
    }

    fn subscript(&self, bit_pos: usize) -> usize {
        bit_pos / 8
    }

    fn position(&self, bit_pos: usize) -> u8 {
        1 << (bit_pos % 8)
    }

    fn check_byte_position(&self, byte_pos: usize) {
        self.check_initialized();
        if byte_pos >= self.byte_length() {
            panic!("BytePos is greater than {}", self.bytes.len());
        }
    }

    fn check_bit_position(&self, bit_pos: usize) {
        self.check_initialized();
        if bit_pos >= self.bit_length() {
            panic!("BitPos is greater than {}", self.bit_length);
        }
    }

    fn check_initialized(&self) {
        if self.bytes.is_empty() {
            panic!("Not initialized!");
        }
    }

    pub fn clone_bits(&self) -> BitsArray {
        BitsArray {
            bytes: self.bytes.clone(),
            bit_length: self.bit_length,
        }
    }
}

impl std::fmt::Display for BitsArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
