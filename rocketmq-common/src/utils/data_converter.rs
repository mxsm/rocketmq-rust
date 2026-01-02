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

use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct DataConverter;

impl DataConverter {
    pub const CHARSET_UTF8: &'static str = "UTF-8";

    pub fn long_to_bytes(v: i64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    pub fn set_bit(value: i32, index: usize, flag: bool) -> i32 {
        assert!(index < 32, "Bit index out of range. Must be < 32.");
        if flag {
            value | (1 << index)
        } else {
            value & !(1 << index)
        }
    }

    pub fn get_bit(value: i32, index: usize) -> bool {
        assert!(index < 32, "Bit index out of range. Must be < 32.");
        (value & (1 << index)) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::DataConverter;

    #[test]
    fn long_to_bytes_converts_correctly() {
        let value: i64 = 123456789;
        let bytes = DataConverter::long_to_bytes(value);
        assert_eq!(bytes, value.to_be_bytes().to_vec());
    }

    #[test]
    fn set_bit_sets_bit_correctly() {
        let value: i32 = 0b0000_0000;
        let result = DataConverter::set_bit(value, 3, true);
        assert_eq!(result, 0b0000_1000);
    }

    #[test]
    fn set_bit_clears_bit_correctly() {
        let value: i32 = 0b0000_1000;
        let result = DataConverter::set_bit(value, 3, false);
        assert_eq!(result, 0b0000_0000);
    }

    #[test]
    fn get_bit_returns_true_when_bit_is_set() {
        let value: i32 = 0b0000_1000;
        let result = DataConverter::get_bit(value, 3);
        assert!(result);
    }

    #[test]
    fn get_bit_returns_false_when_bit_is_not_set() {
        let value: i32 = 0b0000_0000;
        let result = DataConverter::get_bit(value, 3);
        assert!(!result);
    }
}
