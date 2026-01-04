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

/// Error types for Filter operations
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum FilterError {
    #[error("Bytes is empty!")]
    EmptyBytes,

    #[error("Bit is less than 1.")]
    InvalidBitLength,

    #[error("BitLength is less than bytes.length * 8")]
    BitLengthTooSmall,

    #[error("BitPos {0} is greater than {1}")]
    BitPositionOutOfBounds(usize, usize),

    #[error("BytePos {0} is greater than {1}")]
    BytePositionOutOfBounds(usize, usize),

    #[error("Not initialized!")]
    Uninitialized,
}

impl FilterError {
    pub fn empty_bytes() -> Self {
        FilterError::EmptyBytes
    }

    pub fn invalid_bit_length() -> Self {
        FilterError::InvalidBitLength
    }

    pub fn bit_length_too_small() -> Self {
        FilterError::BitLengthTooSmall
    }

    pub fn bit_position_out_of_bounds(pos: usize, max: usize) -> Self {
        FilterError::BitPositionOutOfBounds(pos, max)
    }

    pub fn byte_position_out_of_bounds(pos: usize, max: usize) -> Self {
        FilterError::BytePositionOutOfBounds(pos, max)
    }

    pub fn uninitialized() -> Self {
        FilterError::Uninitialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_error() {
        let err = FilterError::empty_bytes();
        assert_eq!(err.to_string(), "Bytes is empty!");

        let err = FilterError::invalid_bit_length();
        assert_eq!(err.to_string(), "Bit is less than 1.");

        let err = FilterError::bit_length_too_small();
        assert_eq!(err.to_string(), "BitLength is less than bytes.length * 8");

        let err = FilterError::bit_position_out_of_bounds(10, 5);
        assert_eq!(err.to_string(), "BitPos 10 is greater than 5");

        let err = FilterError::byte_position_out_of_bounds(8, 4);
        assert_eq!(err.to_string(), "BytePos 8 is greater than 4");

        let err = FilterError::uninitialized();
        assert_eq!(err.to_string(), "Not initialized!");
    }
}
