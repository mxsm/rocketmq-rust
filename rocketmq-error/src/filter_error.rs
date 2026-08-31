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

use crate::fields;
use crate::ErrorContext;

/// The category of a SQL filter compilation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterCompileErrorKind {
    /// The expression did not contain any SQL tokens.
    EmptyExpression,
    /// The expression exceeded the configured byte limit.
    ExpressionTooLarge,
    /// The expression exceeded the configured token limit.
    TooManyTokens,
    /// The expression exceeded the configured parser nesting limit.
    NestingLimitExceeded,
    /// The parser encountered an unexpected or missing token.
    UnexpectedToken,
    /// A numeric literal could not be parsed safely.
    InvalidNumber,
    /// A `BETWEEN` expression used invalid constant bounds.
    InvalidBetweenBounds,
    /// An operator was used with an unsupported operand form.
    UnsupportedOperand,
    /// A legacy filter implementation returned only an untyped error.
    LegacyAdapter,
}

/// The SQL compilation stage at which a filter failure occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterCompileStage {
    /// Tokenizing the SQL expression.
    Lex,
    /// Parsing SQL expression structure.
    Parse,
    /// Validating expression semantics.
    Semantic,
    /// Mapping a legacy filter error into the typed API.
    Compatibility,
}

/// A fixed, redaction-safe source classification for filter compilation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterCompileSource {
    /// A SQL-92 message property filter expression.
    Sql92,
}

/// A redaction-safe SQL filter compilation error.
///
/// Positions are original UTF-8 byte offsets into the submitted expression.
/// This type deliberately retains neither the expression nor any token, literal,
/// or property text so it can be surfaced at service boundaries safely.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilterCompileError {
    kind: FilterCompileErrorKind,
    stage: FilterCompileStage,
    position: Option<usize>,
    source: Option<FilterCompileSource>,
}

impl FilterCompileError {
    /// Creates a redaction-safe compile error at an original UTF-8 byte offset.
    pub const fn new(kind: FilterCompileErrorKind, stage: FilterCompileStage, position: Option<usize>) -> Self {
        Self {
            kind,
            stage,
            position,
            source: None,
        }
    }

    /// Creates a redaction-safe compile error with a fixed source classification.
    pub const fn new_with_source(
        kind: FilterCompileErrorKind,
        stage: FilterCompileStage,
        position: Option<usize>,
        source: FilterCompileSource,
    ) -> Self {
        Self {
            kind,
            stage,
            position,
            source: Some(source),
        }
    }

    /// Returns the stable compile failure category.
    pub const fn kind(&self) -> FilterCompileErrorKind {
        self.kind
    }

    /// Returns the compiler stage that reported the failure.
    pub const fn stage(&self) -> FilterCompileStage {
        self.stage
    }

    /// Returns the original UTF-8 byte offset, when the failure has one.
    pub const fn position(&self) -> Option<usize> {
        self.position
    }

    /// Returns the fixed source classification, when the compiler provided one.
    pub const fn source(&self) -> Option<FilterCompileSource> {
        self.source
    }

    /// Returns structured, redaction-safe context for this compile failure.
    pub fn context(&self) -> ErrorContext {
        let context = ErrorContext::new()
            .with_text(fields::FILTER_COMPILE_KIND, format!("{:?}", self.kind))
            .with_text(fields::FILTER_COMPILE_STAGE, format!("{:?}", self.stage));
        let context = match self.position {
            Some(position) => context.with_u64(fields::FILTER_COMPILE_POSITION, position as u64),
            None => context,
        };
        match self.source {
            Some(source) => context.with_text(fields::FILTER_COMPILE_SOURCE, format!("{:?}", source)),
            None => context,
        }
    }
}

impl fmt::Debug for FilterCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterCompileError")
            .field("kind", &self.kind)
            .field("stage", &self.stage)
            .field("position", &self.position)
            .field("source", &self.source)
            .finish()
    }
}

impl fmt::Display for FilterCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SQL filter compilation failed: kind={:?}, stage={:?}, position={:?}, source={:?}",
            self.kind, self.stage, self.position, self.source
        )
    }
}

impl std::error::Error for FilterCompileError {}

/// Error types for Filter operations
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum FilterError {
    /// SQL filter compilation failed with structured, redaction-safe details.
    #[error(transparent)]
    Compile(FilterCompileError),

    #[error("Bytes is empty!")]
    /// Represents the empty bytes case.
    EmptyBytes,

    #[error("Bit is less than 1.")]
    /// Represents the invalid bit length case.
    InvalidBitLength,

    #[error("BitLength is less than bytes.length * 8")]
    /// Represents the bit length too small case.
    BitLengthTooSmall,

    #[error("BitPos {0} is greater than {1}")]
    /// Represents the bit position out of bounds case.
    BitPositionOutOfBounds(usize, usize),

    #[error("BytePos {0} is greater than {1}")]
    /// Represents the byte position out of bounds case.
    BytePositionOutOfBounds(usize, usize),

    #[error("Not initialized!")]
    /// Represents the uninitialized case.
    Uninitialized,
}

impl FilterError {
    /// Creates a structured SQL filter compilation error wrapper.
    pub const fn compile(error: FilterCompileError) -> Self {
        Self::Compile(error)
    }

    /// Creates the empty bytes value.
    pub fn empty_bytes() -> Self {
        FilterError::EmptyBytes
    }

    /// Creates the invalid bit length value.
    pub fn invalid_bit_length() -> Self {
        FilterError::InvalidBitLength
    }

    /// Creates the bit length too small value.
    pub fn bit_length_too_small() -> Self {
        FilterError::BitLengthTooSmall
    }

    /// Creates the bit position out of bounds value.
    pub fn bit_position_out_of_bounds(pos: usize, max: usize) -> Self {
        FilterError::BitPositionOutOfBounds(pos, max)
    }

    /// Creates the byte position out of bounds value.
    pub fn byte_position_out_of_bounds(pos: usize, max: usize) -> Self {
        FilterError::BytePositionOutOfBounds(pos, max)
    }

    /// Creates the uninitialized value.
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
