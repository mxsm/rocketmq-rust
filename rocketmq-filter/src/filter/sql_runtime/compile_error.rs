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

use rocketmq_error::FilterCompileError;
use rocketmq_error::FilterCompileErrorKind;
use rocketmq_error::FilterCompileSource;
use rocketmq_error::FilterCompileStage;

use super::Token;

/// A token paired with its original UTF-8 byte offset.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SpannedToken {
    pub(super) token: Token,
    pub(super) position: usize,
}

impl SpannedToken {
    pub(super) const fn new(token: Token, position: usize) -> Self {
        Self { token, position }
    }
}

pub(super) const fn lex_error(kind: FilterCompileErrorKind, position: usize) -> FilterCompileError {
    FilterCompileError::new_with_source(
        kind,
        FilterCompileStage::Lex,
        Some(position),
        FilterCompileSource::Sql92,
    )
}

pub(super) const fn empty_expression() -> FilterCompileError {
    FilterCompileError::new_with_source(
        FilterCompileErrorKind::EmptyExpression,
        FilterCompileStage::Parse,
        Some(0),
        FilterCompileSource::Sql92,
    )
}

pub(super) const fn nesting_limit_exceeded(position: usize) -> FilterCompileError {
    FilterCompileError::new_with_source(
        FilterCompileErrorKind::NestingLimitExceeded,
        FilterCompileStage::Parse,
        Some(position),
        FilterCompileSource::Sql92,
    )
}

pub(super) const fn parse_error(position: usize) -> FilterCompileError {
    FilterCompileError::new_with_source(
        FilterCompileErrorKind::UnexpectedToken,
        FilterCompileStage::Parse,
        Some(position),
        FilterCompileSource::Sql92,
    )
}

pub(super) const fn semantic_error(kind: FilterCompileErrorKind, position: usize) -> FilterCompileError {
    FilterCompileError::new_with_source(
        kind,
        FilterCompileStage::Semantic,
        Some(position),
        FilterCompileSource::Sql92,
    )
}

#[allow(
    deprecated,
    reason = "Only SqlFilter's deprecated compile wrapper projects typed errors to the legacy string error."
)]
pub(in crate::filter) fn legacy_projection(error: FilterCompileError) -> super::super::filter_spi::FilterError {
    let _ = error;
    super::super::filter_spi::FilterError::new("SQL92 expression compilation failed")
}
