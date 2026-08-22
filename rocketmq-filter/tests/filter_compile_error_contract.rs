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

use rocketmq_error::RocketMQError;
use rocketmq_filter::expression::Expression;
use rocketmq_filter::filter::Filter;
use rocketmq_filter::filter::FilterCompileErrorKind;
use rocketmq_filter::filter::FilterCompileSource;
use rocketmq_filter::filter::FilterCompileStage;
use rocketmq_filter::filter::SqlFilter;

fn assert_compile_error(filter: &SqlFilter, expression: &str, kind: FilterCompileErrorKind, stage: FilterCompileStage) {
    let error = rejected(filter.try_compile(expression));
    assert_eq!(error.kind(), kind, "expression: {expression:?}");
    assert_eq!(error.stage(), stage, "expression: {expression:?}");
    assert_eq!(error.source(), Some(FilterCompileSource::Sql92));
}

fn rejected<T>(
    result: Result<T, rocketmq_filter::filter::FilterCompileError>,
) -> rocketmq_filter::filter::FilterCompileError {
    match result {
        Ok(_) => panic!("expression should be rejected"),
        Err(error) => error,
    }
}

#[test]
fn sql_compile_errors_cover_kinds_stages_and_byte_offsets() {
    let filter = SqlFilter::new();
    assert_compile_error(
        &filter,
        "   ",
        FilterCompileErrorKind::EmptyExpression,
        FilterCompileStage::Parse,
    );
    assert_eq!(rejected(filter.try_compile("   ")).position(), Some(0));

    assert_compile_error(
        &filter,
        &"x".repeat(64 * 1024 + 1),
        FilterCompileErrorKind::ExpressionTooLarge,
        FilterCompileStage::Lex,
    );
    assert_compile_error(
        &filter,
        &("flag OR ".repeat(2_049) + "flag"),
        FilterCompileErrorKind::TooManyTokens,
        FilterCompileStage::Lex,
    );
    assert_compile_error(
        &filter,
        &("(".repeat(129) + "flag" + &")".repeat(129)),
        FilterCompileErrorKind::NestingLimitExceeded,
        FilterCompileStage::Parse,
    );

    let utf8_with_leading_whitespace = "  name = '秘密' @";
    let utf8_error = rejected(filter.try_compile(utf8_with_leading_whitespace));
    assert_eq!(utf8_error.kind(), FilterCompileErrorKind::UnexpectedToken);
    assert_eq!(utf8_error.stage(), FilterCompileStage::Lex);
    assert_eq!(utf8_error.position(), utf8_with_leading_whitespace.find('@'));
    assert_compile_error(
        &filter,
        "name = 'unterminated",
        FilterCompileErrorKind::UnexpectedToken,
        FilterCompileStage::Lex,
    );
    assert_compile_error(
        &filter,
        "name = 1e",
        FilterCompileErrorKind::InvalidNumber,
        FilterCompileStage::Lex,
    );
    assert_compile_error(
        &filter,
        "name = 9223372036854775808",
        FilterCompileErrorKind::InvalidNumber,
        FilterCompileStage::Lex,
    );
    assert_compile_error(
        &filter,
        "name = 1e309",
        FilterCompileErrorKind::InvalidNumber,
        FilterCompileStage::Lex,
    );

    assert_compile_error(
        &filter,
        "name =",
        FilterCompileErrorKind::UnexpectedToken,
        FilterCompileStage::Parse,
    );
    assert_compile_error(
        &filter,
        "name = 1 2",
        FilterCompileErrorKind::UnexpectedToken,
        FilterCompileStage::Parse,
    );
    let null_bounds = "name BETWEEN NULL AND 1";
    assert_compile_error(
        &filter,
        null_bounds,
        FilterCompileErrorKind::InvalidBetweenBounds,
        FilterCompileStage::Semantic,
    );
    assert_eq!(
        rejected(filter.try_compile(null_bounds)).position(),
        null_bounds.find("NULL")
    );
    let high_null_bounds = "name BETWEEN 1 AND NULL";
    assert_compile_error(
        &filter,
        high_null_bounds,
        FilterCompileErrorKind::InvalidBetweenBounds,
        FilterCompileStage::Semantic,
    );
    assert_eq!(
        rejected(filter.try_compile(high_null_bounds)).position(),
        high_null_bounds.find("NULL")
    );
    let reversed_bounds = "name BETWEEN 2 AND 1";
    assert_compile_error(
        &filter,
        reversed_bounds,
        FilterCompileErrorKind::InvalidBetweenBounds,
        FilterCompileStage::Semantic,
    );
    assert_eq!(
        rejected(filter.try_compile(reversed_bounds)).position(),
        reversed_bounds.rfind('1')
    );
    let incomparable_bounds = "name BETWEEN TRUE AND FALSE";
    assert_compile_error(
        &filter,
        incomparable_bounds,
        FilterCompileErrorKind::UnsupportedOperand,
        FilterCompileStage::Semantic,
    );
    assert_eq!(
        rejected(filter.try_compile(incomparable_bounds)).position(),
        incomparable_bounds.find("FALSE")
    );
    assert_compile_error(
        &filter,
        "name CONTAINS 1",
        FilterCompileErrorKind::UnsupportedOperand,
        FilterCompileStage::Semantic,
    );
}

#[test]
fn typed_errors_and_unified_context_redact_the_submitted_sql() {
    let filter = SqlFilter::new();
    let expression = "property = 'sensitive-literal' @";
    let error = rejected(filter.try_compile(expression));
    let typed_context = error.context().to_string();
    let unified: RocketMQError = error.into();
    let unified_context = unified.context().to_string();

    for rendered in [error.to_string(), format!("{error:?}"), typed_context, unified_context] {
        assert!(rendered.contains("UnexpectedToken") || rendered.contains("filter_compile_kind"));
        assert!(!rendered.contains(expression));
        assert!(!rendered.contains("sensitive-literal"));
    }
}

#[test]
fn compilation_preserves_trimmed_sql_compatibility_and_original_offsets() {
    let filter = SqlFilter::new();
    let surrounded_by_large_ascii_whitespace = format!(
        "{}name = 'blue'{}",
        " ".repeat(64 * 1024 + 1),
        " ".repeat(64 * 1024 + 1)
    );
    assert!(filter.try_compile(&surrounded_by_large_ascii_whitespace).is_ok());

    assert!(filter.try_compile("\u{00a0}name = 'blue'\u{00a0}").is_ok());

    let utf8_with_leading_whitespace = "  name = '秘密' @";
    let error = rejected(filter.try_compile(utf8_with_leading_whitespace));
    assert_eq!(error.position(), utf8_with_leading_whitespace.find('@'));
}

#[derive(Debug)]
struct LegacyOnlyFilter;

#[allow(
    deprecated,
    reason = "This dedicated contract test verifies the default legacy Filter compatibility adapter."
)]
impl Filter for LegacyOnlyFilter {
    fn compile(&self, _expr: &str) -> Result<Box<dyn Expression>, rocketmq_filter::filter::FilterError> {
        Err(rocketmq_filter::filter::FilterError::new("legacy secret expression"))
    }

    fn of_type(&self) -> &str {
        "LEGACY_ONLY"
    }
}

#[test]
fn legacy_filters_map_failures_without_leaking_their_message() {
    let error = rejected(LegacyOnlyFilter.try_compile("legacy secret expression"));
    assert_eq!(error.kind(), FilterCompileErrorKind::LegacyAdapter);
    assert_eq!(error.stage(), FilterCompileStage::Compatibility);
    assert_eq!(error.position(), None);
    assert_eq!(error.source(), None);
    assert!(!error.to_string().contains("legacy secret expression"));
}

#[test]
#[allow(
    deprecated,
    reason = "This dedicated contract test verifies deprecated compile behavior."
)]
fn deprecated_compile_preserves_success_and_uses_a_fixed_failure_projection() {
    let filter = SqlFilter::new();
    assert!(filter.try_compile("name = 'blue'").is_ok());
    assert!(filter.compile("name = 'blue'").is_ok());
    let legacy_error = match filter.compile("name = 'secret") {
        Ok(_) => panic!("invalid expression should not compile"),
        Err(error) => error,
    };
    assert_eq!(
        legacy_error.to_string(),
        "FilterError: SQL92 expression compilation failed"
    );
}
