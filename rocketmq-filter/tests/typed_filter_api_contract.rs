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

use std::sync::Arc;

use rocketmq_error::ErrorKind;
use rocketmq_error::RocketMQError;
use rocketmq_filter::expression::MessageEvaluationContext;
use rocketmq_filter::expression::Value;
use rocketmq_filter::filter::Filter;
use rocketmq_filter::filter::FilterCompileErrorKind;
use rocketmq_filter::filter::FilterCompileSource;
use rocketmq_filter::filter::FilterCompileStage;
use rocketmq_filter::filter::FilterFactory;

fn sql92_filter_from_factory() -> Arc<dyn Filter> {
    FilterFactory::instance()
        .get("SQL92")
        .expect("the factory registers SQL92 during static initialization")
}

#[test]
fn factory_trait_object_compiles_and_evaluates_through_the_typed_api() {
    let filter = sql92_filter_from_factory();
    let expression = filter
        .try_compile("color = 'blue' AND retries >= 3")
        .expect("valid SQL92 expression should compile");
    let mut context = MessageEvaluationContext::new();
    context.put("color", "blue");
    context.put("retries", "5");

    assert_eq!(expression.evaluate(&context).unwrap(), Value::Boolean(true));
}

#[test]
fn factory_trait_object_typed_failures_preserve_safe_metadata_and_unified_kind() {
    let filter = sql92_filter_from_factory();
    let submitted = "  name = '秘密' @";
    let error = match filter.try_compile(submitted) {
        Ok(_) => panic!("the unexpected token should be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), FilterCompileErrorKind::UnexpectedToken);
    assert_eq!(error.stage(), FilterCompileStage::Lex);
    assert_eq!(error.position(), submitted.find('@'));
    assert_eq!(error.source(), Some(FilterCompileSource::Sql92));

    let unified: RocketMQError = error.into();
    assert_eq!(unified.kind(), ErrorKind::Filter);
    for rendered in [
        error.to_string(),
        format!("{error:?}"),
        error.context().to_string(),
        unified.to_string(),
        format!("{unified:?}"),
        unified.context().to_string(),
    ] {
        assert!(!rendered.contains(submitted));
        assert!(!rendered.contains("秘密"));
    }
}

#[test]
#[allow(
    deprecated,
    reason = "This external contract fixture verifies that the 1.x Filter::compile facade remains available."
)]
fn factory_trait_object_keeps_the_1_x_compile_facade_available() {
    let filter = sql92_filter_from_factory();

    assert!(filter.compile("color = 'blue'").is_ok());
}
