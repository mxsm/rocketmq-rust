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
use rocketmq_error::Error;
use rocketmq_error::PROTOCOL_FILTER_INVALID;

#[test]
fn compile_error_contract_is_typed_and_redaction_safe() {
    let error = FilterCompileError::new_with_source(
        FilterCompileErrorKind::UnexpectedToken,
        FilterCompileStage::Parse,
        Some(7),
        FilterCompileSource::Sql92,
    );

    assert_eq!(error.kind(), FilterCompileErrorKind::UnexpectedToken);
    assert_eq!(error.stage(), FilterCompileStage::Parse);
    assert_eq!(error.position(), Some(7));
    assert_eq!(error.source(), Some(FilterCompileSource::Sql92));
    assert_eq!(error.descriptor(), &PROTOCOL_FILTER_INVALID);

    let display = error.to_string();
    let debug = format!("{error:?}");
    let context = error.context().to_string();
    for rendered in [display, debug, context] {
        assert!(rendered.contains("UnexpectedToken") || rendered.contains("filter_compile_kind"));
        assert!(!rendered.contains("secret_expression"));
    }

    let canonical: Error = error.into();
    assert_eq!(canonical.descriptor(), &PROTOCOL_FILTER_INVALID);
    let canonical_context = canonical.context().to_string();
    assert!(canonical_context.contains("filter_compile_kind=<redacted>"));
    assert!(canonical_context.contains("filter_compile_source=<redacted>"));
    assert!(!canonical_context.contains("UnexpectedToken"));
    assert!(!canonical_context.contains("Sql92"));
    assert!(!canonical_context.contains("secret_expression"));
}
