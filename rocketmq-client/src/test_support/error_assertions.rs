// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_error::{fields, ErrorContext, ErrorDescriptor, ViewValueRef};

use crate::{ClientError, MQClientException};

#[track_caller]
pub(crate) fn assert_error(error: &ClientError, descriptor: &'static ErrorDescriptor) {
    assert_eq!(error.code(), descriptor.code());
    assert_eq!(
        error.to_string(),
        format!("{}: {}", descriptor.code(), descriptor.public_message())
    );
    error.public_view().expect("valid public error context");
    error.diagnostic_view().expect("valid diagnostic error context");
}

#[track_caller]
pub(crate) fn assert_context_field(error: &ClientError, name: &str, expected: ViewValueRef<'_>) {
    let view = error.diagnostic_view().expect("valid diagnostic error context");
    let actual = view
        .fields()
        .find(|field| field.name() == name)
        .map(|field| field.value());
    assert_eq!(actual, Some(expected), "unexpected context field {name}");
}

#[track_caller]
pub(crate) fn assert_invalid_argument(error: &ClientError) {
    assert_error(error, &rocketmq_error::CORE_ARGUMENT_INVALID);
    assert_context_field(error, fields::MESSAGE_PRESENT.schema().name(), ViewValueRef::Redacted);
    assert_eq!(error.public_view().unwrap().fields().count(), 0);
}

#[track_caller]
pub(crate) fn assert_not_initialized(error: &ClientError, component: &str) {
    assert_error(error, &rocketmq_error::CORE_LIFECYCLE_NOT_INITIALIZED);
    assert_eq!(
        error.context(),
        &ErrorContext::new().with_text(fields::COMPONENT_NAME, component)
    );
}

#[track_caller]
pub(crate) fn assert_invalid_state(error: &ClientError, expected: &str, actual: &str) {
    assert_error(error, &rocketmq_error::CLIENT_LIFECYCLE_INVALID_STATE);
    assert_eq!(
        error.context(),
        &ErrorContext::new()
            .with_text(fields::EXPECTED_STATE, expected)
            .with_text(fields::ACTUAL_STATE, actual)
    );
}

#[track_caller]
pub(crate) fn client_exception(error: &ClientError) -> &MQClientException {
    assert_invalid_argument(error);
    // Java compatibility text belongs to the typed source, outside public rendering.
    error
        .source_ref::<MQClientException>()
        .expect("retained Java client exception")
}
