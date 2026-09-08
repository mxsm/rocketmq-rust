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

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;

pub(crate) fn argument_invalid(detail: impl Into<String>) -> Error {
    let _ = detail.into();
    Error::new(&rocketmq_error::CORE_ARGUMENT_INVALID)
        .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
}

pub(crate) fn invariant_violated(invariant: &'static str) -> Error {
    Error::new(&rocketmq_error::CORE_INTERNAL_FAILURE)
        .with_context(ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, invariant))
}
