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

pub mod bits_array;
pub mod bloom_filter;
pub mod bloom_filter_data;

use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;

pub(crate) fn invalid_filter(kind: &'static str) -> Error {
    Error::new(&rocketmq_error::PROTOCOL_FILTER_INVALID)
        .with_context(ErrorContext::new().with_text(fields::FILTER_KIND, kind))
}

pub(crate) fn invalid_filter_position(kind: &'static str, position: usize, limit: usize) -> Error {
    Error::new(&rocketmq_error::PROTOCOL_FILTER_INVALID).with_context(
        ErrorContext::new()
            .with_text(fields::FILTER_KIND, kind)
            .with_u64(fields::POSITION, position as u64)
            .with_u64(fields::LIMIT, limit as u64),
    )
}

pub(crate) fn uninitialized_filter() -> Error {
    Error::new(&rocketmq_error::CORE_LIFECYCLE_NOT_INITIALIZED)
        .with_context(ErrorContext::new().with_text(fields::COMPONENT_NAME, "filter.bits_array"))
}
