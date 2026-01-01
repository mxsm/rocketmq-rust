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

use opentelemetry::Context;
use opentelemetry::KeyValue;

/// No-op LongCounter implementation (equivalent to Java NopLongCounter)
///
/// This is a no-operation counter that implements the same interface as
/// OpenTelemetry's LongCounter but does nothing when methods are called.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopLongCounter;

impl NopLongCounter {
    /// Creates a new NopLongCounter instance
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Add a value (no-op)
    #[inline]
    pub fn add(&self, _value: i64) {
        // no-op
    }

    /// Add a value with attributes (no-op)
    #[inline]
    pub fn add_with_attributes(&self, _value: i64, _attributes: &[KeyValue]) {
        // no-op
    }

    /// Add a value with attributes and context (no-op)
    #[inline]
    pub fn add_with_context(&self, _value: i64, _attributes: &[KeyValue], _context: &Context) {
        // no-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nop_long_counter_default() {
        let counter = NopLongCounter;
        // Should not panic
        counter.add(100);
    }

    #[test]
    fn test_nop_long_counter_new() {
        let counter = NopLongCounter::new();
        // Should not panic
        counter.add(100);
    }

    #[test]
    fn test_nop_long_counter_add_with_attributes() {
        let counter = NopLongCounter::new();
        let attributes = [KeyValue::new("key", "value")];
        // Should not panic
        counter.add_with_attributes(100, &attributes);
    }

    #[test]
    fn test_nop_long_counter_add_with_context() {
        let counter = NopLongCounter::new();
        let attributes = [KeyValue::new("key", "value")];
        let context = Context::current();
        // Should not panic
        counter.add_with_context(100, &attributes, &context);
    }
}
