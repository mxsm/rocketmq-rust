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

/// No-op LongHistogram implementation
///
/// This is a no-operation histogram that implements the same interface as
/// OpenTelemetry's LongHistogram but does nothing when methods are called.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopLongHistogram;

impl NopLongHistogram {
    /// Creates a new NopLongHistogram instance
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Record a value (no-op)
    #[inline]
    pub fn record(&self, _value: i64) {
        // no-op
    }

    /// Record a value with attributes (no-op)
    #[inline]
    pub fn record_with_attributes(&self, _value: i64, _attributes: &[KeyValue]) {
        // no-op
    }

    /// Record a value with attributes and context (no-op)
    #[inline]
    pub fn record_with_context(&self, _value: i64, _attributes: &[KeyValue], _context: &Context) {
        // no-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nop_long_histogram_default() {
        let histogram = NopLongHistogram;
        // Should not panic
        histogram.record(100);
    }

    #[test]
    fn test_nop_long_histogram_new() {
        let histogram = NopLongHistogram::new();
        // Should not panic
        histogram.record(100);
    }

    #[test]
    fn test_nop_long_histogram_record_with_attributes() {
        let histogram = NopLongHistogram::new();
        let attributes = [KeyValue::new("key", "value")];
        // Should not panic
        histogram.record_with_attributes(100, &attributes);
    }

    #[test]
    fn test_nop_long_histogram_record_with_context() {
        let histogram = NopLongHistogram::new();
        let attributes = [KeyValue::new("key", "value")];
        let context = Context::current();
        // Should not panic
        histogram.record_with_context(100, &attributes, &context);
    }
}
