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

/// No-op long counter used when metrics are disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopLongCounter;

impl NopLongCounter {
    /// Creates a new no-op long counter.
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Adds a value to the counter.
    #[inline]
    pub fn add(&self, _value: i64) {}
}

/// No-op long histogram used when metrics are disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopLongHistogram;

impl NopLongHistogram {
    /// Creates a new no-op long histogram.
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Records a value in the histogram.
    #[inline]
    pub fn record(&self, _value: i64) {}
}

/// No-op observable long gauge used when metrics are disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopObservableLongGauge;

impl NopObservableLongGauge {
    /// Creates a new no-op observable long gauge.
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nop_counter_accepts_values() {
        let counter = NopLongCounter::new();
        counter.add(100);
    }

    #[test]
    fn nop_histogram_accepts_values() {
        let histogram = NopLongHistogram::new();
        histogram.record(100);
    }

    #[test]
    fn nop_gauge_is_copyable() {
        let gauge = NopObservableLongGauge::new();
        let copied = gauge;
        let _ = copied;
    }
}
