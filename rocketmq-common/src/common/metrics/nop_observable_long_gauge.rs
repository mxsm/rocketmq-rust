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

/// No-op ObservableLongGauge implementation (equivalent to Java NopObservableLongGauge)
///
/// This is a no-operation observable gauge that does nothing.
/// Observable gauges are typically registered with callbacks that are invoked
/// during metric collection. This no-op implementation is used when metrics
/// are disabled.
///
/// # Note
/// Unlike Counter and Histogram, ObservableGauge in OpenTelemetry is registered
/// with a callback and doesn't have explicit record methods. This struct serves
/// as a placeholder when metrics are disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NopObservableLongGauge;

impl NopObservableLongGauge {
    /// Creates a new NopObservableLongGauge instance
    #[inline]
    pub fn new() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nop_observable_long_gauge_default() {
        let _gauge = NopObservableLongGauge;
        // Just ensure it can be created without issues
    }

    #[test]
    fn test_nop_observable_long_gauge_new() {
        let _gauge = NopObservableLongGauge::new();
        // Just ensure it can be created without issues
    }

    #[test]
    fn test_nop_observable_long_gauge_is_copy() {
        let gauge1 = NopObservableLongGauge::new();
        let gauge2 = gauge1; // Copy
        let _gauge3 = gauge2; // Still can use gauge2
                              // Ensures Copy trait works
    }
}
