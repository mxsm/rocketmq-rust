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

use crate::config::ObservabilityConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub key: &'static str,
    pub value: String,
}

pub fn base_attributes(config: &ObservabilityConfig) -> Vec<Attribute> {
    let mut attributes = vec![
        Attribute::new("cluster", &config.cluster),
        Attribute::new("node_type", &config.node_type),
    ];

    if !config.node_id.is_empty() {
        attributes.push(Attribute::new("node_id", &config.node_id));
    }

    attributes
}

impl Attribute {
    pub fn new(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}

/// Inline capacity that covers the component attribute sets recorded on hot paths.
#[cfg(feature = "otel-metrics")]
type KeyValues = smallvec::SmallVec<[opentelemetry::KeyValue; 4]>;

/// Attributes recorded with a metric measurement.
///
/// Build a set once and reuse it for repeated measurements: recording hands the prepared
/// attributes to the metrics backend without converting them again. Without the `otel-metrics`
/// feature metrics are not recorded, so added attributes are discarded.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetricAttributes {
    #[cfg(feature = "otel-metrics")]
    key_values: KeyValues,
}

impl MetricAttributes {
    /// Creates an empty attribute set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the set with a string attribute appended.
    #[must_use]
    pub fn with(self, key: &'static str, value: impl Into<String>) -> Self {
        #[cfg(feature = "otel-metrics")]
        {
            let mut attributes = self;
            attributes
                .key_values
                .push(opentelemetry::KeyValue::new(key, value.into()));
            attributes
        }
        #[cfg(not(feature = "otel-metrics"))]
        {
            let _ = (key, value);
            self
        }
    }
}

#[cfg(feature = "otel-metrics")]
impl MetricAttributes {
    /// Wraps attributes that were already collected, reusing the vector allocation.
    pub(crate) fn from_key_values(key_values: Vec<opentelemetry::KeyValue>) -> Self {
        Self {
            key_values: KeyValues::from_vec(key_values),
        }
    }

    /// Wraps a fixed attribute list; up to the inline capacity it stays off the heap.
    pub(crate) fn from_array<const N: usize>(key_values: [opentelemetry::KeyValue; N]) -> Self {
        Self {
            key_values: key_values.into_iter().collect(),
        }
    }

    pub(crate) fn as_key_values(&self) -> &[opentelemetry::KeyValue] {
        &self.key_values
    }

    pub(crate) fn push(&mut self, key_value: opentelemetry::KeyValue) {
        self.key_values.push(key_value);
    }

    pub(crate) fn extend(&mut self, key_values: impl IntoIterator<Item = opentelemetry::KeyValue>) {
        self.key_values.extend(key_values);
    }
}

#[cfg(all(test, feature = "otel-metrics"))]
mod tests {
    use opentelemetry::KeyValue;

    use super::MetricAttributes;

    #[test]
    fn with_appends_string_attributes_in_order() {
        let attributes = MetricAttributes::new()
            .with("topic", "TopicA")
            .with("group", String::from("GroupA"));

        assert_eq!(
            attributes.as_key_values(),
            &[KeyValue::new("topic", "TopicA"), KeyValue::new("group", "GroupA")]
        );
    }
}
