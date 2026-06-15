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

pub const TRACEPARENT: &str = crate::semantic::trace::TRACEPARENT;
pub const TRACESTATE: &str = crate::semantic::trace::TRACESTATE;
pub const BAGGAGE: &str = crate::semantic::trace::BAGGAGE;

pub trait MessagePropertiesLike {
    fn get_property(&self, key: &str) -> Option<&str>;

    fn put_property(&mut self, key: &str, value: String);
}

#[cfg(feature = "otel-traces")]
pub struct MessagePropertyInjector<'a, T> {
    inner: &'a mut T,
}

#[cfg(feature = "otel-traces")]
impl<'a, T> MessagePropertyInjector<'a, T> {
    pub fn new(inner: &'a mut T) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "otel-traces")]
impl<T> opentelemetry::propagation::Injector for MessagePropertyInjector<'_, T>
where
    T: MessagePropertiesLike,
{
    fn set(&mut self, key: &str, value: String) {
        self.inner.put_property(key, value);
    }
}

#[cfg(feature = "otel-traces")]
pub struct MessagePropertyExtractor<'a, T> {
    inner: &'a T,
}

#[cfg(feature = "otel-traces")]
impl<'a, T> MessagePropertyExtractor<'a, T> {
    pub fn new(inner: &'a T) -> Self {
        Self { inner }
    }
}

#[cfg(feature = "otel-traces")]
impl<T> opentelemetry::propagation::Extractor for MessagePropertyExtractor<'_, T>
where
    T: MessagePropertiesLike,
{
    fn get(&self, key: &str) -> Option<&str> {
        self.inner.get_property(key)
    }

    fn keys(&self) -> Vec<&str> {
        vec![TRACEPARENT, TRACESTATE, BAGGAGE]
    }
}
