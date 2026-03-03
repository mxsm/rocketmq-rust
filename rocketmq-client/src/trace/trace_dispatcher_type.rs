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

/// Type of trace dispatcher indicating the source of trace data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TraceDispatcherType {
    /// Trace data from message producers
    #[default]
    Producer,
    /// Trace data from message consumers
    Consumer,
}

impl TraceDispatcherType {
    #[inline]
    pub fn is_producer(&self) -> bool {
        matches!(self, Self::Producer)
    }

    #[inline]
    pub fn is_consumer(&self) -> bool {
        matches!(self, Self::Consumer)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn trace_dispatcher_type_default() {
        let default_type: TraceDispatcherType = Default::default();
        assert_eq!(default_type, TraceDispatcherType::Producer);
    }

    #[test]
    fn trace_dispatcher_type_is_producer() {
        let dispatcher_type = TraceDispatcherType::Producer;
        assert!(dispatcher_type.is_producer());
        assert!(!dispatcher_type.is_consumer());
    }

    #[test]
    fn trace_dispatcher_type_is_consumer() {
        let dispatcher_type = TraceDispatcherType::Consumer;
        assert!(dispatcher_type.is_consumer());
        assert!(!dispatcher_type.is_producer());
    }

    #[test]
    fn trace_dispatcher_type_copy() {
        let dispatcher_type: TraceDispatcherType = TraceDispatcherType::Producer;
        let copied_type = dispatcher_type;
        assert_eq!(dispatcher_type, copied_type);
    }

    #[test]
    fn trace_dispatcher_type_clone() {
        let dispatcher_type: TraceDispatcherType = TraceDispatcherType::Consumer;
        let cloned_type = dispatcher_type.clone();
        assert_eq!(dispatcher_type, cloned_type);
    }

    #[test]
    fn trace_dispatcher_type_debug() {
        let dispatcher_type = TraceDispatcherType::Producer;
        let debug_str = format!("{:?}", dispatcher_type);
        assert_eq!(debug_str, "Producer");
    }

    #[test]
    fn trace_dispatcher_type_equality() {
        let type1 = TraceDispatcherType::Producer;
        let type2 = TraceDispatcherType::Producer;
        let type3 = TraceDispatcherType::Consumer;
        assert_eq!(type1, type2);
        assert_ne!(type1, type3);
    }
}
