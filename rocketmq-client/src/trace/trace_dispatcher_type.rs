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
