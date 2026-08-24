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

use crate::semantic::labels;
use crate::semantic::metrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
    ObservableGauge,
    UpDownCounter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricSource {
    Broker,
    Client,
    NameServer,
    Pop,
    Remoting,
    Store,
    Timer,
    RocksDb,
    TieredStore,
    Proxy,
    Controller,
    Observability,
    Mcp,
    Runtime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricDescriptor {
    pub name: &'static str,
    pub kind: MetricKind,
    pub unit: &'static str,
    pub labels: &'static [&'static str],
    pub source: MetricSource,
}

include!("catalog/generated.rs");

pub const fn java_metrics() -> &'static [MetricDescriptor] {
    JAVA_METRICS
}

pub const fn rust_metrics() -> &'static [MetricDescriptor] {
    RUST_METRICS
}

#[cfg(test)]
mod tests;
