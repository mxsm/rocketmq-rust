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

//! Generic metric instrument types used by service-owned metric managers.
//!
//! Service crates own their metric schemas and lifecycle adapters, while this
//! module keeps the OpenTelemetry SDK dependency behind the observability boundary.

pub use opentelemetry::metrics::Counter;
pub use opentelemetry::metrics::Histogram;
pub use opentelemetry::metrics::Meter;
pub use opentelemetry::metrics::MeterProvider;
pub use opentelemetry::metrics::UpDownCounter;
pub use opentelemetry::KeyValue;
pub use opentelemetry_sdk::metrics::SdkMeterProvider;
