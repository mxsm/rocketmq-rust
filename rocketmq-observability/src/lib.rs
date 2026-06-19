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

pub mod attributes;
pub mod config;
pub mod error;
pub mod exporter;
pub mod init;
pub mod logs;
pub mod metrics;
pub mod noop;
pub mod propagation;
pub mod resource;
pub mod semantic;
pub mod trace;

pub use config::ObservabilityConfig;
pub use error::ObservabilityError;
pub use init::init_observability;
#[cfg(feature = "otel-metrics")]
pub use init::meter;
pub use init::TelemetryGuard;
