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

#[cfg(feature = "otlp-metrics")]
pub fn init_otlp_meter_provider(
    _config: &crate::config::ObservabilityConfig,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider, crate::error::ObservabilityError> {
    Ok(opentelemetry_sdk::metrics::SdkMeterProvider::builder().build())
}

#[cfg(feature = "otlp-traces")]
pub fn init_otlp_tracer_provider(
    _config: &crate::config::ObservabilityConfig,
) -> Result<opentelemetry_sdk::trace::SdkTracerProvider, crate::error::ObservabilityError> {
    Ok(opentelemetry_sdk::trace::SdkTracerProvider::builder().build())
}
