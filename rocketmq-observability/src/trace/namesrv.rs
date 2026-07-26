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

pub use super::span_names::NAMESRV_BROKER_REGISTRATION as REGISTER_BROKER;
pub use super::span_names::NAMESRV_ROUTE_LOOKUP as GET_ROUTEINFO;

pub fn route_lookup_span() -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        tracing::info_span!(GET_ROUTEINFO, result = tracing::field::Empty)
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        tracing::Span::none()
    }
}

pub fn broker_registration_span() -> tracing::Span {
    #[cfg(feature = "otel-traces")]
    {
        tracing::info_span!(REGISTER_BROKER, result = tracing::field::Empty)
    }

    #[cfg(not(feature = "otel-traces"))]
    {
        tracing::Span::none()
    }
}
