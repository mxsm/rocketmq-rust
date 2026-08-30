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

//! Minimal imports for Transport composition roots.

pub use crate::api::v1::OneShotTransportClient;
pub use crate::api::v1::RemotingClient;
pub use crate::api::v1::RequestDeadline;
#[allow(
    deprecated,
    reason = "the legacy prelude remains source-compatible until the V1 processor removal"
)]
#[deprecated(since = "1.0.0", note = "Import `api::v2::RequestProcessorV2` explicitly instead")]
pub use crate::api::v1::RequestProcessor;
pub use crate::api::v1::ServerConfig;
pub use crate::api::v1::TransportClient;
pub use crate::api::v1::TransportClientConfig;
pub use crate::api::v1::TransportServer;
