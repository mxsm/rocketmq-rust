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

//! Bounded TCP/TLS transport ownership boundary.
//!
//! File-backed frames use [`api::v1::FileRegion`] to retain an immutable storage lease through writer
//! completion. Portable transfers read one reusable 64 KiB buffer on the runtime-owned blocking
//! I/O lane. With the default-off `linux-sendfile` feature,
//! [`api::v1::FileTransferMode::Auto`] selects Linux `sendfile` for plaintext TCP regions of at
//! least 64 KiB after a cached capability preflight; unsupported filesystems fall back before any
//! frame bytes are written. TLS always uses portable reads so that payload bytes still pass
//! through the userspace rustls record layer.
//!
//! `Bytes` sharing and vectored writes are userspace less-copy techniques. Only the optional
//! plaintext file-region backend avoids the file-to-userspace body copy; it does not imply NIC
//! offload, remote acknowledgement, or support for `MSG_ZEROCOPY`.

mod admission;
mod backend;
mod base;
#[cfg(feature = "test-support")]
pub mod benchmark_support;
mod client;
mod clients;
mod codec;
mod common;
mod config;
mod config_support;
mod connection;
mod connection_context;
mod deadline;
mod discovery;
mod dispatch;
mod error_helpers;
mod error_response;
mod file_region;
mod file_region_writer;
mod hook_registry;
#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
mod linux;
#[cfg(any(test, feature = "test-support"))]
mod local;
mod net;
pub mod prelude;
mod proxy_protocol;
mod public_api;
mod public_api_v2;
mod remoting;
mod remoting_server;
mod request_ordering;
mod request_processor;
mod rpc;
mod runtime;
mod security;
mod server;
mod session_executor;
#[cfg(feature = "socks")]
mod socks;
mod telemetry;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
mod tls;
mod write_result;
mod write_strategy;
mod writer_runtime;

/// Versioned, intentionally curated public API.
pub mod api {
    /// Stable source API for the 1.x release line.
    pub mod v1 {
        pub use crate::public_api::*;
    }

    /// Curated source API boundary for the 2.x release line.
    ///
    /// Only the immutable request timing and identity values approved for the
    /// 2.x request model are exposed here.
    pub mod v2 {
        pub use crate::public_api_v2::*;
    }
}
