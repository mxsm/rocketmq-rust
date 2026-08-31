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

use crate::CliSpec;
use crate::GrpcSpec;
use crate::HttpSpec;
use crate::RemotingSpec;

/// Explicit boundary projections owned by an error descriptor.
///
/// Every catalog declaration supplies all four projections. The catalog does
/// not infer protocol behavior from a canonical condition, and callers cannot
/// construct or override projection metadata outside this crate.
///
/// ```compile_fail
/// use rocketmq_error::ProjectionSpec;
///
/// let projection = ProjectionSpec {
///     remoting: todo!(),
///     grpc: todo!(),
///     http: todo!(),
///     cli: todo!(),
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionSpec {
    remoting: RemotingSpec,
    grpc: GrpcSpec,
    http: HttpSpec,
    cli: CliSpec,
}

impl ProjectionSpec {
    #[inline]
    pub(crate) const fn new(remoting: RemotingSpec, grpc: GrpcSpec, http: HttpSpec, cli: CliSpec) -> Self {
        Self {
            remoting,
            grpc,
            http,
            cli,
        }
    }

    /// Returns the RocketMQ remoting response projection.
    #[inline]
    pub const fn remoting(&self) -> RemotingSpec {
        self.remoting
    }

    /// Returns the gRPC payload and transport-status projection.
    #[inline]
    pub const fn grpc(&self) -> GrpcSpec {
        self.grpc
    }

    /// Returns the HTTP status projection.
    #[inline]
    pub const fn http(&self) -> HttpSpec {
        self.http
    }

    /// Returns the CLI exit-code projection.
    #[inline]
    pub const fn cli(&self) -> CliSpec {
        self.cli
    }
}
