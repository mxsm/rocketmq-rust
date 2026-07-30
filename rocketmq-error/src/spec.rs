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

use crate::boundary::CliSpec;
use crate::boundary::GrpcSpec;
use crate::boundary::HttpSpec;
use crate::boundary::RemotingSpec;
use crate::context::RedactionPolicy;
use crate::kind::ErrorCategory;
use crate::kind::ErrorCode;
use crate::kind::ErrorKind;
use crate::kind::ErrorScope;
use crate::policy::ObserveSpec;
use crate::policy::RecoverySpec;

mod registry;

/// Static registry for all current error kinds.
///
/// The public path remains anchored in this module while the entries are
/// organized by domain in the private registry unit.
pub const ALL_ERROR_SPECS: &[ErrorSpec] = registry::ERROR_SPECS;

/// Static metadata for one [`ErrorKind`].
///
/// The registry is the single source for machine-readable error identity. Later
/// changes extend this struct with protocol, retry, redaction, and observability
/// fields without changing the lookup contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrorSpec {
    pub kind: ErrorKind,
    pub code: ErrorCode,
    pub scope: ErrorScope,
    pub category: ErrorCategory,
    pub public_message: &'static str,
    pub remoting: RemotingSpec,
    pub grpc: GrpcSpec,
    pub http: HttpSpec,
    pub cli: CliSpec,
    pub recovery: RecoverySpec,
    pub observe: ObserveSpec,
    pub redact: RedactionPolicy,
}

impl ErrorSpec {
    #[inline]
    pub const fn new(kind: ErrorKind, public_message: &'static str) -> Self {
        Self {
            kind,
            code: kind.code(),
            scope: kind.scope(),
            category: kind.category(),
            public_message,
            remoting: RemotingSpec::for_kind(kind),
            grpc: GrpcSpec::for_kind(kind),
            http: HttpSpec::for_kind(kind),
            cli: CliSpec::for_kind(kind),
            recovery: RecoverySpec::for_kind(kind),
            observe: ObserveSpec::for_kind(kind),
            redact: RedactionPolicy::for_kind(kind),
        }
    }
}

/// Return the static metadata for an error kind.
#[inline]
pub fn error_spec(kind: ErrorKind) -> &'static ErrorSpec {
    ALL_ERROR_SPECS
        .iter()
        .find(|spec| spec.kind == kind)
        .expect("all ErrorKind variants must have an ErrorSpec")
}

impl ErrorKind {
    /// Return the static metadata for this error kind.
    #[inline]
    pub fn spec(self) -> &'static ErrorSpec {
        error_spec(self)
    }
}
