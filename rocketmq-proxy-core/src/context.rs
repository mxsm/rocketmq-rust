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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use uuid::Uuid;

use crate::error::ProxyError;
use crate::error::ProxyResult;

/// Verified TLS client identity attached by the gRPC transport boundary.
///
/// The bytes are the peer's leaf certificate DER after rustls has validated the configured
/// client-auth policy. Request metadata cannot construct this proof.
#[derive(Clone, PartialEq, Eq)]
pub struct VerifiedTlsIdentity {
    leaf_certificate_der: Arc<[u8]>,
}

impl VerifiedTlsIdentity {
    pub fn from_leaf_certificate_der(certificate: impl Into<Arc<[u8]>>) -> Self {
        Self {
            leaf_certificate_der: certificate.into(),
        }
    }

    pub fn leaf_certificate_der(&self) -> &[u8] {
        self.leaf_certificate_der.as_ref()
    }
}

impl fmt::Debug for VerifiedTlsIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedTlsIdentity")
            .field("leaf_certificate", &"<verified>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResolvedAddressScheme {
    Unspecified,
    Ipv4,
    Ipv6,
    DomainName,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedEndpoint {
    pub scheme: ResolvedAddressScheme,
    pub host: String,
    pub port: u16,
}

/// Transport and request metadata with an opaque, facade-selected principal proof.
///
/// Core does not authenticate or otherwise validate `P`. Security-sensitive facades must
/// specialize this type with a proof whose construction they control and only attach it after
/// successful authentication or an equivalent trusted decision.
#[derive(Debug, Clone)]
pub struct ProxyContextWithPrincipal<P> {
    request_id: String,
    rpc_name: &'static str,
    remote_addr: Option<String>,
    local_addr: Option<String>,
    client_id: Option<String>,
    language: Option<String>,
    client_version: Option<String>,
    namespace: Option<String>,
    connection_id: Option<String>,
    deadline_at: Option<Instant>,
    received_at: Instant,
    authenticated_principal: Option<P>,
}

/// Principal-free Core context used by neutral services and tests.
pub type ProxyContext = ProxyContextWithPrincipal<()>;

/// Owned metadata captured once by an ingress adapter before backend dispatch.
/// Principal proofs are attached separately after authentication.
#[derive(Debug, Clone)]
pub struct ProxyRequestMetadata {
    pub request_id: String,
    pub remote_addr: Option<String>,
    pub local_addr: Option<String>,
    pub client_id: Option<String>,
    pub language: Option<String>,
    pub client_version: Option<String>,
    pub namespace: Option<String>,
    pub connection_id: Option<String>,
    pub deadline_at: Option<Instant>,
    pub received_at: Instant,
}

impl<P> ProxyContextWithPrincipal<P> {
    pub fn from_metadata(rpc_name: &'static str, metadata: ProxyRequestMetadata) -> Self {
        Self {
            rpc_name,
            request_id: metadata.request_id,
            remote_addr: metadata.remote_addr,
            local_addr: metadata.local_addr,
            client_id: metadata.client_id,
            language: metadata.language,
            client_version: metadata.client_version,
            namespace: metadata.namespace,
            connection_id: metadata.connection_id,
            deadline_at: metadata.deadline_at,
            received_at: metadata.received_at,
            authenticated_principal: None,
        }
    }

    /// Clones the request metadata while dropping the facade-owned principal proof.
    pub fn without_principal(&self) -> ProxyContext {
        ProxyContextWithPrincipal {
            request_id: self.request_id.clone(),
            rpc_name: self.rpc_name,
            remote_addr: self.remote_addr.clone(),
            local_addr: self.local_addr.clone(),
            client_id: self.client_id.clone(),
            language: self.language.clone(),
            client_version: self.client_version.clone(),
            namespace: self.namespace.clone(),
            connection_id: self.connection_id.clone(),
            deadline_at: self.deadline_at,
            received_at: self.received_at,
            authenticated_principal: None,
        }
    }

    /// Attaches a principal proof already validated by the owning facade.
    ///
    /// This method stores `P` without validation; the caller is responsible for preserving the
    /// trust invariant documented on [`ProxyContextWithPrincipal`].
    #[doc(hidden)]
    pub fn set_authenticated_principal(&mut self, principal: P) {
        self.authenticated_principal = Some(principal);
    }

    pub fn for_internal_client(rpc_name: &'static str, client_id: impl Into<String>) -> Self {
        Self {
            request_id: Uuid::new_v4().to_string(),
            rpc_name,
            remote_addr: None,
            local_addr: None,
            client_id: Some(client_id.into()),
            language: None,
            client_version: None,
            namespace: None,
            connection_id: None,
            deadline_at: None,
            received_at: Instant::now(),
            authenticated_principal: None,
        }
    }

    pub fn require_client_id(&self) -> ProxyResult<&str> {
        self.client_id.as_deref().ok_or(ProxyError::ClientIdRequired)
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn rpc_name(&self) -> &'static str {
        self.rpc_name
    }

    pub fn remote_addr(&self) -> Option<&str> {
        self.remote_addr.as_deref()
    }

    pub fn local_addr(&self) -> Option<&str> {
        self.local_addr.as_deref()
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    pub fn language(&self) -> Option<&str> {
        self.language.as_deref()
    }

    pub fn client_version(&self) -> Option<&str> {
        self.client_version.as_deref()
    }

    pub fn connection_id(&self) -> Option<&str> {
        self.connection_id.as_deref()
    }

    pub fn deadline(&self) -> Option<Duration> {
        self.deadline_at
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// Returns the immutable request deadline captured at ingress.
    pub fn deadline_at(&self) -> Option<Instant> {
        self.deadline_at
    }

    pub fn received_at(&self) -> Instant {
        self.received_at
    }

    pub fn authenticated_principal(&self) -> Option<&P> {
        self.authenticated_principal.as_ref()
    }
}
