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

use std::time::Duration;
use std::time::Instant;

use tonic::metadata::MetadataMap;
use tonic::Request;
use uuid::Uuid;

use crate::auth::AuthenticatedPrincipal;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::middleware;

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

#[derive(Debug, Clone)]
pub struct ProxyContext {
    request_id: String,
    rpc_name: &'static str,
    remote_addr: Option<String>,
    local_addr: Option<String>,
    client_id: Option<String>,
    language: Option<String>,
    client_version: Option<String>,
    namespace: Option<String>,
    connection_id: Option<String>,
    deadline: Option<Duration>,
    received_at: Instant,
    authenticated_principal: Option<AuthenticatedPrincipal>,
}

impl ProxyContext {
    pub fn from_grpc_request<T>(rpc_name: &'static str, request: &Request<T>) -> ProxyResult<Self> {
        let metadata = request.metadata();
        let deadline = parse_grpc_timeout_metadata(metadata)?;

        Ok(Self {
            request_id: Uuid::new_v4().to_string(),
            rpc_name,
            remote_addr: request.remote_addr().map(|addr| addr.to_string()),
            local_addr: middleware::request_local_addr(request).map(str::to_owned),
            client_id: metadata_string(metadata, "x-mq-client-id"),
            language: metadata_string(metadata, "x-mq-language"),
            client_version: metadata_string(metadata, "x-mq-client-version"),
            namespace: metadata_string(metadata, "x-mq-namespace"),
            connection_id: metadata_string(metadata, "x-mq-channel-id"),
            deadline,
            received_at: Instant::now(),
            authenticated_principal: None,
        })
    }

    pub(crate) fn set_authenticated_principal(&mut self, principal: AuthenticatedPrincipal) {
        self.authenticated_principal = Some(principal);
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
        self.deadline
    }

    pub fn received_at(&self) -> Instant {
        self.received_at
    }

    pub fn authenticated_principal(&self) -> Option<&AuthenticatedPrincipal> {
        self.authenticated_principal.as_ref()
    }
}

fn metadata_string(metadata: &MetadataMap, key: &'static str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn parse_grpc_timeout_metadata(metadata: &MetadataMap) -> ProxyResult<Option<Duration>> {
    let Some(raw) = metadata.get("grpc-timeout") else {
        return Ok(None);
    };
    let raw = raw
        .to_str()
        .map_err(|_| ProxyError::invalid_metadata("grpc-timeout must be valid ASCII"))?;
    parse_grpc_timeout(raw)
        .ok_or_else(|| {
            ProxyError::invalid_metadata(format!(
                "grpc-timeout '{raw}' must use the gRPC timeout format <digits><H|M|S|m|u|n>",
            ))
        })
        .map(Some)
}

pub(crate) fn parse_grpc_timeout(raw: &str) -> Option<Duration> {
    if raw.len() < 2 {
        return None;
    }

    let (value, unit) = raw.split_at(raw.len() - 1);
    let value = value.parse::<u64>().ok()?;

    match unit {
        "H" => Some(Duration::from_secs(value.saturating_mul(60).saturating_mul(60))),
        "M" => Some(Duration::from_secs(value.saturating_mul(60))),
        "S" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_millis(value)),
        "u" => Some(Duration::from_micros(value)),
        "n" => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Request;

    use super::parse_grpc_timeout;
    use super::ProxyContext;
    use crate::error::ProxyError;
    use crate::grpc::middleware::GrpcTransportContext;

    #[test]
    fn parse_grpc_timeout_supports_multiple_units() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("25m"), Some(Duration::from_millis(25)));
        assert_eq!(parse_grpc_timeout("100u"), Some(Duration::from_micros(100)));
    }

    #[test]
    fn parse_grpc_timeout_rejects_invalid_values() {
        assert_eq!(parse_grpc_timeout(""), None);
        assert_eq!(parse_grpc_timeout("abc"), None);
        assert_eq!(parse_grpc_timeout("12X"), None);
    }

    #[test]
    fn proxy_context_reads_local_addr_from_transport_context() {
        let mut request = Request::new(());
        request.extensions_mut().insert(GrpcTransportContext::new(
            "127.0.0.1:8080".parse().expect("socket addr"),
        ));
        request
            .metadata_mut()
            .insert("x-mq-client-id", "client-a".parse().expect("client id metadata"));

        let context = ProxyContext::from_grpc_request("QueryRoute", &request).expect("context should be constructed");
        assert_eq!(context.local_addr(), Some("127.0.0.1:8080"));
        assert_eq!(context.client_id(), Some("client-a"));
    }

    #[test]
    fn proxy_context_rejects_invalid_grpc_timeout_metadata() {
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("grpc-timeout", "bad-timeout".parse().expect("timeout metadata"));

        let error = ProxyContext::from_grpc_request("QueryRoute", &request).expect_err("context should reject timeout");
        assert!(matches!(error, ProxyError::InvalidMetadata { .. }));
        assert!(error.to_string().contains("grpc-timeout"));
    }
}
