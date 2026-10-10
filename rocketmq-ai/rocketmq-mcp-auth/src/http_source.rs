// Copyright 2026 The RocketMQ Rust Authors
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

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::path::Path;
use std::sync::Arc;

use crate::policy::JwksPolicy;
use crate::policy::OutboundAddressPolicy;
use crate::verifier::JwksSource;

/// Fetches a JWKS document over HTTPS without following redirects.
#[derive(Clone)]
pub struct HttpJwksSource {
    client: reqwest::Client,
    url: Arc<str>,
    max_jwks_bytes: usize,
}

/// Why an [`HttpJwksSource`] could not be created.
#[derive(Debug, thiserror::Error)]
pub enum HttpJwksSourceError {
    /// The URL is not absolute or names no host.
    #[error("JWKS URL is not an absolute URL with a host")]
    InvalidUrl,
    /// The URL names an address literal that the outbound policy does not allow.
    #[error("JWKS URL names an address that the outbound policy does not allow")]
    AddressNotAllowed,
    /// The CA bundle file could not be read.
    #[error("JWKS CA bundle could not be read")]
    CaUnreadable(#[source] std::io::Error),
    /// The CA bundle file is not a PEM certificate bundle.
    #[error("JWKS CA bundle is not a PEM certificate bundle")]
    CaMalformed(#[source] reqwest::Error),
    /// The CA bundle file holds no certificate.
    #[error("JWKS CA bundle holds no certificate")]
    CaEmpty,
    /// The HTTP client could not be built.
    #[error("JWKS HTTP client could not be built")]
    Client(#[source] reqwest::Error),
}

/// Why an [`HttpJwksSource`] returned no document.
///
/// `Display` names no endpoint. The request error kept as a source has its URL removed, but its own
/// sources may still describe the connection.
#[derive(Debug, thiserror::Error)]
pub enum JwksFetchError {
    /// The request failed, or the endpoint answered with an error status.
    #[error("JWKS request failed")]
    Request(#[source] reqwest::Error),
    /// The response is larger than [`JwksPolicy::max_jwks_bytes`].
    #[error("JWKS response exceeds the size limit")]
    DocumentTooLarge,
}

impl JwksFetchError {
    fn request(error: reqwest::Error) -> Self {
        Self::Request(error.without_url())
    }
}

impl HttpJwksSource {
    /// Creates a source for `url`, trusting the certificates of `ca_path` in addition to the built-in roots.
    ///
    /// # Errors
    ///
    /// Returns an [`HttpJwksSourceError`] when the URL has no host or names an address literal that
    /// `outbound` does not allow, when the CA bundle is unreadable or holds no PEM certificate, or
    /// when the HTTP client cannot be built.
    pub fn new(
        url: impl Into<Arc<str>>,
        ca_path: Option<&Path>,
        policy: &JwksPolicy,
        outbound: OutboundAddressPolicy,
    ) -> Result<Self, HttpJwksSourceError> {
        let url = url.into();
        let parsed = url::Url::parse(&url).map_err(|_| HttpJwksSourceError::InvalidUrl)?;
        let host = parsed.host().ok_or(HttpJwksSourceError::InvalidUrl)?;
        let mut builder = base_client(policy);
        if outbound == OutboundAddressPolicy::PublicOnly {
            builder = match host {
                url::Host::Domain(host) => builder.dns_resolver(Arc::new(SafeDnsResolver::new(host))),
                // An address literal is never resolved, so it is judged here.
                url::Host::Ipv4(address) if is_public_ipv4(address) => builder,
                url::Host::Ipv6(address) if is_public_ipv6(address) => builder,
                url::Host::Ipv4(_) | url::Host::Ipv6(_) => return Err(HttpJwksSourceError::AddressNotAllowed),
            }
            .no_proxy();
        }
        if let Some(path) = ca_path {
            let pem = std::fs::read(path).map_err(HttpJwksSourceError::CaUnreadable)?;
            let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(HttpJwksSourceError::CaMalformed)?;
            if certificates.is_empty() {
                return Err(HttpJwksSourceError::CaEmpty);
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        let client = builder.build().map_err(HttpJwksSourceError::Client)?;
        Ok(Self {
            client,
            url,
            max_jwks_bytes: policy.max_jwks_bytes,
        })
    }
}

fn base_client(policy: &JwksPolicy) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(policy.fetch_timeout)
}

#[derive(Clone)]
struct SafeDnsResolver {
    allowed_host: Arc<str>,
}

impl SafeDnsResolver {
    fn new(host: &str) -> Self {
        Self {
            allowed_host: Arc::from(host.to_ascii_lowercase()),
        }
    }
}

impl reqwest::dns::Resolve for SafeDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let expected = self.allowed_host.clone();
        Box::pin(async move {
            if !name.as_str().eq_ignore_ascii_case(&expected) {
                return Err(Box::new(DnsPolicyError) as Box<dyn std::error::Error + Send + Sync>);
            }
            let addresses = tokio::net::lookup_host((name.as_str(), 0))
                .await
                .map_err(|_| Box::new(DnsPolicyError) as Box<dyn std::error::Error + Send + Sync>)?
                .collect::<Vec<_>>();
            validated_public_addresses(addresses)
                .map(|addresses| Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct DnsPolicyError;

impl std::fmt::Display for DnsPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DNS policy rejected the endpoint")
    }
}

impl std::error::Error for DnsPolicyError {}

impl JwksSource for HttpJwksSource {
    type Error = JwksFetchError;

    async fn fetch(&self) -> Result<Vec<u8>, JwksFetchError> {
        let mut response = self
            .client
            .get(self.url.as_ref())
            .send()
            .await
            .map_err(JwksFetchError::request)?
            .error_for_status()
            .map_err(JwksFetchError::request)?;
        if response
            .content_length()
            .is_some_and(|length| length > self.max_jwks_bytes as u64)
        {
            return Err(JwksFetchError::DocumentTooLarge);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(JwksFetchError::request)? {
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or(JwksFetchError::DocumentTooLarge)?;
            if next_len > self.max_jwks_bytes {
                return Err(JwksFetchError::DocumentTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

fn validated_public_addresses(
    addresses: Vec<std::net::SocketAddr>,
) -> Result<Vec<std::net::SocketAddr>, DnsPolicyError> {
    if addresses.is_empty() || addresses.iter().any(|address| !is_public_ip(address.ip())) {
        return Err(DnsPolicyError);
    }
    Ok(addresses)
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, d] = address.octets();
    if a == 192 && b == 0 && c == 0 {
        return matches!(d, 9 | 10);
    }
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 88 && c == 99)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if address.to_ipv4_mapped().is_some() {
        return false;
    }
    // IANA currently allocates global unicast from 2000::/3. Conservatively
    // reject all other scopes and every special or transition block inside it.
    let segments = address.segments();
    let global_unicast = (segments[0] & 0xe000) == 0x2000;
    let ietf_special = segments[0] == 0x2001 && segments[1] <= 0x01ff;
    let documentation =
        (segments[0] == 0x2001 && segments[1] == 0x0db8) || (segments[0] == 0x3fff && (segments[1] & 0xf000) == 0);
    let six_to_four = segments[0] == 0x2002;
    global_unicast && !ietf_special && !documentation && !six_to_four
}

#[cfg(test)]
mod tests {
    use reqwest::dns::Resolve;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::test_support::policy;

    #[test]
    fn http_source_accepts_only_readable_pem_ca_bundles() {
        let temp_dir = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, .. } =
            rcgen::generate_simple_self_signed(vec!["issuer.example.test".to_string()]).unwrap();
        let ca_path = temp_dir.path().join("issuer-ca.pem");
        std::fs::write(&ca_path, cert.pem()).unwrap();
        let url = "https://issuer.example.test/jwks";

        for outbound in [OutboundAddressPolicy::PublicOnly, OutboundAddressPolicy::Unrestricted] {
            assert!(HttpJwksSource::new(url, None, &policy(), outbound).is_ok());
            assert!(HttpJwksSource::new(url, Some(&ca_path), &policy(), outbound).is_ok());
        }

        let invalid_path = temp_dir.path().join("invalid-ca.pem");
        std::fs::write(&invalid_path, b"not a certificate").unwrap();
        let missing_path = temp_dir.path().join("missing-ca.pem");
        let outbound = OutboundAddressPolicy::PublicOnly;
        assert!(matches!(
            HttpJwksSource::new(url, Some(&invalid_path), &policy(), outbound),
            Err(HttpJwksSourceError::CaMalformed(_) | HttpJwksSourceError::CaEmpty)
        ));
        assert!(matches!(
            HttpJwksSource::new(url, Some(&missing_path), &policy(), outbound),
            Err(HttpJwksSourceError::CaUnreadable(_))
        ));
    }

    #[test]
    fn url_must_name_a_host_and_address_literals_follow_the_outbound_policy() {
        for url in ["issuer.example.test/jwks", "/jwks", "mailto:keys@example.test"] {
            for outbound in [OutboundAddressPolicy::PublicOnly, OutboundAddressPolicy::Unrestricted] {
                assert!(
                    matches!(
                        HttpJwksSource::new(url, None, &policy(), outbound),
                        Err(HttpJwksSourceError::InvalidUrl)
                    ),
                    "accepted {url}"
                );
            }
        }
        for url in [
            "https://127.0.0.1/jwks",
            "https://10.0.0.9/jwks",
            "https://169.254.169.254/jwks",
            "https://[::1]/jwks",
            "https://[fd00::1]/jwks",
            "https://[::ffff:8.8.8.8]/jwks",
        ] {
            assert!(
                matches!(
                    HttpJwksSource::new(url, None, &policy(), OutboundAddressPolicy::PublicOnly),
                    Err(HttpJwksSourceError::AddressNotAllowed)
                ),
                "accepted {url}"
            );
            assert!(HttpJwksSource::new(url, None, &policy(), OutboundAddressPolicy::Unrestricted).is_ok());
        }
        for url in ["https://8.8.8.8/jwks", "https://[2606:4700:4700::1111]/jwks"] {
            assert!(HttpJwksSource::new(url, None, &policy(), OutboundAddressPolicy::PublicOnly).is_ok());
        }
    }

    #[tokio::test]
    async fn resolver_answers_only_for_the_jwks_host_and_only_with_public_addresses() {
        // Another name is refused before any lookup.
        let resolver = SafeDnsResolver::new("issuer.example.test");
        let refused = resolver.resolve("other.example.test".parse().unwrap()).await;
        assert!(refused.is_err_and(|error| error.is::<DnsPolicyError>()));

        // The configured name is looked up, and a loopback answer is refused.
        let resolver = SafeDnsResolver::new("LOCALHOST");
        let refused = resolver.resolve("localhost".parse().unwrap()).await;
        assert!(refused.is_err_and(|error| error.is::<DnsPolicyError>()));
    }

    #[test]
    fn connect_time_dns_policy_rejects_non_public_answers() {
        for address in [
            "0.0.0.1:443",
            "127.0.0.1:443",
            "10.0.0.1:443",
            "169.254.1.1:443",
            "100.64.0.1:443",
            "192.0.0.8:443",
            "192.0.0.170:443",
            "192.0.2.1:443",
            "192.88.99.2:443",
            "192.168.1.1:443",
            "198.18.0.1:443",
            "198.51.100.1:443",
            "203.0.113.1:443",
            "240.0.0.1:443",
            "[::1]:443",
            "[fe80::1]:443",
            "[fd00::1]:443",
        ] {
            assert!(validated_public_addresses(vec![address.parse().unwrap()]).is_err());
        }
        for address in [
            "8.8.8.8:443",
            "192.0.0.9:443",
            "192.0.0.10:443",
            "192.31.196.1:443",
            "192.52.193.1:443",
            "192.175.48.1:443",
        ] {
            assert!(
                validated_public_addresses(vec![address.parse().unwrap()]).is_ok(),
                "rejected {address}"
            );
        }
        for address in [
            "[::]:443",
            "[::127.0.0.1]:443",
            "[::ffff:127.0.0.1]:443",
            "[::ffff:8.8.8.8]:443",
            "[64:ff9b::7f00:1]:443",
            "[64:ff9b:1::7f00:1]:443",
            "[2002:7f00:1::]:443",
            "[2001:0:4136:e378:8000:63bf:3fff:fdd2]:443",
            "[2001:2::1]:443",
            "[2001:10::1]:443",
            "[2001:20::1]:443",
            "[2001:db8::1]:443",
            "[3fff:0fff::1]:443",
            "[ff02::1]:443",
        ] {
            assert!(
                validated_public_addresses(vec![address.parse().unwrap()]).is_err(),
                "accepted {address}"
            );
        }
        for address in [
            "[2606:4700:4700::1111]:443",
            "[2001:4860:4860::8888]:443",
            "[3ff1::1]:443",
            "[3fff:1000::1]:443",
        ] {
            assert!(
                validated_public_addresses(vec![address.parse().unwrap()]).is_ok(),
                "rejected {address}"
            );
        }
        assert!(validated_public_addresses(vec![
            "192.31.196.1:443".parse().unwrap(),
            "127.0.0.1:443".parse().unwrap(),
        ])
        .is_err());
        assert!(validated_public_addresses(Vec::new()).is_err());
    }

    /// Answers one request on loopback with `response` and returns what the source made of it.
    ///
    /// The client is the production one except that it speaks plain HTTP, so that no certificate
    /// is needed, and ignores proxy settings of the machine running the test.
    async fn fetch_answered_with(response: &[u8], max_jwks_bytes: usize) -> Result<Vec<u8>, JwksFetchError> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let source = HttpJwksSource {
            client: base_client(&policy()).https_only(false).no_proxy().build().unwrap(),
            url: Arc::from(format!("http://127.0.0.1:{port}/jwks")),
            max_jwks_bytes,
        };
        let answer = async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer).await.unwrap();
                assert_ne!(read, 0, "request ended before its head was complete");
                request.extend_from_slice(&buffer[..read]);
            }
            assert!(request.starts_with(b"GET /jwks HTTP/1.1\r\n"));
            // The source may hang up as soon as it has seen enough, so a failed write is expected.
            let _ = stream.write_all(response).await;
            let _ = stream.shutdown().await;
            // Closing the listener makes a second request fail instead of wait.
            drop(listener);
        };
        let (fetched, ()) = tokio::join!(source.fetch(), answer);
        fetched
    }

    fn response(head: &str, body: &[u8]) -> Vec<u8> {
        let mut response = format!("HTTP/1.1 {head}\r\nConnection: close\r\n\r\n").into_bytes();
        response.extend_from_slice(body);
        response
    }

    #[tokio::test]
    async fn fetch_returns_the_body_up_to_the_size_limit() {
        let body = [b'k'; 64];
        let sized = response("200 OK\r\nContent-Length: 64", &body);
        assert_eq!(fetch_answered_with(&sized, 64).await.unwrap(), body);
        assert!(matches!(
            fetch_answered_with(&sized, 63).await,
            Err(JwksFetchError::DocumentTooLarge)
        ));

        // Without a declared length the limit is applied to what actually arrives.
        let mut chunked = b"28\r\n".to_vec();
        chunked.extend_from_slice(&body[..40]);
        chunked.extend_from_slice(b"\r\n18\r\n");
        chunked.extend_from_slice(&body[40..]);
        chunked.extend_from_slice(b"\r\n0\r\n\r\n");
        let streamed = response("200 OK\r\nTransfer-Encoding: chunked", &chunked);
        assert_eq!(fetch_answered_with(&streamed, 64).await.unwrap(), body);
        assert!(matches!(
            fetch_answered_with(&streamed, 63).await,
            Err(JwksFetchError::DocumentTooLarge)
        ));
    }

    #[tokio::test]
    async fn fetch_refuses_error_statuses_and_does_not_follow_redirects() {
        for head in [
            "404 Not Found\r\nContent-Length: 0",
            "503 Service Unavailable\r\nContent-Length: 0",
        ] {
            let failed = fetch_answered_with(&response(head, b""), 64).await.unwrap_err();
            assert!(matches!(failed, JwksFetchError::Request(_)));
            // Neither the message nor the kept request error names the endpoint.
            assert_eq!(failed.to_string(), "JWKS request failed");
            assert!(!format!("{failed:?}").contains("127.0.0.1"));
        }

        // The redirect is returned as it is: following it would hit the closed listener and fail.
        let redirect = response("302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0", b"");
        assert_eq!(fetch_answered_with(&redirect, 64).await.unwrap(), b"");
    }
}
