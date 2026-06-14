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

#[cfg(feature = "tls")]
use std::fs;
#[cfg(feature = "tls")]
use std::net::IpAddr;
use std::net::SocketAddr;
#[cfg(feature = "tls")]
use std::sync::Arc as StdArc;
use std::time::Duration;
#[cfg(feature = "tls")]
use std::time::SystemTime;

#[cfg(feature = "tls")]
use rocketmq_common::common::tls_config::TlsClientAuth;
pub use rocketmq_common::common::tls_config::TlsConfig;
pub use rocketmq_common::common::tls_config::TlsMode;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio::time;
#[cfg(feature = "tls")]
use tracing::debug;
use tracing::warn;

use crate::connection::Connection;

const TLS_HANDSHAKE_MAGIC_CODE: u8 = 0x16;
const TLS_RELOAD_POLL_INTERVAL: Duration = Duration::from_secs(5);
pub const TLS_DISABLED_ERROR_REASON: &str = "rocketmq-remoting was compiled without the tls feature";

#[cfg(feature = "tls")]
type TlsAcceptorSlot = arc_swap::ArcSwapOption<tokio_rustls::TlsAcceptor>;

#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct TlsServerRuntime {
    mode: TlsMode,
    acceptor: StdArc<TlsAcceptorSlot>,
    base_config: StdArc<TlsConfig>,
}

#[cfg(not(feature = "tls"))]
#[derive(Clone)]
pub struct TlsServerRuntime {
    mode: TlsMode,
}

impl TlsServerRuntime {
    pub fn new(base_config: TlsConfig) -> Self {
        #[cfg(feature = "tls")]
        {
            let effective_config = effective_tls_config(&base_config);
            let acceptor = StdArc::new(TlsAcceptorSlot::empty());
            let mode = base_config.server.mode;

            if mode != TlsMode::Disabled {
                match build_server_acceptor(&effective_config) {
                    Ok(tls_acceptor) => acceptor.store(Some(StdArc::new(tls_acceptor))),
                    Err(error) => {
                        warn!("failed to build initial TLS server acceptor: {error}");
                    }
                }
            }

            let runtime = Self {
                mode,
                acceptor,
                base_config: StdArc::new(base_config),
            };
            runtime.spawn_reload_task();
            runtime
        }

        #[cfg(not(feature = "tls"))]
        {
            Self {
                mode: base_config.server.mode,
            }
        }
    }

    pub async fn into_connection(&self, stream: TcpStream, remote_addr: SocketAddr) -> Option<Connection> {
        let is_tls_handshake = match peek_tls_handshake(&stream).await {
            Ok(value) => value,
            Err(error) => {
                warn!("failed to inspect TLS handshake byte from {remote_addr}: {error}");
                return None;
            }
        };

        match self.mode {
            TlsMode::Disabled => {
                if is_tls_handshake {
                    warn!("client {remote_addr} attempted TLS while server TLS mode is disabled");
                    None
                } else {
                    Some(Connection::new(stream))
                }
            }
            TlsMode::Permissive => {
                if is_tls_handshake {
                    self.accept_tls(stream, remote_addr).await
                } else {
                    Some(Connection::new(stream))
                }
            }
            TlsMode::Enforcing => {
                if is_tls_handshake {
                    self.accept_tls(stream, remote_addr).await
                } else {
                    warn!("client {remote_addr} attempted plaintext while server TLS mode is enforcing");
                    None
                }
            }
        }
    }

    #[cfg(feature = "tls")]
    async fn accept_tls(&self, stream: TcpStream, remote_addr: SocketAddr) -> Option<Connection> {
        let Some(acceptor) = self.acceptor.load_full() else {
            warn!("client {remote_addr} attempted TLS but no TLS server acceptor is configured");
            return None;
        };

        match acceptor.accept(stream).await {
            Ok(tls_stream) => Some(Connection::new_with_stream(tls_stream)),
            Err(error) => {
                warn!("TLS handshake from {remote_addr} failed: {error}");
                None
            }
        }
    }

    #[cfg(not(feature = "tls"))]
    async fn accept_tls(&self, _stream: TcpStream, remote_addr: SocketAddr) -> Option<Connection> {
        warn!("client {remote_addr} attempted TLS but rocketmq-remoting was compiled without TLS support");
        None
    }

    #[cfg(feature = "tls")]
    fn spawn_reload_task(&self) {
        if self.mode == TlsMode::Disabled {
            return;
        }

        let base_config = self.base_config.clone();
        let acceptor = self.acceptor.clone();
        tokio::spawn(async move {
            let mut previous_snapshot = file_snapshot(&effective_tls_config(&base_config).watched_server_paths());
            loop {
                time::sleep(TLS_RELOAD_POLL_INTERVAL).await;

                let effective_config = effective_tls_config(&base_config);
                let paths = effective_config.watched_server_paths();
                let current_snapshot = file_snapshot(&paths);
                if current_snapshot == previous_snapshot {
                    continue;
                }

                previous_snapshot = current_snapshot;
                match build_server_acceptor(&effective_config) {
                    Ok(tls_acceptor) => {
                        acceptor.store(Some(StdArc::new(tls_acceptor)));
                        debug!("TLS server acceptor reloaded after file change");
                    }
                    Err(error) => {
                        warn!("failed to reload TLS server acceptor; keeping previous acceptor: {error}");
                    }
                }
            }
        });
    }
}

#[cfg(feature = "tls")]
pub async fn connect_tls_stream(
    stream: TcpStream,
    server_name: &str,
    tls_config: &TlsConfig,
) -> RocketMQResult<tokio_rustls::client::TlsStream<TcpStream>> {
    let config = build_client_config(tls_config)?;
    let connector = tokio_rustls::TlsConnector::from(StdArc::new(config));
    connector
        .connect(parse_server_name(server_name)?, stream)
        .await
        .map_err(|error| {
            RocketMQError::network_connection_failed(server_name, format!("TLS handshake failed: {error}"))
        })
}

#[cfg(not(feature = "tls"))]
pub async fn connect_tls_stream(
    _stream: TcpStream,
    _server_name: &str,
    _tls_config: &TlsConfig,
) -> RocketMQResult<TcpStream> {
    Err(tls_disabled_error())
}

#[cfg(feature = "tls")]
pub fn build_client_config(tls_config: &TlsConfig) -> RocketMQResult<tokio_rustls::rustls::ClientConfig> {
    use tokio_rustls::rustls::client::danger::HandshakeSignatureValid;
    use tokio_rustls::rustls::client::danger::ServerCertVerified;
    use tokio_rustls::rustls::client::danger::ServerCertVerifier;
    use tokio_rustls::rustls::pki_types::CertificateDer;
    use tokio_rustls::rustls::pki_types::UnixTime;
    use tokio_rustls::rustls::DigitallySignedStruct;
    use tokio_rustls::rustls::Error;
    use tokio_rustls::rustls::SignatureScheme;

    #[derive(Debug)]
    struct NoCertificateVerification;

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ED25519,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
            ]
        }
    }

    let effective_config = effective_tls_config(tls_config);
    let protocol_versions = configured_protocol_versions(&effective_config)?;
    let client_builder = match protocol_versions.as_deref() {
        Some(versions) => tokio_rustls::rustls::ClientConfig::builder_with_protocol_versions(versions),
        None => tokio_rustls::rustls::ClientConfig::builder(),
    };
    let builder = if effective_config.test_mode_enable || !effective_config.client.auth_server {
        client_builder
            .dangerous()
            .with_custom_certificate_verifier(StdArc::new(NoCertificateVerification))
    } else {
        client_builder.with_root_certificates(load_client_root_store(
            effective_config.client.trust_cert_path.as_deref(),
        )?)
    };

    if effective_config.client.key_password.is_some() {
        return Err(config_error(
            "tls.client.keyPassword",
            "<redacted>",
            "encrypted private keys are not supported by rocketmq-rust TLS v1",
        ));
    }

    match (
        effective_config.client.cert_path.as_deref(),
        effective_config.client.key_path.as_deref(),
    ) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_certificates(cert_path, "tls.client.certPath")?;
            let key = load_private_key(key_path, "tls.client.keyPath")?;
            builder.with_client_auth_cert(certs, key).map_err(|error| {
                config_error(
                    "tls.client.certificate",
                    cert_path,
                    format!("failed to configure client certificate: {error}"),
                )
            })
        }
        (None, None) => Ok(builder.with_no_client_auth()),
        _ => Err(config_error(
            "tls.client.certificate",
            "<partial>",
            "tls.client.certPath and tls.client.keyPath must be configured together",
        )),
    }
}

#[cfg(feature = "tls")]
pub fn build_server_acceptor(tls_config: &TlsConfig) -> RocketMQResult<tokio_rustls::TlsAcceptor> {
    let effective_config = effective_tls_config(tls_config);

    if effective_config.server.key_password.is_some() {
        return Err(config_error(
            "tls.server.keyPassword",
            "<redacted>",
            "encrypted private keys are not supported by rocketmq-rust TLS v1",
        ));
    }

    let (certs, key) = if effective_config.test_mode_enable
        && (effective_config.server.cert_path.is_none() || effective_config.server.key_path.is_none())
    {
        generate_self_signed_certificate()?
    } else {
        let cert_path = effective_config.server.cert_path.as_deref().ok_or_else(|| {
            config_error(
                "tls.server.certPath",
                "",
                "server certificate path is required when TLS test mode is disabled",
            )
        })?;
        let key_path = effective_config.server.key_path.as_deref().ok_or_else(|| {
            config_error(
                "tls.server.keyPath",
                "",
                "server private key path is required when TLS test mode is disabled",
            )
        })?;
        (
            load_certificates(cert_path, "tls.server.certPath")?,
            load_private_key(key_path, "tls.server.keyPath")?,
        )
    };

    let verifier = build_client_cert_verifier(&effective_config)?;
    let protocol_versions = configured_protocol_versions(&effective_config)?;
    let server_builder = match protocol_versions.as_deref() {
        Some(versions) => tokio_rustls::rustls::ServerConfig::builder_with_protocol_versions(versions),
        None => tokio_rustls::rustls::ServerConfig::builder(),
    };
    let server_config = server_builder
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|error| config_error("tls.server.certificate", "<configured>", error.to_string()))?;

    Ok(tokio_rustls::TlsAcceptor::from(StdArc::new(server_config)))
}

#[cfg(feature = "tls")]
fn build_client_cert_verifier(
    tls_config: &TlsConfig,
) -> RocketMQResult<StdArc<dyn tokio_rustls::rustls::server::danger::ClientCertVerifier>> {
    use tokio_rustls::rustls::server::WebPkiClientVerifier;

    match tls_config.server.need_client_auth {
        TlsClientAuth::None => Ok(WebPkiClientVerifier::no_client_auth()),
        TlsClientAuth::Optional | TlsClientAuth::Require => {
            if !tls_config.server.auth_client {
                return Err(config_error(
                    "tls.server.authClient",
                    "false",
                    "client certificate verification requires tls.server.authClient=true",
                ));
            }

            let trust_path = tls_config.server.trust_cert_path.as_deref().ok_or_else(|| {
                config_error(
                    "tls.server.trustCertPath",
                    "",
                    "server trust certificate path is required for client certificate authentication",
                )
            })?;
            let root_store = StdArc::new(load_root_store_from_pem(trust_path, "tls.server.trustCertPath")?);
            let builder = WebPkiClientVerifier::builder(root_store);
            let builder = if tls_config.server.need_client_auth == TlsClientAuth::Optional {
                builder.allow_unauthenticated()
            } else {
                builder
            };
            builder.build().map(|verifier| verifier as StdArc<_>).map_err(|error| {
                config_error(
                    "tls.server.trustCertPath",
                    trust_path,
                    format!("failed to build client certificate verifier: {error}"),
                )
            })
        }
    }
}

#[cfg(feature = "tls")]
fn generate_self_signed_certificate() -> RocketMQResult<(
    Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
)> {
    use tokio_rustls::rustls::pki_types::PrivateKeyDer;
    use tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer;

    let rcgen::CertifiedKey { cert, key_pair } = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|error| {
            config_error(
                "tls.test.mode.enable",
                "true",
                format!("failed to generate self-signed test certificate: {error}"),
            )
        })?;
    let certs = vec![cert.der().clone()];
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));
    Ok((certs, key))
}

#[cfg(feature = "tls")]
fn load_client_root_store(trust_cert_path: Option<&str>) -> RocketMQResult<tokio_rustls::rustls::RootCertStore> {
    match trust_cert_path {
        Some(path) => load_root_store_from_pem(path, "tls.client.trustCertPath"),
        None => load_native_root_store(),
    }
}

#[cfg(feature = "tls")]
fn load_native_root_store() -> RocketMQResult<tokio_rustls::rustls::RootCertStore> {
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    let cert_result = rustls_native_certs::load_native_certs();
    let mut added_roots = 0usize;

    for cert in cert_result.certs {
        root_store
            .add(cert)
            .map_err(|error| config_error("tls.root_certificates", "native-certs", error.to_string()))?;
        added_roots += 1;
    }

    for error in cert_result.errors {
        warn!("failed to load a native TLS root certificate: {error}");
    }

    if added_roots == 0 {
        return Err(config_error(
            "tls.root_certificates",
            "native-certs",
            "no native root certificates were loaded",
        ));
    }

    Ok(root_store)
}

#[cfg(feature = "tls")]
fn load_root_store_from_pem(path: &str, key: &'static str) -> RocketMQResult<tokio_rustls::rustls::RootCertStore> {
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    for cert in load_certificates(path, key)? {
        root_store
            .add(cert)
            .map_err(|error| config_error(key, path, format!("failed to add root certificate: {error}")))?;
    }

    if root_store.is_empty() {
        return Err(config_error(key, path, "no PEM certificates were loaded"));
    }

    Ok(root_store)
}

#[cfg(feature = "tls")]
pub fn load_certificates(
    path: &str,
    key: &'static str,
) -> RocketMQResult<Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>> {
    let file = fs::File::open(path)
        .map_err(|error| config_error(key, path, format!("failed to open certificate file: {error}")))?;
    let mut reader = std::io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| config_error(key, path, format!("failed to read PEM certificates: {error}")))?;

    if certs.is_empty() {
        return Err(config_error(key, path, "no PEM certificates were found"));
    }

    Ok(certs)
}

#[cfg(feature = "tls")]
fn load_private_key(
    path: &str,
    key: &'static str,
) -> RocketMQResult<tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>> {
    let file = fs::File::open(path)
        .map_err(|error| config_error(key, path, format!("failed to open private key file: {error}")))?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|error| config_error(key, path, format!("failed to read PEM private key: {error}")))?
        .ok_or_else(|| config_error(key, path, "no supported PEM private key was found"))
}

#[cfg(feature = "tls")]
fn parse_server_name(server_name: &str) -> RocketMQResult<tokio_rustls::rustls::pki_types::ServerName<'static>> {
    let value = server_name.trim_matches(['[', ']']);
    if let Ok(ip_addr) = value.parse::<IpAddr>() {
        return Ok(tokio_rustls::rustls::pki_types::ServerName::IpAddress(ip_addr.into()));
    }

    tokio_rustls::rustls::pki_types::ServerName::try_from(value.to_string()).map_err(|error| {
        config_error(
            "tls.server_name",
            server_name,
            format!("invalid TLS server name: {error}"),
        )
    })
}

#[cfg(feature = "tls")]
fn configured_protocol_versions(
    tls_config: &TlsConfig,
) -> RocketMQResult<Option<Vec<&'static tokio_rustls::rustls::SupportedProtocolVersion>>> {
    let Some(protocols) = tls_config.protocols.as_deref() else {
        return Ok(None);
    };

    let mut versions = Vec::new();
    for raw_protocol in protocols.split(',') {
        let protocol = raw_protocol.trim();
        if protocol.is_empty() {
            continue;
        }

        let version = match protocol.to_ascii_lowercase().as_str() {
            "tlsv1.3" | "tls1.3" | "tls13" | "1.3" => &tokio_rustls::rustls::version::TLS13,
            "tlsv1.2" | "tls1.2" | "tls12" | "1.2" => &tokio_rustls::rustls::version::TLS12,
            _ => {
                return Err(config_error(
                    "tls.protocols",
                    protocol,
                    "only TLSv1.3 and TLSv1.2 are supported by rocketmq-rust TLS",
                ));
            }
        };

        if !versions
            .iter()
            .any(|existing: &&tokio_rustls::rustls::SupportedProtocolVersion| existing.version == version.version)
        {
            versions.push(version);
        }
    }

    if versions.is_empty() {
        return Err(config_error(
            "tls.protocols",
            protocols,
            "at least one TLS protocol version must be configured",
        ));
    }

    Ok(Some(versions))
}

#[cfg(feature = "tls")]
fn effective_tls_config(base_config: &TlsConfig) -> TlsConfig {
    let mut effective_config = base_config.clone();
    let enable = effective_config.enable;
    let server_mode = effective_config.server.mode;
    let config_file = effective_config.config_file.clone();

    if let Ok(content) = fs::read_to_string(&config_file) {
        effective_config.apply_java_properties_str(&content);
        effective_config.enable = enable;
        effective_config.server.mode = server_mode;
        effective_config.config_file = config_file;
    }

    effective_config
}

#[cfg(feature = "tls")]
fn file_snapshot(paths: &[String]) -> Vec<(String, Option<SystemTime>, Option<u64>)> {
    paths
        .iter()
        .map(|path| {
            let metadata = fs::metadata(path);
            let modified = metadata.as_ref().ok().and_then(|metadata| metadata.modified().ok());
            let len = metadata.as_ref().ok().map(|metadata| metadata.len());
            (path.clone(), modified, len)
        })
        .collect()
}

async fn peek_tls_handshake(stream: &TcpStream) -> std::io::Result<bool> {
    let mut first_byte = [0u8; 1];
    let read = stream.peek(&mut first_byte).await?;
    Ok(read > 0 && first_byte[0] == TLS_HANDSHAKE_MAGIC_CODE)
}

pub fn tls_disabled_error() -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key: "use_tls",
        value: "true".to_string(),
        reason: TLS_DISABLED_ERROR_REASON.to_string(),
    }
}

fn config_error(key: &'static str, value: impl Into<String>, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::ConfigInvalidValue {
        key,
        value: value.into(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tls")]
    #[test]
    fn tls_config_file_does_not_override_enable_or_server_mode() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let config_file = temp_dir.path().join("tls.properties");
        fs::write(
            &config_file,
            r#"
            tls.enable=false
            tls.server.mode=disabled
            tls.server.certPath=/tmp/server.pem
            tls.client.authServer=false
            "#,
        )
        .expect("write tls config file");
        let base = TlsConfig {
            enable: true,
            config_file: config_file.to_string_lossy().to_string(),
            server: rocketmq_common::common::tls_config::TlsServerConfig {
                mode: TlsMode::Enforcing,
                ..Default::default()
            },
            ..Default::default()
        };

        let effective = effective_tls_config(&base);

        assert!(effective.enable);
        assert_eq!(effective.server.mode, TlsMode::Enforcing);
        assert_eq!(effective.server.cert_path.as_deref(), Some("/tmp/server.pem"));
        assert!(!effective.client.auth_server);
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn tls_disabled_error_mentions_feature() {
        let error = tls_disabled_error();

        assert!(error.to_string().contains("tls feature"));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_mode_builds_self_signed_server_acceptor_without_cert_files() {
        let config = TlsConfig {
            test_mode_enable: true,
            server: rocketmq_common::common::tls_config::TlsServerConfig {
                mode: TlsMode::Enforcing,
                ..Default::default()
            },
            ..Default::default()
        };

        build_server_acceptor(&config).expect("test mode should generate a self-signed certificate");
    }

    #[cfg(feature = "tls")]
    #[test]
    fn non_test_mode_requires_server_certificate_files() {
        let config = TlsConfig {
            server: rocketmq_common::common::tls_config::TlsServerConfig {
                mode: TlsMode::Enforcing,
                ..Default::default()
            },
            ..Default::default()
        };

        let error = match build_server_acceptor(&config) {
            Ok(_) => panic!("missing certs should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("tls.server.certPath"));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn configured_protocol_versions_accept_java_protocol_names() {
        let mut config = TlsConfig {
            protocols: Some("TLSv1.3,TLSv1.2,TLS13".to_string()),
            ..Default::default()
        };

        let versions = configured_protocol_versions(&config)
            .expect("valid protocols should parse")
            .expect("protocols should be configured");

        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, tokio_rustls::rustls::ProtocolVersion::TLSv1_3);
        assert_eq!(versions[1].version, tokio_rustls::rustls::ProtocolVersion::TLSv1_2);

        config.protocols = Some("TLSv1.1".to_string());
        let error = configured_protocol_versions(&config).expect_err("unsupported protocols should fail");
        assert!(error.to_string().contains("tls.protocols"));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn pem_loader_rejects_missing_certificate_file() {
        let error = load_certificates("missing.pem", "tls.server.certPath").expect_err("missing cert path should fail");
        assert!(error.to_string().contains("missing.pem"));
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn tls_modes_gate_plaintext_and_tls_connections() {
        assert!(plaintext_connects(TlsMode::Disabled).await);
        assert!(!tls_connects(TlsMode::Disabled, None).await);
        assert!(plaintext_connects(TlsMode::Permissive).await);
        assert!(tls_connects(TlsMode::Permissive, None).await);
        assert!(!plaintext_connects(TlsMode::Enforcing).await);
        assert!(tls_connects(TlsMode::Enforcing, None).await);
    }

    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn mtls_require_rejects_missing_client_cert_and_accepts_configured_cert() {
        let certs = TestCertificates::new();
        let mut server_config = certs.server_tls_config(TlsMode::Enforcing);
        server_config.server.need_client_auth = TlsClientAuth::Require;
        server_config.server.auth_client = true;
        server_config.server.trust_cert_path = Some(certs.ca_cert_path());

        let no_client_cert_config = TlsConfig {
            enable: true,
            client: rocketmq_common::common::tls_config::TlsClientConfig {
                auth_server: false,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(
            !tls_connects(
                TlsMode::Enforcing,
                Some((server_config.clone(), Some(no_client_cert_config)))
            )
            .await
        );

        let mut client_config = certs.client_tls_config();
        client_config.client.auth_server = false;
        assert!(tls_connects(TlsMode::Enforcing, Some((server_config, Some(client_config)))).await);
    }

    #[cfg(feature = "tls")]
    async fn plaintext_connects(mode: TlsMode) -> bool {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let server_config = TlsConfig {
            test_mode_enable: true,
            server: rocketmq_common::common::tls_config::TlsServerConfig {
                mode,
                ..Default::default()
            },
            ..Default::default()
        };
        let runtime = TlsServerRuntime::new(server_config);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server = tokio::spawn(async move {
            let (stream, remote_addr) = listener.accept().await.expect("accept client");
            runtime.into_connection(stream, remote_addr).await.is_some()
        });

        let mut stream = TcpStream::connect(addr).await.expect("connect plaintext client");
        stream.write_all(&[0]).await.expect("write first plaintext byte");

        time::timeout(Duration::from_secs(3), server)
            .await
            .expect("server should complete")
            .expect("server task should not panic")
    }

    #[cfg(feature = "tls")]
    async fn tls_connects(mode: TlsMode, configs: Option<(TlsConfig, Option<TlsConfig>)>) -> bool {
        use tokio::net::TcpListener;

        let (server_config, client_config) = configs.unwrap_or_else(|| {
            let server_config = TlsConfig {
                test_mode_enable: true,
                server: rocketmq_common::common::tls_config::TlsServerConfig {
                    mode,
                    ..Default::default()
                },
                ..Default::default()
            };
            let client_config = TlsConfig {
                test_mode_enable: true,
                client: rocketmq_common::common::tls_config::TlsClientConfig {
                    auth_server: false,
                    ..Default::default()
                },
                ..Default::default()
            };
            (server_config, Some(client_config))
        });
        let runtime = TlsServerRuntime::new(server_config);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");

        let server = tokio::spawn(async move {
            let (stream, remote_addr) = listener.accept().await.expect("accept client");
            runtime.into_connection(stream, remote_addr).await.is_some()
        });

        let stream = TcpStream::connect(addr).await.expect("connect tls client");
        let client_result = if let Some(client_config) = client_config {
            time::timeout(
                Duration::from_secs(3),
                connect_tls_stream(stream, "localhost", &client_config),
            )
            .await
            .expect("client handshake should complete")
            .is_ok()
        } else {
            drop(stream);
            false
        };
        let server_result = time::timeout(Duration::from_secs(3), server)
            .await
            .expect("server should complete")
            .expect("server task should not panic");

        client_result && server_result
    }

    #[cfg(feature = "tls")]
    struct TestCertificates {
        _temp_dir: tempfile::TempDir,
        ca_cert_path: String,
        server_cert_path: String,
        server_key_path: String,
        client_cert_path: String,
        client_key_path: String,
    }

    #[cfg(feature = "tls")]
    impl TestCertificates {
        fn new() -> Self {
            use rcgen::BasicConstraints;
            use rcgen::Certificate;
            use rcgen::CertificateParams;
            use rcgen::ExtendedKeyUsagePurpose;
            use rcgen::IsCa;
            use rcgen::KeyPair;
            use rcgen::KeyUsagePurpose;

            let temp_dir = tempfile::tempdir().expect("create cert temp dir");
            let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("create ca params");
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.key_usages.push(KeyUsagePurpose::KeyCertSign);
            ca_params.key_usages.push(KeyUsagePurpose::CrlSign);
            let ca_key = KeyPair::generate().expect("generate ca key");
            let ca_cert = ca_params.self_signed(&ca_key).expect("self sign ca");

            fn end_entity(
                ca_cert: &Certificate,
                ca_key: &KeyPair,
                name: &str,
                usage: ExtendedKeyUsagePurpose,
            ) -> (Certificate, KeyPair) {
                let mut params = CertificateParams::new(vec![name.to_string()]).expect("create leaf params");
                params.use_authority_key_identifier_extension = true;
                params.key_usages.push(KeyUsagePurpose::DigitalSignature);
                params.extended_key_usages.push(usage);
                let key = KeyPair::generate().expect("generate leaf key");
                let cert = params.signed_by(&key, ca_cert, ca_key).expect("sign leaf");
                (cert, key)
            }

            let (server_cert, server_key) =
                end_entity(&ca_cert, &ca_key, "localhost", ExtendedKeyUsagePurpose::ServerAuth);
            let (client_cert, client_key) =
                end_entity(&ca_cert, &ca_key, "client", ExtendedKeyUsagePurpose::ClientAuth);

            let ca_cert_path = write_pem(temp_dir.path(), "ca.pem", ca_cert.pem());
            let server_cert_path = write_pem(temp_dir.path(), "server.pem", server_cert.pem());
            let server_key_path = write_pem(temp_dir.path(), "server.key", server_key.serialize_pem());
            let client_cert_path = write_pem(temp_dir.path(), "client.pem", client_cert.pem());
            let client_key_path = write_pem(temp_dir.path(), "client.key", client_key.serialize_pem());

            Self {
                _temp_dir: temp_dir,
                ca_cert_path,
                server_cert_path,
                server_key_path,
                client_cert_path,
                client_key_path,
            }
        }

        fn server_tls_config(&self, mode: TlsMode) -> TlsConfig {
            TlsConfig {
                server: rocketmq_common::common::tls_config::TlsServerConfig {
                    mode,
                    cert_path: Some(self.server_cert_path.clone()),
                    key_path: Some(self.server_key_path.clone()),
                    ..Default::default()
                },
                ..Default::default()
            }
        }

        fn client_tls_config(&self) -> TlsConfig {
            TlsConfig {
                enable: true,
                client: rocketmq_common::common::tls_config::TlsClientConfig {
                    cert_path: Some(self.client_cert_path.clone()),
                    key_path: Some(self.client_key_path.clone()),
                    ..Default::default()
                },
                ..Default::default()
            }
        }

        fn ca_cert_path(&self) -> String {
            self.ca_cert_path.clone()
        }
    }

    #[cfg(feature = "tls")]
    fn write_pem(path: &std::path::Path, file_name: &str, content: String) -> String {
        let path = path.join(file_name);
        fs::write(&path, content).expect("write pem file");
        path.to_string_lossy().to_string()
    }
}
