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

#![recursion_limit = "256"]

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use pkcs8::LineEnding;
use pkcs8::PrivateKeyInfoRef;
use rocketmq_client_rust::ClientConfig;
use rocketmq_transport::api::PrivateKeyLoader;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::SocksProxyConfig;
use rocketmq_transport::api::TlsConfig;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[test]
fn encrypted_pkcs8_profile_is_secret_safe() {
    let rcgen::CertifiedKey { signing_key, .. } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("generate key");
    let key_der = signing_key.serialize_der();
    let key_info = PrivateKeyInfoRef::try_from(key_der.as_slice()).expect("parse key");
    let encrypted = key_info
        .encrypt("transport-password")
        .expect("encrypt key")
        .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
        .expect("encode PEM");
    let directory = tempfile::tempdir().expect("temp directory");
    let path = directory.path().join("encrypted-key.pem");
    fs::write(&path, encrypted.as_bytes()).expect("write encrypted key");

    assert!(
        !PrivateKeyLoader::load(&path, "tls.client.keyPath", Some("transport-password"))
            .expect("decrypt key")
            .secret_der()
            .is_empty()
    );
    let error = PrivateKeyLoader::load(&path, "tls.client.keyPath", Some("wrong-secret"))
        .expect_err("wrong password must fail")
        .to_string();
    assert!(error.contains("decryption failed"));
    assert!(!error.contains("wrong-secret"));
}

#[test]
fn client_profile_validates_auth_and_selects_domain_or_cidr_route() {
    let mut client = ClientConfig::default();
    client.set_socks_proxy_config(
        r#"{
            "0.0.0.0/0":{"addr":"127.0.0.1:1080"},
            "*.example.com":{"addr":"127.0.0.1:1081"}
        }"#
        .into(),
    );
    let proxy = client.parse_socks_proxy_config().expect("parse client proxy config");
    assert_eq!(
        proxy
            .route_for("broker.example.com", None)
            .expect("domain route")
            .endpoint(),
        "127.0.0.1:1081"
    );
    assert_eq!(
        proxy
            .route_for("10.0.0.1", Some("10.0.0.1".parse().unwrap()))
            .expect("CIDR route")
            .endpoint(),
        "127.0.0.1:1080"
    );

    client.set_socks_proxy_config(r#"{"0.0.0.0/0":{"addr":"127.0.0.1:1080","username":"alice"}}"#.into());
    assert!(client.parse_socks_proxy_config().is_err());
}

#[tokio::test]
async fn socks_tunnel_preserves_business_dns_name_for_connect_and_tls_sni() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake SOCKS server");
    let proxy_addr = listener.local_addr().expect("proxy address");
    let observed = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept SOCKS client");
        let mut greeting = [0u8; 3];
        stream.read_exact(&mut greeting).await.expect("read greeting");
        stream.write_all(&[5, 0]).await.expect("select no auth");
        let mut connect = [0u8; 4];
        stream.read_exact(&mut connect).await.expect("read CONNECT header");
        assert_eq!(connect, [5, 1, 0, 3]);
        let host_len = stream.read_u8().await.expect("read host length") as usize;
        let mut host = vec![0u8; host_len];
        stream.read_exact(&mut host).await.expect("read host");
        let _port = stream.read_u16().await.expect("read port");
        stream
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 80])
            .await
            .expect("accept CONNECT");

        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["broker.example.com".to_string()]).expect("generate cert");
        let key = tokio_rustls::rustls::pki_types::PrivateKeyDer::Pkcs8(
            tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(signing_key.serialize_der()),
        );
        let server = tokio_rustls::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.der().clone()], key)
            .expect("server TLS config");
        let tls = tokio_rustls::TlsAcceptor::from(Arc::new(server))
            .accept(stream)
            .await
            .expect("accept TLS");
        (
            String::from_utf8(host).expect("UTF-8 host"),
            tls.get_ref().1.server_name().map(str::to_string),
        )
    });

    let proxy = SocksProxyConfig::parse_java_json(&format!(r#"{{"*.example.com":{{"addr":"{proxy_addr}"}}}}"#))
        .expect("parse proxy config");
    let tls = TlsConfig {
        enable: true,
        test_mode_enable: true,
        ..TlsConfig::default()
    };
    let _stream = proxy
        .route_for("broker.example.com", None)
        .expect("domain route")
        .connect_tls(
            "broker.example.com",
            10911,
            &tls,
            RequestDeadline::after(Duration::from_secs(5)),
        )
        .await
        .expect("TLS over SOCKS");

    assert_eq!(
        observed.await.expect("proxy task"),
        ("broker.example.com".to_string(), Some("broker.example.com".to_string()))
    );
}
