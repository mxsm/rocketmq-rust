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

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::SocksProxyConfig;
use rocketmq_transport::api::TlsConfig;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[tokio::test]
async fn socks5_connector_preserves_domain_target_and_authentication() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake SOCKS server");
    let proxy_addr = listener.local_addr().expect("proxy address");
    let observed = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept SOCKS client");
        let mut greeting = [0u8; 4];
        stream.read_exact(&mut greeting[..2]).await.expect("read greeting");
        let method_count = greeting[1] as usize;
        stream
            .read_exact(&mut greeting[..method_count])
            .await
            .expect("read auth methods");
        stream.write_all(&[5, 2]).await.expect("select password auth");

        let mut auth_header = [0u8; 2];
        stream.read_exact(&mut auth_header).await.expect("read auth header");
        let mut username = vec![0u8; auth_header[1] as usize];
        stream.read_exact(&mut username).await.expect("read username");
        let password_len = stream.read_u8().await.expect("read password length") as usize;
        let mut password = vec![0u8; password_len];
        stream.read_exact(&mut password).await.expect("read password");
        stream.write_all(&[1, 0]).await.expect("accept auth");

        let mut request = [0u8; 5];
        stream.read_exact(&mut request[..4]).await.expect("read CONNECT header");
        assert_eq!(&request[..4], &[5, 1, 0, 3]);
        stream.read_exact(&mut request[..1]).await.expect("read domain length");
        let mut host = vec![0u8; request[0] as usize];
        stream.read_exact(&mut host).await.expect("read target host");
        let port = stream.read_u16().await.expect("read target port");
        stream
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 80])
            .await
            .expect("accept CONNECT");
        (
            String::from_utf8(username).expect("UTF-8 username"),
            String::from_utf8(password).expect("UTF-8 password"),
            String::from_utf8(host).expect("UTF-8 host"),
            port,
        )
    });

    let json = format!(r#"{{"*.example.com":{{"addr":"{proxy_addr}","username":"alice","password":"secret"}}}}"#);
    let config = SocksProxyConfig::parse_java_json(&json).expect("parse proxy config");
    let route = config
        .route_for("broker.example.com", None)
        .expect("domain route must match");
    let _stream = route
        .connect(
            "broker.example.com",
            10911,
            RequestDeadline::after(Duration::from_secs(2)),
        )
        .await
        .expect("SOCKS CONNECT");

    assert_eq!(
        observed.await.expect("fake SOCKS server"),
        (
            "alice".to_string(),
            "secret".to_string(),
            "broker.example.com".to_string(),
            10911
        )
    );
}

#[test]
fn proxy_rules_are_validated_and_choose_the_most_specific_match() {
    let config = SocksProxyConfig::parse_java_json(
        r#"{
            "0.0.0.0/0":{"addr":"127.0.0.1:1080"},
            "10.1.0.0/16":{"addr":"127.0.0.1:1081"},
            "*.example.com":{"addr":"127.0.0.1:1082"},
            "broker.example.com":{"addr":"127.0.0.1:1083"}
        }"#,
    )
    .expect("parse proxy config");

    assert_eq!(
        config
            .route_for("broker.example.com", None)
            .expect("exact domain route")
            .endpoint()
            .rsplit_once(':')
            .unwrap()
            .1
            .parse::<u16>()
            .unwrap(),
        1083
    );
    assert_eq!(
        config
            .route_for("other.example.com", None)
            .expect("wildcard domain route")
            .endpoint()
            .rsplit_once(':')
            .unwrap()
            .1
            .parse::<u16>()
            .unwrap(),
        1082
    );
    assert_eq!(
        config
            .route_for("10.1.2.3", Some("10.1.2.3".parse::<IpAddr>().unwrap()))
            .expect("specific CIDR route")
            .endpoint()
            .rsplit_once(':')
            .unwrap()
            .1
            .parse::<u16>()
            .unwrap(),
        1081
    );

    let partial_auth =
        SocksProxyConfig::parse_java_json(r#"{"0.0.0.0/0":{"addr":"127.0.0.1:1080","username":"alice"}}"#)
            .expect_err("partial auth must fail");
    assert!(partial_auth.to_string().contains("username and password"));
}

#[tokio::test]
async fn tls_over_socks_uses_the_original_business_host_as_sni() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake SOCKS server");
    let proxy_addr = listener.local_addr().expect("proxy address");
    let observed_sni = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept SOCKS client");
        let mut greeting = [0u8; 3];
        stream.read_exact(&mut greeting).await.expect("read greeting");
        assert_eq!(greeting, [5, 1, 0]);
        stream.write_all(&[5, 0]).await.expect("select no auth");

        let mut request = [0u8; 4];
        stream.read_exact(&mut request).await.expect("read CONNECT header");
        assert_eq!(request, [5, 1, 0, 3]);
        let host_len = stream.read_u8().await.expect("read host length") as usize;
        let mut host = vec![0u8; host_len];
        stream.read_exact(&mut host).await.expect("read target host");
        let _port = stream.read_u16().await.expect("read target port");
        stream
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 80])
            .await
            .expect("accept CONNECT");

        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["business.example.com".to_string()]).expect("generate cert");
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

    let config = SocksProxyConfig::parse_java_json(&format!(r#"{{"*.example.com":{{"addr":"{proxy_addr}"}}}}"#))
        .expect("parse proxy config");
    let route = config.route_for("business.example.com", None).expect("business route");
    let tls_config = TlsConfig {
        enable: true,
        test_mode_enable: true,
        ..TlsConfig::default()
    };
    let _tls = route
        .connect_tls(
            "business.example.com",
            10911,
            &tls_config,
            RequestDeadline::after(Duration::from_secs(5)),
        )
        .await
        .expect("TLS over SOCKS");

    assert_eq!(
        observed_sni.await.expect("fake proxy TLS task"),
        (
            "business.example.com".to_string(),
            Some("business.example.com".to_string())
        )
    );
}
