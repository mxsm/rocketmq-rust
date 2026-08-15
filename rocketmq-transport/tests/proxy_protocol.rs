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
use std::net::Ipv4Addr;
use std::net::SocketAddr;

use rocketmq_transport::api::v1::read_proxy_protocol;
use rocketmq_transport::api::v1::ProxyProtocolConfig;
use rocketmq_transport::api::v1::UnknownTlvPolicy;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

fn enabled_config() -> ProxyProtocolConfig {
    ProxyProtocolConfig {
        enabled: true,
        trusted_proxies: vec!["127.0.0.0/8".parse().expect("CIDR")],
        allowed_tlvs: vec![0xe1],
        unknown_tlv_policy: UnknownTlvPolicy::Ignore,
        max_header_bytes: 512,
        header_timeout_millis: 250,
    }
}

async fn connected_pair() -> (TcpStream, TcpStream, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let client = TcpStream::connect(address).await.expect("connect");
    let (server, peer) = listener.accept().await.expect("accept");
    (client, server, peer)
}

#[tokio::test]
async fn parses_v1_and_preserves_the_application_payload() {
    let (mut client, mut server, peer) = connected_pair().await;
    client
        .write_all(b"PROXY TCP4 198.51.100.7 192.0.2.10 43123 10911\r\npayload")
        .await
        .expect("write");

    let metadata = read_proxy_protocol(&mut server, peer, &enabled_config())
        .await
        .expect("parse")
        .expect("metadata");

    assert_eq!(metadata.transport_peer, peer);
    assert_eq!(metadata.source, "198.51.100.7:43123".parse().expect("source"));
    assert_eq!(metadata.destination, "192.0.2.10:10911".parse().expect("destination"));
    let mut payload = [0_u8; 7];
    server.read_exact(&mut payload).await.expect("read payload");
    assert_eq!(&payload, b"payload");
}

#[tokio::test]
async fn parses_v2_tcp4_and_only_retains_allowlisted_tlvs() {
    let (mut client, mut server, peer) = connected_pair().await;
    let mut header = Vec::from(*b"\r\n\r\n\0\r\nQUIT\n");
    header.extend_from_slice(&[0x21, 0x11]);
    let mut payload = vec![198, 51, 100, 8, 192, 0, 2, 11, 0xa8, 0x74, 0x2a, 0x9f];
    payload.extend_from_slice(&[0xe1, 0, 4]);
    payload.extend_from_slice(b"rust");
    payload.extend_from_slice(&[0xe2, 0, 3]);
    payload.extend_from_slice(b"old");
    header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    header.extend_from_slice(&payload);
    header.extend_from_slice(b"frame");
    client.write_all(&header).await.expect("write");

    let metadata = read_proxy_protocol(&mut server, peer, &enabled_config())
        .await
        .expect("parse")
        .expect("metadata");

    assert_eq!(metadata.source, "198.51.100.8:43124".parse().expect("source"));
    assert_eq!(metadata.destination, "192.0.2.11:10911".parse().expect("destination"));
    assert_eq!(metadata.tlvs.get(&0xe1).map(Vec::as_slice), Some(b"rust".as_slice()));
    assert!(!metadata.tlvs.contains_key(&0xe2));
    let mut frame = [0_u8; 5];
    server.read_exact(&mut frame).await.expect("read frame");
    assert_eq!(&frame, b"frame");
}

#[tokio::test]
async fn disabled_mode_consumes_no_bytes() {
    let (mut client, mut server, peer) = connected_pair().await;
    let bytes = b"PROXY TCP4 198.51.100.7 192.0.2.10 43123 10911\r\npayload";
    client.write_all(bytes).await.expect("write");

    assert!(read_proxy_protocol(&mut server, peer, &ProxyProtocolConfig::default())
        .await
        .expect("disabled")
        .is_none());
    let mut actual = vec![0_u8; bytes.len()];
    server.read_exact(&mut actual).await.expect("read unchanged bytes");
    assert_eq!(actual, bytes);
}

#[tokio::test]
async fn untrusted_or_malformed_headers_fail_closed() {
    let (mut client, mut server, _) = connected_pair().await;
    client
        .write_all(b"PROXY TCP4 198.51.100.7 192.0.2.10 43123 10911\r\n")
        .await
        .expect("write");
    let untrusted = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)), 1234);
    assert!(read_proxy_protocol(&mut server, untrusted, &enabled_config())
        .await
        .is_err());

    let (mut client, mut server, peer) = connected_pair().await;
    client.write_all(b"PROXY TCP4 broken\r\n").await.expect("write");
    assert!(read_proxy_protocol(&mut server, peer, &enabled_config()).await.is_err());
}

#[tokio::test]
async fn reject_policy_rejects_unknown_tlvs() {
    let (mut client, mut server, peer) = connected_pair().await;
    let mut header = Vec::from(*b"\r\n\r\n\0\r\nQUIT\n");
    header.extend_from_slice(&[0x21, 0x11, 0, 18]);
    header.extend_from_slice(&[127, 0, 0, 1, 127, 0, 0, 1, 0x2a, 0x9f, 0x2a, 0x9f]);
    header.extend_from_slice(&[0xe2, 0, 3]);
    header.extend_from_slice(b"bad");
    client.write_all(&header).await.expect("write");
    let mut config = enabled_config();
    config.unknown_tlv_policy = UnknownTlvPolicy::Reject;

    assert!(read_proxy_protocol(&mut server, peer, &config).await.is_err());
}
