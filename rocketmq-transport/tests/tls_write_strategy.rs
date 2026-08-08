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

#![cfg(all(feature = "tls", feature = "test-support"))]

use rocketmq_protocol::protocol::encoded_frame::EncodedFrame;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_transport::benchmark_support;
use rocketmq_transport::FrameWriteMode;
use rocketmq_transport::FrameWriter;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;

async fn write_and_decrypt(body_bytes: usize, mode: FrameWriteMode) {
    let frame = EncodedFrame::from_command(
        RemotingCommand::create_remoting_command(10_100)
            .set_opaque(7)
            .set_body(vec![0x5a; body_bytes]),
    )
    .expect("encode TLS test frame");
    let expected = frame.clone().into_bytes();

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind TLS test listener");
    let address = listener.local_addr().expect("TLS test address");
    let acceptor = benchmark_support::tls_acceptor();
    let server_expected = expected.clone();
    let receiver = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept TLS test socket");
        let mut tls = acceptor.accept(socket).await.expect("server TLS handshake");
        let mut plaintext = vec![0_u8; server_expected.len()];
        tls.read_exact(&mut plaintext).await.expect("read decrypted frame");
        plaintext
    });

    let socket = TcpStream::connect(address).await.expect("connect TLS test socket");
    let server_name = ServerName::try_from("localhost".to_string()).expect("TLS server name");
    let tls = benchmark_support::tls_connector()
        .connect(server_name, socket)
        .await
        .expect("client TLS handshake");
    let mut writer = FrameWriter::new(tls, mode).expect("TLS frame writer");
    writer.write_frame(&frame).await.expect("write TLS frame");
    writer.shutdown().await.expect("shutdown TLS writer");

    assert_eq!(receiver.await.expect("TLS receiver task"), expected);
}

#[tokio::test]
async fn tls_auto_preserves_small_coalesced_and_large_vectored_frames() {
    const MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;
    const CROSSOVER_BYTES: usize = 16 * 1024;

    for body_bytes in [128, 64 * 1024] {
        write_and_decrypt(
            body_bytes,
            FrameWriteMode::TlsAuto {
                max_plaintext_frame_bytes: MAX_FRAME_BYTES,
                coalesce_below_bytes: CROSSOVER_BYTES,
            },
        )
        .await;
    }
}

#[tokio::test]
async fn explicit_tls_strategies_emit_identical_plaintext() {
    const MAX_FRAME_BYTES: usize = 128 * 1024;

    write_and_decrypt(
        4 * 1024,
        FrameWriteMode::TlsCoalesced {
            max_plaintext_frame_bytes: MAX_FRAME_BYTES,
        },
    )
    .await;
    write_and_decrypt(
        4 * 1024,
        FrameWriteMode::TlsVectored {
            max_plaintext_frame_bytes: MAX_FRAME_BYTES,
        },
    )
    .await;
}
