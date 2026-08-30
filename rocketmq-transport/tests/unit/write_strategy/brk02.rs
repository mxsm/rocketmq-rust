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

use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
use tokio::io::AsyncReadExt;

use super::select_file_transfer;
use super::SelectedFileTransfer;
use crate::connection::Connection;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionLease;
use crate::file_region::FileRegionSequence;
use crate::file_region::FileTransferMode;

struct CountingLease {
    file: File,
    accesses: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl FileRegionLease for CountingLease {
    fn file(&self) -> &File {
        self.accesses.fetch_add(1, Ordering::SeqCst);
        &self.file
    }
}

impl Drop for CountingLease {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_region(body: &[u8]) -> (FileRegion, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let mut file = tempfile::tempfile().expect("temporary transfer file");
    file.write_all(body).expect("write transfer body");
    let accesses = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = Arc::new(CountingLease {
        file,
        accesses: Arc::clone(&accesses),
        drops: Arc::clone(&drops),
    });
    let region = FileRegion::try_new(lease.clone(), 0, body.len() as u64).expect("validated transfer region");
    drop(lease);
    (region, accesses, drops)
}

fn response_head(code: i32, opaque: i32) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(code).set_opaque(opaque)
}

#[tokio::test]
async fn forced_portable_and_auto_compat_fallback_are_selected_without_global_counters() {
    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    let blocking = owner
        .root_context()
        .component("brk02-auto-portable")
        .storage_io()
        .clone();
    let body = b"auto-portable-file-region";
    let (region, accesses, drops) = counting_region(body);

    let forced = select_file_transfer(FileTransferMode::Portable, Some(blocking.clone()), true, true, &region)
        .await
        .expect("forced portable selection");
    assert!(matches!(forced, SelectedFileTransfer::Portable { fallback: false, .. }));
    let automatic = select_file_transfer(FileTransferMode::Auto, Some(blocking.clone()), false, false, &region)
        .await
        .expect("automatic compatibility selection");
    assert!(matches!(
        automatic,
        SelectedFileTransfer::Portable { fallback: true, .. }
    ));

    let probe = region.clone();
    let (server_io, client_io) = tokio::io::duplex(256 * 1024);
    let mut server =
        Connection::new_with_plaintext_stream(server_io).with_file_region_io(blocking, FileTransferMode::Auto);
    let mut client = Connection::new_with_plaintext_stream(client_io);
    let send = server.send_file_regions_response(response_head(701, 71), FileRegionSequence::single(region));
    let receive = client.receive_command();
    let (send_result, receive_result) = tokio::join!(send, receive);

    send_result.expect("automatic portable file response");
    let received = receive_result
        .expect("automatic portable receive")
        .expect("automatic portable frame");
    assert_eq!(received.opaque(), 71);
    assert_eq!(received.body().map(bytes::Bytes::as_ref), Some(&body[..]));
    assert!(accesses.load(Ordering::SeqCst) >= 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    assert_eq!(probe.cached_sendfile_support(), None);
    drop(probe);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn real_rustls_file_region_stays_portable_and_never_probes_sendfile() {
    use tokio_rustls::rustls::pki_types::ServerName;

    let tls_config = crate::config::TlsConfig {
        enable: true,
        test_mode_enable: true,
        ..crate::config::TlsConfig::default()
    };
    let acceptor = crate::tls::build_server_acceptor(&tls_config).expect("test TLS acceptor");
    let connector = tokio_rustls::TlsConnector::from(Arc::new(
        crate::tls::build_client_config(&tls_config).expect("test TLS client config"),
    ));
    let server_name = ServerName::try_from("localhost").expect("test TLS server name");
    let (server_io, client_io) = tokio::io::duplex(256 * 1024);
    let (server_tls, client_tls) = tokio::join!(acceptor.accept(server_io), connector.connect(server_name, client_io));
    let server_tls = server_tls.expect("server TLS handshake");
    let client_tls = client_tls.expect("client TLS handshake");

    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    let blocking = owner
        .root_context()
        .component("brk02-rustls-portable")
        .storage_io()
        .clone();
    let body = vec![0x6b; 32 * 1024];
    let (region, accesses, drops) = counting_region(&body);
    let probe = region.clone();
    let mut server = Connection::new_with_tls_stream(server_tls).with_file_region_io(blocking, FileTransferMode::Auto);
    let mut client = Connection::new_with_tls_stream(client_tls);
    let send = server.send_file_regions_response(response_head(702, 72), FileRegionSequence::single(region));
    let receive = client.receive_command();
    let (send_result, receive_result) = tokio::join!(send, receive);

    send_result.expect("TLS portable file response");
    let received = receive_result.expect("TLS receive").expect("TLS frame");
    assert_eq!(received.opaque(), 72);
    assert_eq!(received.body().map(bytes::Bytes::as_ref), Some(body.as_slice()));
    assert!(accesses.load(Ordering::SeqCst) >= 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    #[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
    assert_eq!(probe.cached_sendfile_support(), None);
    drop(probe);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
async fn capture_real_tcp_file_frame(
    region: FileRegion,
    blocking: rocketmq_runtime::BlockingExecutor,
    mode: FileTransferMode,
) -> Vec<u8> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("loopback listener address");
    let (client, accepted) = tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
    let mut client = client.expect("connect loopback client");
    let server = accepted.expect("accept loopback client").0;
    let mut server = Connection::new(server).with_file_region_io(blocking, mode);

    let send = server.send_file_regions_response(response_head(703, 73), FileRegionSequence::single(region));
    let receive = async move {
        let mut prefix = [0_u8; 4];
        client.read_exact(&mut prefix).await.expect("read raw frame prefix");
        let announced = i32::from_be_bytes(prefix);
        let payload_len = usize::try_from(announced).expect("positive raw frame length");
        let mut frame = Vec::with_capacity(payload_len + prefix.len());
        frame.extend_from_slice(&prefix);
        frame.resize(payload_len + prefix.len(), 0);
        client
            .read_exact(&mut frame[prefix.len()..])
            .await
            .expect("read complete raw frame payload");
        frame
    };
    let (send_result, frame) = tokio::join!(send, receive);
    send_result.expect("real TCP file response");
    frame
}

#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
#[tokio::test]
async fn auto_sendfile_and_portable_produce_identical_complete_real_tcp_frames() {
    const BODY_LEN: usize = 64 * 1024;

    let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("runtime owner");
    let blocking = owner
        .root_context()
        .component("brk02-linux-sendfile")
        .storage_io()
        .clone();
    let body = vec![0x73; BODY_LEN];
    let (region, accesses, drops) = counting_region(&body);
    let probe = region.clone();
    let portable = capture_real_tcp_file_frame(region.clone(), blocking.clone(), FileTransferMode::Portable).await;
    assert_eq!(probe.cached_sendfile_support(), None);
    let automatic = capture_real_tcp_file_frame(region, blocking, FileTransferMode::Auto).await;

    assert_eq!(
        portable, automatic,
        "portable and sendfile must emit identical wire bytes"
    );
    let announced = usize::try_from(i32::from_be_bytes(portable[..4].try_into().expect("raw prefix")))
        .expect("positive raw frame length");
    assert_eq!(portable.len(), announced + 4);
    assert!(portable.len() >= BODY_LEN + 4);
    assert_eq!(probe.cached_sendfile_support(), Some(true));
    assert!(accesses.load(Ordering::SeqCst) >= 4);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(probe);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
