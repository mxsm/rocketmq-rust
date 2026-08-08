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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::connect_with_config;
use rocketmq_transport::AdmissionController;
use rocketmq_transport::AdmissionLimits;
use rocketmq_transport::FrameLimits;
use rocketmq_transport::RequestDeadline;
use rocketmq_transport::ResourceLimit;
use rocketmq_transport::SessionProcessor as RequestProcessor;
use rocketmq_transport::SessionTransportServer;
use rocketmq_transport::SessionTransportServerConfig;
use rocketmq_transport::TlsConfig;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

const ANNOUNCED_FRAME_BYTES: usize = 4 * 1024 * 1024 + 4;

struct EchoProcessor;

impl RequestProcessor for EchoProcessor {
    fn process(
        &self,
        request: RemotingCommand,
    ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
        Box::pin(async move { Ok(RemotingCommand::create_response_command_with_code(0).set_opaque(request.opaque())) })
    }
}

async fn wait_for_partial_count(admission: &AdmissionController, expected: usize) {
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if admission.snapshot().partial_frames.current_count == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "partial-frame admission count should converge to {expected}: {:?}",
        admission.snapshot()
    );
}

async fn announce_incomplete_frame(address: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect partial-frame peer");
    let total = u32::try_from(ANNOUNCED_FRAME_BYTES - 4).expect("test frame length");
    stream
        .write_all(&total.to_be_bytes())
        .await
        .expect("announce partial frame");
    stream
}

#[tokio::test]
async fn announced_frames_are_bounded_before_payload_arrives_and_capacity_is_reclaimed() {
    let runtime = RuntimeContext::from_current("partial-frame-admission");
    let service = runtime.service_context("partial-frame-admission");
    let limits = AdmissionLimits {
        partial_frames: ResourceLimit {
            count: 2,
            bytes: ANNOUNCED_FRAME_BYTES * 2,
        },
        ..AdmissionLimits::default()
    };
    let admission = Arc::new(AdmissionController::new(limits));
    let server = SessionTransportServer::bind(
        service,
        SessionTransportServerConfig::loopback(),
        Arc::new(EchoProcessor),
        admission.clone(),
    )
    .await
    .expect("bind transport server");
    let address = server.local_addr();
    server.start().expect("start transport server");

    let first = announce_incomplete_frame(address).await;
    wait_for_partial_count(&admission, 1).await;
    let second = announce_incomplete_frame(address).await;
    wait_for_partial_count(&admission, 2).await;
    let snapshot = admission.snapshot().partial_frames;
    assert_eq!(snapshot.current_bytes, ANNOUNCED_FRAME_BYTES * 2);

    let mut rejected = announce_incomplete_frame(address).await;
    let mut byte = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), rejected.read(&mut byte))
        .await
        .expect("over-budget peer should be closed")
        .expect("read rejected peer");
    assert_eq!(read, 0);
    let snapshot = admission.snapshot().partial_frames;
    assert_eq!(snapshot.current_count, 2);
    assert_eq!(snapshot.current_bytes, ANNOUNCED_FRAME_BYTES * 2);
    assert_eq!(snapshot.rejected_count, 1);

    drop(first);
    drop(second);
    drop(rejected);
    wait_for_partial_count(&admission, 0).await;

    let mut healthy = connect_with_config(
        &address.to_string(),
        &TlsConfig::default(),
        FrameLimits::default(),
        RequestDeadline::after(Duration::from_secs(2)),
    )
    .await
    .expect("connect healthy peer")
    .into_parts()
    .0;
    healthy
        .send_command(RemotingCommand::create_remoting_command(RequestCode::HeartBeat).set_opaque(77))
        .await
        .expect("send healthy request");
    let response = healthy
        .receive_command()
        .await
        .expect("read healthy response")
        .expect("healthy response frame");
    assert_eq!(response.opaque(), 77);
    assert_eq!(admission.snapshot().partial_frames.current_count, 0);

    drop(healthy);
    let report = server
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(2)))
        .await;
    assert!(report.is_healthy(), "{}", report.to_json());
    let report = runtime.shutdown_tasks(Duration::from_secs(2)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
}
