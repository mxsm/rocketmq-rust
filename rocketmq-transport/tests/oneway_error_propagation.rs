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

#![cfg(feature = "test-support")]

use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetLimit;
use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::RPCHook;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::TransportClient;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::test_support::Connection;
use socket2::SockRef;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

fn test_client(name: &'static str) -> TransportClient {
    TransportClient::builder(
        Arc::new(TransportClientConfig::default()),
        DefaultRequestProcessor,
        RuntimeContext::from_current(name).service_context("oneway-test-client"),
    )
    .build()
    .expect("valid transport client configuration")
}

fn request(opaque: i32) -> RemotingCommand {
    RemotingCommand::create_remoting_command(RequestCode::GetBrokerClusterInfo).set_opaque(opaque)
}

async fn unused_loopback_address() -> CheetahString {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("reserve address");
    let address = listener.local_addr().expect("reserved address");
    drop(listener);
    CheetahString::from_string(address.to_string())
}

#[tokio::test]
async fn connection_refusal_is_returned() {
    let client = test_client("oneway-connection-refused");
    let target = unused_loopback_address().await;
    let result = client.invoke_request_oneway(&target, request(1), 200).await;
    assert!(result.is_err());
    client.shutdown();
}

#[tokio::test]
async fn expired_absolute_deadline_is_returned_before_connect() {
    let client = test_client("oneway-expired-deadline");
    let target = unused_loopback_address().await;
    let result = client
        .invoke_request_oneway_with_deadline(&target, request(2), RequestDeadline::after(Duration::ZERO))
        .await;
    assert!(result.is_err());
    client.shutdown();
}

struct RejectingHook {
    calls: AtomicUsize,
}

impl RPCHook for RejectingHook {
    fn do_before_request(&self, _remote_addr: SocketAddr, _request: &mut RemotingCommand) -> RocketMQResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(RocketMQError::illegal_argument("injected one-way hook rejection"))
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn before_hook_error_is_returned_without_sending_a_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind hook peer");
    let target = CheetahString::from_string(listener.local_addr().expect("hook peer address").to_string());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept hook client");
        let mut byte = [0_u8; 1];
        socket.read(&mut byte).await.expect("read hook client")
    });
    let client = test_client("oneway-hook-error");
    let hook = Arc::new(RejectingHook {
        calls: AtomicUsize::new(0),
    });
    client.register_rpc_hook(hook.clone());

    let result = client.invoke_request_oneway(&target, request(3), 1_000).await;
    assert!(result.is_err());
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    client.shutdown();
    assert_eq!(
        peer.await.expect("hook peer task"),
        0,
        "hook rejection must not write bytes"
    );
}

#[tokio::test]
async fn admission_rebind_rejection_is_returned() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind admission peer");
    let target = CheetahString::from_string(listener.local_addr().expect("admission peer address").to_string());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept admission client");
        let mut bytes = Vec::new();
        socket.read_to_end(&mut bytes).await.expect("drain admission client");
        bytes
    });
    let client = test_client("oneway-admission-reject");
    let source = ResourceBudgetTree::new(
        "oneway-test-source",
        BudgetLimit::new(1, 300 * 1024 * 1024, FullPolicy::Reject),
    )
    .expect("source budget");
    let permit = source
        .root()
        .try_acquire(300 * 1024 * 1024, BudgetClass::Data)
        .expect("source reservation");

    let result = client
        .invoke_oneway_with_permit(
            &target,
            request(4),
            RequestDeadline::after(Duration::from_secs(1)),
            permit,
        )
        .await;
    assert!(result.is_err());
    client.shutdown();
    assert!(peer.await.expect("admission peer task").is_empty());
}

#[tokio::test]
async fn peer_close_is_returned_by_the_next_oneway_send() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind closing peer");
    let target = CheetahString::from_string(listener.local_addr().expect("closing peer address").to_string());
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept closing client");
        SockRef::from(&socket)
            .set_linger(Some(Duration::ZERO))
            .expect("force a deterministic reset after the first frame");
        let mut connection = Connection::new(socket);
        connection
            .receive_command()
            .await
            .expect("receive first frame")
            .expect("first one-way frame");
    });
    let client = test_client("oneway-peer-close");
    client
        .invoke_request_oneway(&target, request(5), 1_000)
        .await
        .expect("first one-way send");
    peer.await.expect("closing peer task");
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    let result = client.invoke_request_oneway(&target, request(6), 300).await;
    assert!(result.is_err());
    client.shutdown();
}

#[tokio::test]
async fn client_shutdown_is_returned_without_reconnecting() {
    let client = test_client("oneway-client-shutdown");
    client.shutdown();
    let target = unused_loopback_address().await;
    let result = client.invoke_request_oneway(&target, request(7), 200).await;
    assert!(matches!(result, Err(RocketMQError::ClientNotStarted)));
}
