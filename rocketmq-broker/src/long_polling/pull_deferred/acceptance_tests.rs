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

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cheetah_string::CheetahString;
use futures::SinkExt;
use parking_lot::Mutex;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownReport;
use rocketmq_store::CqExtUnit;
use rocketmq_store::MessageFilter;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v2::DeferredAdmission;
use rocketmq_transport::api::v2::DeferredExpiryMargins;
use rocketmq_transport::api::v2::DeferredId;
use rocketmq_transport::api::v2::DeferredWaitLimits;
use rocketmq_transport::api::v2::DeferredWakeReason;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrdering;
use rocketmq_transport::api::v2::RequestOrderingKey;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::TransportServerV2;
use rocketmq_transport::api::v2::WriteProgress;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::RemotingCommandCodec;
use tokio::io::AsyncReadExt;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio_util::codec::Framed;

use super::index::PullArrivalView;
use super::index::PullCriteriaIndex;
use super::index::PullCriteriaLimits;
use super::index::PullIndexSnapshot;
use super::index::PullScanCursor;
use super::service::PreparedPullRegistration;
use super::service::PullDeferredPrepareError;
use super::service::PullDeferredPrepareErrorKind;
use super::service::PullDeferredRegisterErrorKind;
use super::service::PullDeferredService;
use super::service::PullRetainedEstimate;
use super::service::PullSuspendTiming;
use super::PullMatchCriteria;

const TEST_TIMEOUT_MILLIS: u64 = 60_000;
const SENTINEL_CODE: i32 = 98_311;
const ORDERING_KEY: u64 = 98_312;

mod lifecycle_tests;
mod p1_tests;
#[cfg(windows)]
mod session_close_tests;

struct CountingBodyOwner {
    body: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl CountingBodyOwner {
    fn new(body: Vec<u8>, drops: Arc<AtomicUsize>) -> Self {
        Self { body, drops }
    }
}

impl AsRef<[u8]> for CountingBodyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

impl Drop for CountingBodyOwner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg(not(windows))]
fn disable_loopback_fast_path(_socket: &TcpSocket) -> std::io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn disable_loopback_fast_path(socket: &TcpSocket) -> std::io::Result<()> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawSocket;

    const SIO_LOOPBACK_FAST_PATH: u32 = 0x9800_0010;

    #[link(name = "Ws2_32")]
    unsafe extern "system" {
        #[link_name = "WSAIoctl"]
        fn wsa_ioctl(
            socket: usize,
            control_code: u32,
            input: *const c_void,
            input_len: u32,
            output: *mut c_void,
            output_len: u32,
            bytes_returned: *mut u32,
            overlapped: *mut c_void,
            completion: *mut c_void,
        ) -> i32;
    }

    let disabled = 0_i32;
    let mut bytes_returned = 0_u32;
    // SAFETY: the socket handle is valid, the input points to a Win32 BOOL for the complete call,
    // and every optional output/overlapped pointer is null as required for this synchronous IOCTL.
    let result = unsafe {
        wsa_ioctl(
            socket.as_raw_socket() as usize,
            SIO_LOOPBACK_FAST_PATH,
            std::ptr::from_ref(&disabled).cast(),
            std::mem::size_of_val(&disabled) as u32,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn configure_abortive_close(socket: &TcpStream) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: `linger` has the platform ABI required by `SO_LINGER`, and the socket descriptor
    // remains valid for the complete call.
    let linger_result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::from_ref(&linger).cast(),
            std::mem::size_of_val(&linger) as libc::socklen_t,
        )
    };
    if linger_result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn configure_abortive_close(socket: &TcpStream) -> std::io::Result<()> {
    use std::os::windows::io::AsRawSocket;

    const SOL_SOCKET: i32 = 0xffff;
    const SO_LINGER: i32 = 0x0080;

    #[repr(C)]
    struct SocketLinger {
        onoff: u16,
        linger: u16,
    }

    #[link(name = "Ws2_32")]
    unsafe extern "system" {
        fn setsockopt(socket: usize, level: i32, name: i32, value: *const i8, value_len: i32) -> i32;
    }

    let raw_socket = socket.as_raw_socket() as usize;
    let linger = SocketLinger { onoff: 1, linger: 0 };
    // SAFETY: `linger` uses the WinSock `linger` layout and the socket handle remains valid for
    // the complete call.
    let linger_result = unsafe {
        setsockopt(
            raw_socket,
            SOL_SOCKET,
            SO_LINGER,
            std::ptr::from_ref(&linger).cast(),
            std::mem::size_of_val(&linger) as i32,
        )
    };
    if linger_result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test value is non-zero")
}

struct MatchAll;

impl MessageFilter for MatchAll {
    fn is_matched_by_consume_queue(&self, _tags_code: Option<i64>, _cq_ext_unit: Option<&CqExtUnit>) -> bool {
        true
    }

    fn is_matched_by_commit_log(
        &self,
        _msg_buffer: Option<&[u8]>,
        _properties: Option<&std::collections::HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        true
    }
}

fn service_with_limits(
    controller: &AdmissionController,
    waiters: usize,
    retained_bytes: usize,
    index_entries: usize,
    per_key: usize,
) -> Arc<PullDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(waiters, retained_bytes))
        .expect("Pull deferred admission");
    Arc::new(PullDeferredService::new(
        admission,
        PullCriteriaLimits::new(nonzero(index_entries), nonzero(per_key)),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        nonzero(1),
        nonzero(1),
    ))
}

fn service(controller: &AdmissionController) -> Arc<PullDeferredService> {
    service_with_limits(controller, 4, 16 * 1024 * 1024, 4, 4)
}

fn request_header() -> PullMessageRequestHeader {
    PullMessageRequestHeader {
        consumer_group: CheetahString::from_static_str("GroupA"),
        topic: CheetahString::from_static_str("TopicA"),
        queue_id: 0,
        queue_offset: 7,
        max_msg_nums: 8,
        sys_flag: 0,
        commit_offset: 0,
        suspend_timeout_millis: TEST_TIMEOUT_MILLIS,
        sub_version: 0,
        subscription: Some(CheetahString::from_static_str("*")),
        expression_type: Some(CheetahString::from_static_str("TAG")),
        ..Default::default()
    }
}

fn request_command(opaque: i32) -> RemotingCommand {
    let mut command =
        RemotingCommand::create_request_command(RequestCode::PullMessage, request_header()).set_opaque(opaque);
    command.make_custom_header_to_net();
    command
}

#[derive(Clone, Copy)]
struct RegistrationObservation {
    id: DeferredId,
    peer: SocketAddr,
}

#[derive(Default)]
struct ProcessorBarrier {
    before_outcome: Notify,
    release_outcome: Notify,
    commit_observed: Notify,
}

#[derive(Clone)]
struct PullDeferredTestProcessor {
    service: Arc<PullDeferredService>,
    registrations: mpsc::UnboundedSender<RegistrationObservation>,
    barrier: Arc<ProcessorBarrier>,
    hold_before_outcome: bool,
    rollback_registration: bool,
}

impl RequestProcessorV2 for PullDeferredTestProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if request.command().code() == SENTINEL_CODE {
            self.barrier.commit_observed.notify_one();
            return success_reply();
        }
        let header = request
            .command()
            .decode_command_custom_header::<PullMessageRequestHeader>()?;
        let peer = match request.origin() {
            rocketmq_transport::api::v2::RequestOrigin::Network { peer } => peer.address(),
            _ => {
                return Err(RocketMQError::illegal_argument(
                    "Pull deferred test requires TCP ingress",
                ))
            }
        };
        let criteria = PullMatchCriteria::new(
            header.topic.clone(),
            header.queue_id,
            header.queue_offset,
            SubscriptionData::default(),
            Arc::new(MatchAll),
        );
        let fallback = ResponsePlan::command(RemotingCommand::create_response_command_with_code(
            ResponseCode::PullNotFound,
        ))
        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let prepared = self
            .service
            .prepare(
                request,
                criteria,
                fallback,
                PullSuspendTiming::from_policy(
                    current_millis(),
                    tokio::time::Instant::now(),
                    true,
                    header.suspend_timeout_millis,
                    1_000,
                ),
                PullRetainedEstimate::default(),
            )
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        let registration = self
            .service
            .register(prepared, request)
            .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?;
        self.registrations
            .send(RegistrationObservation {
                id: registration.deferred_id(),
                peer,
            })
            .map_err(|_| RocketMQError::illegal_argument("Pull registration observer closed"))?;
        if self.rollback_registration {
            drop(registration);
            return Err(RocketMQError::illegal_argument(
                "intentional Pull registration rollback",
            ));
        }
        if self.hold_before_outcome {
            self.barrier.before_outcome.notify_one();
            self.barrier.release_outcome.notified().await;
        }
        Ok(HandlerOutcome::Deferred(registration))
    }

    fn request_ordering(&self, _ingress: rocketmq_transport::api::v2::IngressRequestView<'_>) -> RequestOrdering {
        RequestOrdering::Ordered(RequestOrderingKey::new(ORDERING_KEY))
    }
}

fn success_reply() -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    ResponsePlan::command(RemotingCommand::create_response_command_with_code(
        ResponseCode::Success,
    ))
    .map(HandlerOutcome::Reply)
    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
}

struct RunningServer {
    owner: RuntimeOwner,
    actions: ChildServiceContext,
    shutdown: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<ShutdownReport>,
}

impl RunningServer {
    async fn finish(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let report = self.result.await.expect("owned Pull V2 server report");
        assert_clean_shutdown("Pull V2 server", &report);
        let tasks = self.owner.shutdown_tasks().await;
        assert_clean_shutdown("Pull V2 runtime", &tasks);
        let background = self.owner.shutdown_background();
        assert_clean_shutdown("Pull V2 finalization", &background);
    }
}

fn assert_clean_shutdown(owner: &str, report: &ShutdownReport) {
    assert!(report.is_healthy(), "{owner}: {}", report.to_json());
    assert_eq!(report.aborted, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.panicked, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.timed_out, 0, "{owner}: {}", report.to_json());
    assert_eq!(report.leaked, 0, "{owner}: {}", report.to_json());
    assert!(report.remaining_tasks.is_empty(), "{owner}: {}", report.to_json());
    for child in &report.children {
        assert_clean_shutdown(owner, child);
    }
}

async fn start_running_server<P>(processor: P, controller: Arc<AdmissionController>) -> (SocketAddr, RunningServer)
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("broker-pull-deferred-acceptance"))
        .expect("Pull V2 test runtime owner");
    let server_context = owner.root_context().component("pull-deferred.server");
    let runner_context = owner.root_context().component("pull-deferred.runner");
    let actions = owner.root_context().component("pull-deferred.actions");
    let server = TransportServerV2::new(
        Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        }),
        server_context,
        processor,
    )
    .with_admission_controller(controller);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner_context
        .spawn_service("pull-deferred-v2-server", async move {
            let report = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await
                .expect("owned Pull V2 server shutdown report");
            let _ = result_tx.send(report);
        })
        .expect("spawn owned Pull V2 server");
    let address = startup_rx
        .await
        .expect("Pull V2 startup channel")
        .expect("Pull V2 server startup");
    (
        address,
        RunningServer {
            owner,
            actions,
            shutdown: Some(shutdown_tx),
            result: result_rx,
        },
    )
}

async fn start_server<P>(processor: P, controller: Arc<AdmissionController>) -> (Connection, SocketAddr, RunningServer)
where
    P: RequestProcessorV2 + Clone + Sync + 'static,
{
    let (address, running) = start_running_server(processor, controller).await;
    let client = Connection::new(TcpStream::connect(address).await.expect("connect Pull V2 client"));
    (client, address, running)
}

async fn commit_barrier(client: &mut Connection, barrier: &ProcessorBarrier, opaque: i32) {
    client
        .send_command(
            RemotingCommand::create_remoting_command(SENTINEL_CODE)
                .set_opaque(opaque)
                .mark_oneway_rpc(),
        )
        .await
        .expect("send ordered Pull commit sentinel");
    barrier.commit_observed.notified().await;
}

fn assert_released(service: &PullDeferredService) {
    assert_eq!(service.index_snapshot(), PullIndexSnapshot::default());
    let admission = service.admission_snapshot();
    assert_eq!(admission.waiting_count(), 0);
    assert_eq!(admission.retained_bytes(), 0);
}

#[tokio::test]
async fn tcp_pending_wake_replays_then_writes_exactly_one_bound_frame() {
    const ORIGINAL_OPAQUE: i32 = 9_831;
    const RESPONSE_BODY: &[u8] = b"owner-backed-pull-response";
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = PullDeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: true,
        rollback_registration: false,
    };
    let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
    client
        .send_command(request_command(ORIGINAL_OPAQUE))
        .await
        .expect("send deferred Pull request");
    let registered = registrations.recv().await.expect("observe Pull registration");
    barrier.before_outcome.notified().await;

    let topic = CheetahString::from_static_str("TopicA");
    let mut cursor = PullScanCursor::new();
    let mut candidates = service.reserve_arrival_batch(&PullArrivalView::new(&topic, 0, 8), &mut cursor);
    assert_eq!(candidates.len(), 1);
    let candidate = candidates.pop().expect("one Pull candidate");
    let mut pending_claim = Box::pin(service.claim_candidate(candidate, DeferredWakeReason::MessageArrived));
    tokio::select! {
        biased;
        result = &mut pending_claim => panic!("prepared Pull claim completed before dispatcher commit: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }

    barrier.release_outcome.notify_one();
    let claim = pending_claim.await.expect("pending Pull wake replays after commit");
    assert_eq!(claim.reason(), DeferredWakeReason::MessageArrived);
    assert_eq!(service.index_snapshot(), PullIndexSnapshot::default());
    assert_eq!(service.admission_snapshot().waiting_count(), 1);

    let rereads = Arc::new(AtomicUsize::new(0));
    let reread_count = Arc::clone(&rereads);
    let peer = registered.peer;
    let service_for_resume = Arc::clone(&service);
    let owner_drops = Arc::new(AtomicUsize::new(0));
    let response_owner_drops = Arc::clone(&owner_drops);
    let (plan_ready_tx, plan_ready_rx) = oneshot::channel();
    let (release_plan_tx, release_plan_rx) = oneshot::channel();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    running
        .actions
        .spawn_service("pull-deferred-resume", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    rocketmq_transport::api::v2::DeferredResumeRetainedSize::default(),
                    move |resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        assert_eq!(resume.request().effective_peer(), peer);
                        assert_eq!(resume.criteria().pull_from_offset(), 7);
                        reread_count.fetch_add(1, Ordering::SeqCst);
                        let body =
                            Bytes::from_owner(CountingBodyOwner::new(RESPONSE_BODY.to_vec(), response_owner_drops));
                        let _ = plan_ready_tx.send(());
                        release_plan_rx
                            .await
                            .map_err(|_| RocketMQError::illegal_argument("Pull response plan release closed"))?;
                        ResponsePlan::bytes(
                            RemotingCommand::create_response_command_with_code(ResponseCode::PullNotFound),
                            body,
                        )
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn owned Pull resume");
    plan_ready_rx.await.expect("owner-backed Pull response plan ready");
    assert_eq!(owner_drops.load(Ordering::SeqCst), 0);
    let mut receipt_rx = Box::pin(receipt_rx);
    tokio::select! {
        biased;
        result = &mut receipt_rx => panic!("Pull response completed before writer release: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    release_plan_tx
        .send(())
        .expect("release owner-backed Pull response plan");

    let response = client
        .receive_command()
        .await
        .expect("Pull V2 connection remains open")
        .expect("one Pull response frame");
    receipt_rx
        .await
        .expect("Pull resume receipt channel")
        .expect("canonical Pull resume/write");
    assert_eq!(response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(response.code(), ResponseCode::PullNotFound as i32);
    assert_eq!(response.body().map(|body| body.as_ref()), Some(RESPONSE_BODY));
    assert_eq!(rereads.load(Ordering::SeqCst), 1);
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_released(&service);
    let _ = service.shutdown();
    running.finish().await;
    assert!(
        client.receive_command().await.is_none(),
        "strictly one Pull response frame"
    );
}

#[tokio::test]
#[cfg_attr(
    windows,
    ignore = "Windows loopback accepts the maximum legal frame before user-space can RST; covered by deterministic transport partial-write tests"
)]
async fn tcp_partial_write_drops_owner_once_without_retrying() {
    const ORIGINAL_OPAQUE: i32 = 9_832;
    const PARTIAL_BODY_BYTES: usize = 16 * 1024 * 1024 - 4 * 1024;
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service(controller.as_ref());
    let barrier = Arc::new(ProcessorBarrier::default());
    let (registration_tx, mut registrations) = mpsc::unbounded_channel();
    let processor = PullDeferredTestProcessor {
        service: Arc::clone(&service),
        registrations: registration_tx,
        barrier: Arc::clone(&barrier),
        hold_before_outcome: false,
        rollback_registration: false,
    };
    let (address, running) = start_running_server(processor, Arc::clone(&controller)).await;
    let connector = TcpSocket::new_v4().expect("create raw Pull V2 socket");
    disable_loopback_fast_path(&connector).expect("disable Windows loopback fast path");
    connector
        .set_recv_buffer_size(4 * 1024)
        .expect("bound partial-write receive buffer");
    let raw_client = connector.connect(address).await.expect("connect raw Pull V2 client");
    configure_abortive_close(&raw_client).expect("configure deterministic partial-write reset");
    let mut framed = Framed::new(raw_client, RemotingCommandCodec::new());
    framed
        .send(request_command(ORIGINAL_OPAQUE))
        .await
        .expect("send partial-write Pull request");
    framed
        .send(
            RemotingCommand::create_remoting_command(SENTINEL_CODE)
                .set_opaque(ORIGINAL_OPAQUE + 1)
                .mark_oneway_rpc(),
        )
        .await
        .expect("send partial-write commit sentinel");
    barrier.commit_observed.notified().await;
    let registered = registrations.recv().await.expect("observe partial-write registration");

    let topic = CheetahString::from_static_str("TopicA");
    let mut cursor = PullScanCursor::new();
    let mut candidates = service.reserve_arrival_batch(&PullArrivalView::new(&topic, 0, 8), &mut cursor);
    let candidate = candidates.pop().expect("one partial-write Pull candidate");
    assert!(candidates.is_empty());
    let claim = service
        .claim_candidate(candidate, DeferredWakeReason::MessageArrived)
        .await
        .expect("claim partial-write Pull");

    let owner_drops = Arc::new(AtomicUsize::new(0));
    let response_owner_drops = Arc::clone(&owner_drops);
    let rereads = Arc::new(AtomicUsize::new(0));
    let reread_count = Arc::clone(&rereads);
    let (plan_ready_tx, plan_ready_rx) = oneshot::channel();
    let (release_plan_tx, release_plan_rx) = oneshot::channel();
    let (receipt_tx, receipt_rx) = oneshot::channel();
    let service_for_resume = Arc::clone(&service);
    running
        .actions
        .spawn_service("pull-deferred-partial-write", async move {
            let result = service_for_resume
                .resume_claimed(
                    claim,
                    rocketmq_transport::api::v2::DeferredResumeRetainedSize::default(),
                    move |resume, reason| async move {
                        assert_eq!(reason, DeferredWakeReason::MessageArrived);
                        assert_eq!(resume.request().effective_peer(), registered.peer);
                        reread_count.fetch_add(1, Ordering::SeqCst);
                        let body = Bytes::from_owner(CountingBodyOwner::new(
                            vec![b'x'; PARTIAL_BODY_BYTES],
                            response_owner_drops,
                        ));
                        let _ = plan_ready_tx.send(());
                        release_plan_rx
                            .await
                            .map_err(|_| RocketMQError::illegal_argument("partial Pull plan release closed"))?;
                        ResponsePlan::bytes(
                            RemotingCommand::create_response_command_with_code(ResponseCode::Success),
                            body,
                        )
                        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
                    },
                )
                .await;
            let _ = receipt_tx.send(result);
        })
        .expect("spawn owned partial-write Pull resume");

    plan_ready_rx.await.expect("partial Pull response plan ready");
    assert_eq!(owner_drops.load(Ordering::SeqCst), 0);
    release_plan_tx.send(()).expect("release partial Pull response plan");
    let mut socket = framed.into_inner();
    let mut prefix = [0_u8; 64];
    socket
        .read_exact(&mut prefix)
        .await
        .expect("read a canonical Pull frame prefix");
    assert_ne!(prefix, [0_u8; 64]);
    assert_eq!(
        owner_drops.load(Ordering::SeqCst),
        0,
        "owner must remain live while the canonical write is incomplete"
    );
    drop(socket);

    let error = receipt_rx
        .await
        .expect("partial Pull resume receipt channel")
        .expect_err("peer close must fail the incomplete canonical write");
    assert_eq!(error.write_progress(), Some(WriteProgress::PossiblyPartial));
    assert_eq!(rereads.load(Ordering::SeqCst), 1, "partial writes are never retried");
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
    assert_released(&service);
    let _ = service.shutdown();
    running.finish().await;
}

#[test]
fn retained_index_estimate_is_checked_and_non_zero() {
    assert!(PullCriteriaIndex::<DeferredId>::try_retained_bytes_per_entry().is_some_and(|bytes| bytes > 0));
}
