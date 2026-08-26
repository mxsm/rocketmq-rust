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

use std::error::Error as _;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::command_custom_header::CommandCustomHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_runtime::BlockingLane;
use rocketmq_runtime::BlockingPoolPolicy;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::TaskGroup;

use super::*;
use crate::connection::ConnectionState;
use crate::deadline::RequestDeadline;
use crate::dispatch::BoundResponsePlan;
use crate::dispatch::OriginalRequestIdentity;
use crate::dispatch::RequestMeta;
use crate::dispatch::ResponsePlan;
use crate::dispatch::ResponseSink;
use crate::dispatch::ResponseTerminalState;
use crate::file_region::FileRegion;
use crate::file_region::FileRegionLease;
use crate::file_region::FileRegionSequence;
use crate::session_view::SessionStateView;

struct ControlHarness {
    runtime: RuntimeContext,
    parent: TaskGroup,
    _state_tx: tokio::sync::watch::Sender<ConnectionState>,
    _closed_tx: tokio::sync::watch::Sender<bool>,
}

impl ControlHarness {
    fn new(name: &'static str, deadline: Option<RequestDeadline>) -> (Self, RequestControlView) {
        Self::with_runtime(RuntimeContext::from_current(name), name, deadline)
    }

    fn with_policy(
        name: &'static str,
        deadline: Option<RequestDeadline>,
        policy: BlockingPoolPolicy,
    ) -> (Self, RequestControlView) {
        let runtime = RuntimeContext::try_from_current_with_blocking_policy(name, policy)
            .expect("blocking policy should construct a test runtime");
        Self::with_runtime(runtime, name, deadline)
    }

    fn with_runtime(
        runtime: RuntimeContext,
        name: &'static str,
        deadline: Option<RequestDeadline>,
    ) -> (Self, RequestControlView) {
        let parent = runtime.service_context(name).task_group().clone();
        let (state_tx, state_rx) = tokio::sync::watch::channel(ConnectionState::Healthy);
        let (closed_tx, closed_rx) = tokio::sync::watch::channel(false);
        let control = RequestControlView::from_meta(
            &RequestMeta::new(Instant::now(), deadline),
            SessionStateView::from_receivers(state_rx, closed_rx),
            &parent,
        );
        (
            Self {
                runtime,
                parent,
                _state_tx: state_tx,
                _closed_tx: closed_tx,
            },
            control,
        )
    }

    fn blocking(&self) -> &BlockingExecutor {
        self.runtime.blocking(BlockingLane::StorageIo)
    }

    async fn shutdown(self) {
        let report = self.runtime.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    }
}

fn response_head(code: i32, opaque: i32) -> RemotingCommand {
    RemotingCommand::create_response_command_with_code(code).set_opaque(opaque)
}

fn bind(plan: ResponsePlan, owner: u64, opaque: i32) -> BoundResponsePlan {
    let request = RemotingCommand::create_remoting_command(31).set_opaque(opaque);
    let identity = OriginalRequestIdentity::capture(owner, &AtomicU64::new(1), &request)
        .expect("test request identity should allocate");
    plan.bind(identity).expect("ordinary request should bind")
}

async fn handoff(plan: ResponsePlan, control: RequestControlView) -> (LocalResponsePlanReceiver, ResponseSink) {
    let (sink, receiver) = ResponseSink::local_plan(control);
    let duplicate = sink.clone();
    sink.send_plan(bind(plan, 701, 91))
        .await
        .expect("local response ownership handoff should complete");
    (receiver, duplicate)
}

fn limits(max_body_bytes: usize, max_body_parts: usize) -> LegacyMaterializationLimits {
    LegacyMaterializationLimits::try_new(FrameLimits::default(), max_body_bytes, max_body_parts)
        .expect("test materialization limits should be valid")
}

fn expect_materialization_error(
    result: Result<RemotingCommand, LegacyLocalMaterializationError>,
    context: &'static str,
) -> LegacyLocalMaterializationError {
    match result {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    }
}

struct CountingHeader {
    encodes: Arc<AtomicUsize>,
}

impl CommandCustomHeader for CountingHeader {
    fn to_map(&self) -> Option<std::collections::HashMap<CheetahString, CheetahString>> {
        self.encodes.fetch_add(1, Ordering::SeqCst);
        Some(std::collections::HashMap::new())
    }
}

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

fn counting_region(
    contents: &[u8],
) -> (
    FileRegionSequence,
    Arc<CountingLease>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let mut file = tempfile::tempfile().expect("temporary file");
    file.write_all(contents).expect("write file contents");
    let accesses = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = Arc::new(CountingLease {
        file,
        accesses: Arc::clone(&accesses),
        drops: Arc::clone(&drops),
    });
    let region = FileRegion::try_new(lease.clone(), 0, contents.len() as u64).expect("valid file region");
    (FileRegionSequence::single(region), lease, accesses, drops)
}

#[test]
fn materialization_limits_are_fallible_bounded_and_have_no_implicit_default() {
    let frame_limits = FrameLimits::default();
    assert!(LegacyMaterializationLimits::try_new(frame_limits, frame_limits.max_body_bytes, usize::MAX).is_ok());
    assert!(matches!(
        LegacyMaterializationLimits::try_new(frame_limits, frame_limits.max_body_bytes + 1, 1),
        Err(LegacyLocalMaterializationError::Limits { .. })
    ));

    let invalid_frame_limits = FrameLimits {
        max_frame_bytes: 7,
        ..frame_limits
    };
    assert!(matches!(
        LegacyMaterializationLimits::try_new(invalid_frame_limits, 0, 0),
        Err(LegacyLocalMaterializationError::Limits { .. })
    ));
}

#[tokio::test]
async fn empty_bytes_single_segment_and_multi_segment_follow_their_copy_contracts() {
    let (harness, control) = ControlHarness::new("legacy-materializer-body-kinds", None);

    let (receiver, _) = handoff(
        ResponsePlan::command(response_head(71, 1)).expect("empty plan"),
        control.clone(),
    )
    .await;
    let empty = receiver
        .receive_command(limits(0, 0), harness.blocking())
        .await
        .expect("empty command");
    assert!(empty.body().is_none());

    let bytes = Bytes::from_static(b"contiguous");
    let bytes_pointer = bytes.as_ptr();
    let (receiver, _) = handoff(
        ResponsePlan::bytes(response_head(72, 2), bytes).expect("bytes plan"),
        control.clone(),
    )
    .await;
    let bytes_command = receiver
        .receive_command(limits(10, 1), harness.blocking())
        .await
        .expect("bytes command");
    assert_eq!(bytes_command.body().expect("body").as_ptr(), bytes_pointer);

    let segment = Bytes::from_static(b"one-segment");
    let segment_pointer = segment.as_ptr();
    let (receiver, _) = handoff(
        ResponsePlan::segments(response_head(73, 3), vec![segment]).expect("single segment plan"),
        control.clone(),
    )
    .await;
    let segment_command = receiver
        .receive_command(limits(11, 1), harness.blocking())
        .await
        .expect("single segment command");
    assert_eq!(segment_command.body().expect("body").as_ptr(), segment_pointer);

    let first = Bytes::from_static(b"first");
    let second = Bytes::from_static(b"second");
    let first_pointer = first.as_ptr();
    let second_pointer = second.as_ptr();
    let (receiver, _) = handoff(
        ResponsePlan::segments(response_head(74, 4), vec![first, second]).expect("multi segment plan"),
        control,
    )
    .await;
    let multi = receiver
        .receive_command(limits(11, 2), harness.blocking())
        .await
        .expect("multi segment command");
    let body = multi.body().expect("body");
    assert_eq!(body.as_ref(), b"firstsecond");
    assert_ne!(body.as_ptr(), first_pointer);
    assert_ne!(body.as_ptr(), second_pointer);

    harness.shutdown().await;
}

#[tokio::test]
async fn cached_body_and_part_limits_are_exact_and_reject_one_over_before_encoding_or_file_access() {
    let (harness, control) = ControlHarness::new("legacy-materializer-cached-limits", None);

    let (receiver, _) = handoff(
        ResponsePlan::bytes(response_head(75, 5), Bytes::from_static(b"exact")).expect("exact body plan"),
        control.clone(),
    )
    .await;
    assert_eq!(
        receiver
            .receive_command(limits(5, 1), harness.blocking())
            .await
            .expect("exact body limit")
            .body()
            .expect("body"),
        &Bytes::from_static(b"exact")
    );

    let encodes = Arc::new(AtomicUsize::new(0));
    let head = response_head(76, 6).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    });
    let (regions, lease, accesses, drops) = counting_region(b"one-over");
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    let (receiver, duplicate) = handoff(
        ResponsePlan::file_regions(head, regions).expect("file plan"),
        control.clone(),
    )
    .await;
    let error = expect_materialization_error(
        receiver.receive_command(limits(7, 1), harness.blocking()).await,
        "one-over body limit must fail",
    );
    assert!(matches!(error, LegacyLocalMaterializationError::Limits { .. }));
    assert_eq!(encodes.load(Ordering::SeqCst), 0);
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert!(matches!(
        duplicate
            .send_plan(bind(
                ResponsePlan::command(response_head(77, 7)).expect("duplicate plan"),
                702,
                92,
            ))
            .await,
        Err(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Completed
        })
    ));
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let (receiver, _) = handoff(
        ResponsePlan::segments(
            response_head(78, 8),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .expect("two part plan"),
        control.clone(),
    )
    .await;
    assert!(receiver.receive_command(limits(2, 2), harness.blocking()).await.is_ok());

    let (receiver, _) = handoff(
        ResponsePlan::segments(
            response_head(79, 9),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .expect("two part plan"),
        control,
    )
    .await;
    assert!(matches!(
        receiver.receive_command(limits(2, 1), harness.blocking()).await,
        Err(LegacyLocalMaterializationError::Limits { .. })
    ));

    harness.shutdown().await;
}

#[tokio::test]
async fn exact_frame_preflight_applies_to_empty_and_rejects_invalid_or_one_over_heads() {
    let (harness, control) = ControlHarness::new("legacy-materializer-frame-preflight", None);
    let encodes = Arc::new(AtomicUsize::new(0));
    let counted_head = response_head(80, 10).set_command_custom_header(CountingHeader {
        encodes: Arc::clone(&encodes),
    });
    let (receiver, _) = handoff(
        ResponsePlan::command(counted_head).expect("counted empty plan"),
        control.clone(),
    )
    .await;
    assert!(receiver.receive_command(limits(0, 0), harness.blocking()).await.is_ok());
    assert_eq!(encodes.load(Ordering::SeqCst), 1);

    let head = response_head(80, 10).set_remark("exact frame head");
    let oracle = FrameLimits::java_compatibility()
        .encode_frame_head(head.clone(), 0)
        .expect("oracle head")
        .encoded_len();
    let exact_frame_limits = FrameLimits::try_new(oracle, FrameLimits::java_compatibility().max_header_bytes, 0, 8)
        .expect("exact frame limits");
    let exact_limits = LegacyMaterializationLimits::try_new(exact_frame_limits, 0, 0).expect("exact limits");
    let (receiver, _) = handoff(ResponsePlan::command(head).expect("empty plan"), control.clone()).await;
    assert!(receiver.receive_command(exact_limits, harness.blocking()).await.is_ok());

    let one_over_frame_limits =
        FrameLimits::try_new(oracle - 1, FrameLimits::java_compatibility().max_header_bytes, 0, 8)
            .expect("one-over frame limits");
    let one_over_limits = LegacyMaterializationLimits::try_new(one_over_frame_limits, 0, 0).expect("one-over limits");
    let (receiver, _) = handoff(
        ResponsePlan::command(response_head(80, 10).set_remark("exact frame head")).expect("empty plan"),
        control.clone(),
    )
    .await;
    assert!(matches!(
        receiver.receive_command(one_over_limits, harness.blocking()).await,
        Err(LegacyLocalMaterializationError::Frame { .. })
    ));

    let mut invalid = ResponsePlan::command(response_head(81, 11)).expect("empty plan");
    invalid.head = invalid.head.set_body(Bytes::from_static(b"must remain bodyless"));
    let (receiver, _) = handoff(invalid, control).await;
    assert!(matches!(
        receiver.receive_command(limits(0, 0), harness.blocking()).await,
        Err(LegacyLocalMaterializationError::Frame { .. })
    ));

    harness.shutdown().await;
}

#[tokio::test]
async fn materialization_preserves_bound_head_metadata_and_never_decodes_frame_shaped_body_bytes() {
    let (harness, control) = ControlHarness::new("legacy-materializer-metadata", None);
    let typed_header = SendMessageResponseHeader::new(
        CheetahString::from("message-id"),
        17,
        91,
        Some(CheetahString::from("transaction-id")),
        None,
        None,
    );
    let mut head = response_head(-19, 999)
        .set_language(LanguageCode::CPP)
        .set_version(-31)
        .set_flag(1 | (1 << 9))
        .set_serialize_type(SerializeType::JSON)
        .set_remark("preserved remark")
        .set_command_custom_header(typed_header);
    head.add_ext_field("extension-key", "extension-value");
    let frame_shaped = Bytes::from_static(&[0, 0, 0, 2, 0x7f, 0xff]);
    let pointer = frame_shaped.as_ptr();
    let (receiver, _) = handoff(ResponsePlan::bytes(head, frame_shaped).expect("metadata plan"), control).await;

    let command = receiver
        .receive_command(limits(6, 1), harness.blocking())
        .await
        .expect("materialized command");
    assert_eq!(command.code(), -19);
    assert_eq!(command.opaque(), 91);
    assert_eq!(command.language(), LanguageCode::CPP);
    assert_eq!(command.version(), -31);
    assert_eq!(command.flag(), 1 | (1 << 9));
    assert_eq!(command.serialize_type(), SerializeType::JSON);
    assert_eq!(command.remark().map(CheetahString::as_str), Some("preserved remark"));
    assert_eq!(
        command
            .ext_fields()
            .and_then(|fields| fields.get("extension-key"))
            .map(CheetahString::as_str),
        Some("extension-value")
    );
    let header = command
        .read_custom_header_ref::<SendMessageResponseHeader>()
        .expect("typed header must remain attached");
    assert_eq!(header.msg_id().as_str(), "message-id");
    assert_eq!(command.body().expect("body").as_ptr(), pointer);
    assert_eq!(command.body().expect("body").as_ref(), &[0, 0, 0, 2, 0x7f, 0xff]);

    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn simultaneous_parent_cancellation_and_deadline_is_cancellation_first() {
    let deadline = RequestDeadline::after(Duration::from_secs(1));
    let (harness, control) = ControlHarness::new("legacy-materializer-stop-priority", Some(deadline));
    let (receiver, _) = handoff(
        ResponsePlan::command(response_head(82, 12)).expect("empty plan"),
        control,
    )
    .await;
    harness.parent.cancel();
    tokio::time::advance(Duration::from_secs(1)).await;

    assert!(matches!(
        receiver.receive_command(limits(0, 0), harness.blocking()).await,
        Err(LegacyLocalMaterializationError::Cancelled)
    ));

    harness.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn session_close_and_deadline_keep_distinct_direct_error_variants() {
    let (closed_harness, closed_control) = ControlHarness::new(
        "legacy-materializer-session-close",
        Some(RequestDeadline::after(Duration::from_secs(1))),
    );
    let (receiver, _) = handoff(
        ResponsePlan::command(response_head(83, 13)).expect("empty plan"),
        closed_control,
    )
    .await;
    closed_harness
        ._closed_tx
        .send(true)
        .expect("session close observer should remain open");
    assert!(matches!(
        receiver.receive_command(limits(0, 0), closed_harness.blocking()).await,
        Err(LegacyLocalMaterializationError::SessionClosed)
    ));
    closed_harness.shutdown().await;

    let (deadline_harness, deadline_control) = ControlHarness::new(
        "legacy-materializer-deadline",
        Some(RequestDeadline::after(Duration::from_secs(1))),
    );
    let (receiver, _) = handoff(
        ResponsePlan::command(response_head(84, 14)).expect("empty plan"),
        deadline_control,
    )
    .await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        receiver
            .receive_command(limits(0, 0), deadline_harness.blocking())
            .await,
        Err(LegacyLocalMaterializationError::DeadlineExceeded)
    ));
    deadline_harness.shutdown().await;
}

#[tokio::test]
async fn ordinary_plan_receive_keeps_file_regions_lazy_and_owned() {
    let (harness, control) = ControlHarness::new("legacy-materializer-ordinary-receive", None);
    let (regions, lease, accesses, drops) = counting_region(b"ordinary-v2-plan");
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    let (receiver, _) = handoff(
        ResponsePlan::file_regions(response_head(83, 13), regions).expect("file plan"),
        control,
    )
    .await;

    let plan = receiver.receive().await.expect("ordinary plan receive");
    assert_eq!(accesses.load(Ordering::SeqCst), 1);
    assert_eq!(plan.body_len(), 16);
    drop(plan);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    harness.shutdown().await;
}

#[test]
fn errors_are_typed_redacted_not_started_and_non_retryable() {
    let allocation = allocate_body(usize::MAX).expect_err("capacity overflow should fail deterministically");
    assert!(matches!(allocation, LegacyLocalMaterializationError::Allocation { .. }));
    assert_eq!(allocation.write_progress(), WriteProgress::NotStarted);
    assert!(!allocation.retryable());
    assert!(allocation
        .source()
        .and_then(|source| source.downcast_ref::<TryReserveError>())
        .is_some());

    for error in [
        LegacyLocalMaterializationError::Cancelled,
        LegacyLocalMaterializationError::SessionClosed,
        LegacyLocalMaterializationError::DeadlineExceeded,
    ] {
        assert_eq!(error.write_progress(), WriteProgress::NotStarted);
        assert!(!error.retryable());
        assert!(error.source().is_none());
    }

    let errors = [
        LegacyLocalMaterializationError::response(ResponseError::AlreadyCompleted {
            state: ResponseTerminalState::Completed,
        }),
        LegacyLocalMaterializationError::limits(RocketMQError::illegal_argument("secret-limit-source")),
        LegacyLocalMaterializationError::frame(RocketMQError::illegal_argument("secret-frame-source")),
        LegacyLocalMaterializationError::runtime(RuntimeError::InvalidConfig("secret-runtime-source".to_owned())),
        LegacyLocalMaterializationError::file_io(io::Error::new(io::ErrorKind::UnexpectedEof, "secret-file-source")),
    ];
    for error in errors {
        assert_eq!(error.write_progress(), WriteProgress::NotStarted);
        assert!(!error.retryable());
        let debug = format!("{error:?}");
        let display = error.to_string();
        assert!(!debug.contains("secret"));
        assert!(!display.contains("secret"));
        assert!(error.source().is_some());
    }
    let file_error =
        LegacyLocalMaterializationError::file_io(io::Error::new(io::ErrorKind::PermissionDenied, "hidden path"));
    assert!(format!("{file_error:?}").contains("PermissionDenied"));
    assert!(file_error.to_string().contains("PermissionDenied"));
}

#[path = "tests/executor.rs"]
mod executor;

#[path = "tests/file.rs"]
mod file;
