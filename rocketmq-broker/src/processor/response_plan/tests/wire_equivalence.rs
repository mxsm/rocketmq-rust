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

use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_protocol::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::LanguageCode;
use rocketmq_protocol::protocol::SerializeType;
use rocketmq_protocol::version::CURRENT_VERSION;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_store::DefaultMappedFile;
use rocketmq_store::GetMessageResult;
use rocketmq_store::MappedFile;
use rocketmq_store::SelectMappedBufferResult;
use rocketmq_transport::api::v1::FileTransferMode;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::TransportServerV2;
use rocketmq_transport::test_support::Connection;
use rocketmq_transport::test_support::TestChannelBuilder;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::processor::default_pull_message_result_handler::pull_bytes_wire_fixture_parts;
use crate::processor::default_pull_message_result_handler::pull_store_wire_fixture_parts;
use crate::processor::query_message_processor::query_wire_fixture_legacy_command;
use crate::processor::query_message_processor::query_wire_fixture_parts;
use crate::processor::query_message_processor::QueryWireFixtureKind;

use super::pop::attach_pop_response_header;
use super::pop::pop_heap_response_parts;
use super::pop::pop_segmented_response_parts;
use super::BrokerResponseParts;
use super::LegacyResponseDelivery;

const INGRESS_OPAQUE: i32 = 9_271;
const BUILDER_PLACEHOLDER_OPAQUE: i32 = -1;
const MAX_TEST_FRAME_BYTES: usize = 1024 * 1024;

enum FixtureBody {
    Empty,
    Bytes(Vec<u8>),
    Segments(Vec<Vec<u8>>),
    FileRegion { directory: PathBuf, body: Vec<u8> },
}

#[derive(Clone, Copy)]
enum FixtureBuilder {
    PullBytes,
    PullStore,
    PopHeap,
    PopSegments,
    Query,
    View,
}

struct ExpectedFrame {
    code: ResponseCode,
    remark: Option<&'static str>,
    ext_fields: &'static [(&'static str, &'static str)],
    body: &'static [u8],
}

struct SemanticFixture {
    label: &'static str,
    request_code: RequestCode,
    finalized_body_free_head: Option<RemotingCommand>,
    body: FixtureBody,
    builder: FixtureBuilder,
    expected: ExpectedFrame,
    builds: AtomicUsize,
}

enum LegacyFixtureProjection {
    Parts(BrokerResponseParts),
    Command(RemotingCommand),
}

impl SemanticFixture {
    fn new(
        label: &'static str,
        request_code: RequestCode,
        finalized_body_free_head: Option<RemotingCommand>,
        body: FixtureBody,
        builder: FixtureBuilder,
        expected: ExpectedFrame,
    ) -> Self {
        let finalized_body_free_head = finalized_body_free_head.map(|mut head| {
            head.set_opaque_mut(BUILDER_PLACEHOLDER_OPAQUE);
            head.make_custom_header_to_net();
            assert!(head.body().is_none());
            head
        });
        Self {
            label,
            request_code,
            finalized_body_free_head,
            body,
            builder,
            expected,
            builds: AtomicUsize::new(0),
        }
    }

    fn finalized_head(&self, opaque: i32) -> RocketMQResult<RemotingCommand> {
        let mut head = self.finalized_body_free_head.clone().ok_or_else(|| {
            RocketMQError::invariant_violated("fixture builder does not own a finalized response head")
        })?;
        head.set_opaque_mut(opaque);
        Ok(head)
    }

    fn pull_store_result(&self, build_index: usize) -> RocketMQResult<GetMessageResult> {
        let selections = match &self.body {
            FixtureBody::Segments(segments) => segments
                .iter()
                .enumerate()
                .map(|(index, segment)| {
                    SelectMappedBufferResult::from_bytes(index as u64, Bytes::copy_from_slice(segment))
                        .ok_or_else(|| RocketMQError::invariant_violated("Pull segment length is not representable"))
                })
                .collect::<RocketMQResult<Vec<_>>>()?,
            FixtureBody::FileRegion { directory, body } => {
                let path = directory.join(format!("{build_index:020}"));
                let mapped_file =
                    DefaultMappedFile::try_new(CheetahString::from(path.to_string_lossy().into_owned()), 64)
                        .map_err(|source| RocketMQError::internal("wire-equivalence-pull-mapped-file", source))?;
                if !mapped_file.append_message_bytes(body) {
                    return Err(RocketMQError::invariant_violated(
                        "Pull FileRegion fixture did not fit its mapped file",
                    ));
                }
                vec![mapped_file
                    .try_file_range_selection(0, body.len())
                    .map_err(|source| RocketMQError::internal("wire-equivalence-pull-file-range", source))?
                    .ok_or_else(|| RocketMQError::invariant_violated("Pull FileRegion fixture was not published"))?]
            }
            _ => {
                return Err(RocketMQError::invariant_violated(
                    "Pull store fixture requires segments or a FileRegion",
                ));
            }
        };
        let mut result = GetMessageResult::new_result_size(selections.len());
        for (index, selection) in selections.into_iter().enumerate() {
            result.add_message(selection, index as u64, 1);
        }
        Ok(result)
    }

    async fn build_parts(&self, head_opaque: i32) -> RocketMQResult<BrokerResponseParts> {
        let build_index = self.builds.fetch_add(1, Ordering::SeqCst);
        match (&self.builder, &self.body) {
            (FixtureBuilder::PullBytes, FixtureBody::Bytes(body)) => {
                pull_bytes_wire_fixture_parts(self.finalized_head(head_opaque)?, Bytes::copy_from_slice(body))
            }
            (FixtureBuilder::PullStore, FixtureBody::Segments(_) | FixtureBody::FileRegion { .. }) => {
                pull_store_wire_fixture_parts(self.finalized_head(head_opaque)?, self.pull_store_result(build_index)?)
            }
            (FixtureBuilder::PopHeap, FixtureBody::Bytes(body)) => {
                pop_heap_response_parts(self.finalized_head(head_opaque)?, Some(Bytes::copy_from_slice(body)))
            }
            (FixtureBuilder::PopSegments, FixtureBody::Segments(segments)) => pop_segmented_response_parts(
                self.finalized_head(head_opaque)?,
                segments.iter().map(|segment| Bytes::copy_from_slice(segment)).collect(),
            ),
            (FixtureBuilder::Query, FixtureBody::Bytes(body)) => {
                query_wire_fixture_parts(QueryWireFixtureKind::Query, Some(body)).await
            }
            (FixtureBuilder::Query, FixtureBody::Empty) => {
                query_wire_fixture_parts(QueryWireFixtureKind::Query, None).await
            }
            (FixtureBuilder::View, FixtureBody::Bytes(body)) => {
                query_wire_fixture_parts(QueryWireFixtureKind::View, Some(body)).await
            }
            (FixtureBuilder::View, FixtureBody::Empty) => {
                query_wire_fixture_parts(QueryWireFixtureKind::View, None).await
            }
            _ => Err(RocketMQError::invariant_violated(
                "wire equivalence fixture body does not match its builder",
            )),
        }
    }

    async fn build_legacy_projection(&self) -> RocketMQResult<LegacyFixtureProjection> {
        let query_body = match &self.body {
            FixtureBody::Bytes(body) => Some(body.as_slice()),
            FixtureBody::Empty => None,
            _ => None,
        };
        match self.builder {
            FixtureBuilder::Query => {
                self.builds.fetch_add(1, Ordering::SeqCst);
                query_wire_fixture_legacy_command(QueryWireFixtureKind::Query, query_body, INGRESS_OPAQUE)
                    .await
                    .map(LegacyFixtureProjection::Command)
            }
            FixtureBuilder::View => {
                self.builds.fetch_add(1, Ordering::SeqCst);
                query_wire_fixture_legacy_command(QueryWireFixtureKind::View, query_body, INGRESS_OPAQUE)
                    .await
                    .map(LegacyFixtureProjection::Command)
            }
            _ => self
                .build_parts(INGRESS_OPAQUE)
                .await
                .map(LegacyFixtureProjection::Parts),
        }
    }
}

#[derive(Clone)]
struct FixtureProcessor {
    fixture: Arc<SemanticFixture>,
}

impl RequestProcessorV2 for FixtureProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
        self.fixture
            .build_parts(BUILDER_PLACEHOLDER_OPAQUE)
            .await?
            .into_handler_outcome()
    }
}

async fn read_raw_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut length_prefix = [0_u8; 4];
    stream
        .read_exact(&mut length_prefix)
        .await
        .expect("read complete RocketMQ frame length prefix");
    let payload_len = i32::from_be_bytes(length_prefix);
    assert!(
        payload_len >= 4,
        "invalid RocketMQ test frame payload length {payload_len}"
    );
    let payload_len = usize::try_from(payload_len).expect("positive test frame length");
    assert!(payload_len <= MAX_TEST_FRAME_BYTES, "unexpectedly large test frame");

    let mut frame = vec![0_u8; payload_len + length_prefix.len()];
    frame[..length_prefix.len()].copy_from_slice(&length_prefix);
    stream
        .read_exact(&mut frame[length_prefix.len()..])
        .await
        .expect("read complete RocketMQ frame payload");
    frame
}

async fn capture_legacy_frame(owner: &RuntimeOwner, fixture: &SemanticFixture) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind V1 wire-equivalence listener");
    let address = listener.local_addr().expect("V1 wire-equivalence address");
    let mut peer = TcpStream::connect(address)
        .await
        .expect("connect V1 wire-equivalence peer");
    let (server_stream, _) = listener.accept().await.expect("accept V1 wire-equivalence peer");
    let channel_context = owner.root_context().component(format!("{}-v1-channel", fixture.label));
    let connection = Connection::new(server_stream)
        .with_file_region_io(channel_context.storage_io().clone(), FileTransferMode::Portable);
    let channel = TestChannelBuilder::new(connection, channel_context.task_group().clone())
        .addresses(address, address)
        .build()
        .expect("build V1 wire-equivalence channel");

    match fixture
        .build_legacy_projection()
        .await
        .expect("build V1 semantic response projection")
    {
        LegacyFixtureProjection::Parts(parts) => {
            match parts
                .deliver_legacy(&channel)
                .await
                .expect("deliver V1 semantic response")
            {
                LegacyResponseDelivery::Command(command) => channel
                    .send_command(command.set_opaque(INGRESS_OPAQUE))
                    .await
                    .expect("send V1 command response through the real channel"),
                LegacyResponseDelivery::Written => {}
            }
        }
        LegacyFixtureProjection::Command(command) => channel
            .send_command(command.set_opaque(INGRESS_OPAQUE))
            .await
            .expect("send the production V1 Query/View projection through the real channel"),
    }

    let frame = tokio::time::timeout(Duration::from_secs(2), read_raw_frame(&mut peer))
        .await
        .expect("V1 raw frame capture deadline");
    peer.shutdown().await.expect("half-close the raw V1 peer");
    let report = channel.close_with_report(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{}", report.to_json());
    drop(channel);
    let mut trailing = Vec::new();
    peer.read_to_end(&mut trailing)
        .await
        .expect("read the drained V1 socket to EOF");
    assert!(
        trailing.is_empty(),
        "{} V1 socket emitted trailing bytes",
        fixture.label
    );
    frame
}

async fn capture_v2_frame(owner: &RuntimeOwner, fixture: Arc<SemanticFixture>) -> Vec<u8> {
    let label = fixture.label;
    let request_code = fixture.request_code;
    let server = TransportServerV2::new(
        Arc::new(ServerConfig {
            bind_address: "127.0.0.1".to_owned(),
            listen_port: 0,
            ..ServerConfig::default()
        }),
        owner.root_context().component(format!("{label}-v2-server")),
        FixtureProcessor { fixture },
    );
    let runner = owner.root_context().component(format!("{label}-v2-runner"));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (startup_tx, startup_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    runner
        .spawn_service(format!("{label}-v2-run"), async move {
            let result = server
                .try_run_with_shutdown_report_and_startup(
                    async move {
                        let _ = shutdown_rx.await;
                    },
                    startup_tx,
                )
                .await;
            let _ = result_tx.send(result);
        })
        .expect("spawn owned V2 wire-equivalence server");

    let address = startup_rx
        .await
        .expect("V2 startup channel")
        .expect("V2 wire-equivalence server startup");
    let mut stream = TcpStream::connect(address)
        .await
        .expect("connect raw V2 wire-equivalence client");
    let mut request = RemotingCommand::create_remoting_command(request_code as i32).set_opaque(INGRESS_OPAQUE);
    let request_frame = request
        .encode_header_with_body_length(0)
        .expect("encode raw V2 wire-equivalence request");
    stream
        .write_all(&request_frame)
        .await
        .expect("write raw V2 wire-equivalence request");

    let frame = tokio::time::timeout(Duration::from_secs(2), read_raw_frame(&mut stream))
        .await
        .expect("V2 raw frame capture deadline");
    stream.shutdown().await.expect("shutdown raw V2 client");
    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(2), result_rx)
        .await
        .expect("V2 server shutdown deadline")
        .expect("V2 server result channel")
        .expect("V2 server shutdown report");
    assert!(report.is_healthy(), "{}", report.to_json());
    let mut trailing = Vec::new();
    stream
        .read_to_end(&mut trailing)
        .await
        .expect("read the drained V2 socket to EOF");
    assert!(trailing.is_empty(), "{label} V2 socket emitted trailing bytes");
    frame
}

fn assert_protocol_expectations(frame: &[u8], fixture: &SemanticFixture) {
    let announced = i32::from_be_bytes(frame[..4].try_into().expect("complete frame prefix"));
    assert_eq!(
        usize::try_from(announced).expect("positive announced frame length"),
        frame.len() - 4,
        "{} announced payload length",
        fixture.label
    );
    assert_eq!(
        SerializeType::JSON.get_code(),
        frame[4],
        "{} raw serialization marker",
        fixture.label
    );

    let mut encoded = BytesMut::from(frame);
    let command = RemotingCommand::decode(&mut encoded)
        .expect("decode captured response frame")
        .expect("captured response is one complete frame");
    assert!(encoded.is_empty(), "{} must contain exactly one frame", fixture.label);
    assert_eq!(
        fixture.expected.code as i32,
        command.code(),
        "{} response code",
        fixture.label
    );
    assert_eq!(
        INGRESS_OPAQUE,
        command.opaque(),
        "{} bound ingress opaque",
        fixture.label
    );
    assert_eq!(1, command.flag(), "{} response flag bits", fixture.label);
    assert!(command.is_response_type(), "{} response type", fixture.label);
    assert!(!command.is_oneway_rpc(), "{} cannot be one-way", fixture.label);
    assert_eq!(
        fixture.expected.remark,
        command.remark().map(|remark| remark.as_str()),
        "{} response remark",
        fixture.label
    );
    assert_eq!(LanguageCode::RUST, command.language(), "{} language", fixture.label);
    assert_eq!(CURRENT_VERSION as i32, command.version(), "{} version", fixture.label);
    assert_eq!(
        SerializeType::JSON,
        command.serialize_type(),
        "{} serialize type",
        fixture.label
    );

    let fields = command.ext_fields();
    assert_eq!(
        fixture.expected.ext_fields.len(),
        fields.map_or(0, |fields| fields.len()),
        "{} extension-field count",
        fixture.label
    );
    for &(key, value) in fixture.expected.ext_fields {
        assert_eq!(
            Some(value),
            fields.and_then(|fields| fields.get(key)).map(|value| value.as_str()),
            "{} extension field {key}",
            fixture.label
        );
    }
    assert_eq!(
        fixture.expected.body,
        command.body().map(Bytes::as_ref).unwrap_or_default(),
        "{} ordered response body",
        fixture.label
    );
}

fn finalized_pull_head() -> RemotingCommand {
    test_command_factory()
        .create_success_response_command_with_header(PullMessageResponseHeader {
            suggest_which_broker_id: 1,
            next_begin_offset: 11,
            min_offset: 3,
            max_offset: 29,
            offset_delta: Some(2),
            topic_sys_flag: Some(4),
            group_sys_flag: Some(8),
            forbidden_type: Some(0),
        })
        .set_remark("wire-equivalence-pull")
}

fn finalized_pop_head() -> RemotingCommand {
    attach_pop_response_header(
        test_command_factory()
            .create_success_response_command()
            .set_remark("wire-equivalence-pop"),
        PopMessageResponseHeader {
            pop_time: 101,
            invisible_time: 30_000,
            revive_qid: 7,
            rest_num: 13,
            start_offset_info: Some("0 1 2".into()),
            msg_offset_info: Some("3 4 5".into()),
            order_count_info: Some("6 7 8".into()),
        },
    )
}

fn test_command_factory() -> RemotingCommandFactory {
    RemotingCommandFactory::new(RemotingCommandDefaults::default())
}

const PULL_EXT_FIELDS: &[(&str, &str)] = &[
    ("suggestWhichBrokerId", "1"),
    ("nextBeginOffset", "11"),
    ("minOffset", "3"),
    ("maxOffset", "29"),
    ("offsetDelta", "2"),
    ("topicSysFlag", "4"),
    ("groupSysFlag", "8"),
    ("forbiddenType", "0"),
];
const POP_EXT_FIELDS: &[(&str, &str)] = &[
    ("popTime", "101"),
    ("invisibleTime", "30000"),
    ("reviveQid", "7"),
    ("restNum", "13"),
    ("startOffsetInfo", "0 1 2"),
    ("msgOffsetInfo", "3 4 5"),
    ("orderCountInfo", "6 7 8"),
];
const QUERY_SUCCESS_EXT_FIELDS: &[(&str, &str)] =
    &[("indexLastUpdateTimestamp", "23"), ("indexLastUpdatePhyoffset", "17")];
const QUERY_EMPTY_EXT_FIELDS: &[(&str, &str)] = &[("indexLastUpdateTimestamp", "0"), ("indexLastUpdatePhyoffset", "0")];

#[tokio::test]
async fn broker_special_paths_match_complete_v1_and_v2_wire_frames() {
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("broker-wire-equivalence"))
        .expect("wire-equivalence runtime owner");
    let directory = tempfile::tempdir().expect("wire-equivalence file-region directory");

    let fixtures = [
        Arc::new(SemanticFixture::new(
            "pull-heap-bytes",
            RequestCode::PullMessage,
            Some(finalized_pull_head()),
            FixtureBody::Bytes(b"pull-heap-body".to_vec()),
            FixtureBuilder::PullBytes,
            ExpectedFrame {
                code: ResponseCode::Success,
                remark: Some("wire-equivalence-pull"),
                ext_fields: PULL_EXT_FIELDS,
                body: b"pull-heap-body",
            },
        )),
        Arc::new(SemanticFixture::new(
            "pull-segments",
            RequestCode::PullMessage,
            Some(finalized_pull_head()),
            FixtureBody::Segments(vec![b"pull-segment-a".to_vec(), b"pull-segment-b".to_vec()]),
            FixtureBuilder::PullStore,
            ExpectedFrame {
                code: ResponseCode::Success,
                remark: Some("wire-equivalence-pull"),
                ext_fields: PULL_EXT_FIELDS,
                body: b"pull-segment-apull-segment-b",
            },
        )),
        Arc::new(SemanticFixture::new(
            "pull-file-region",
            RequestCode::PullMessage,
            Some(finalized_pull_head()),
            FixtureBody::FileRegion {
                directory: directory.path().to_owned(),
                body: b"pull-file-region-body".to_vec(),
            },
            FixtureBuilder::PullStore,
            ExpectedFrame {
                code: ResponseCode::Success,
                remark: Some("wire-equivalence-pull"),
                ext_fields: PULL_EXT_FIELDS,
                body: b"pull-file-region-body",
            },
        )),
        Arc::new(SemanticFixture::new(
            "pop-heap-bytes",
            RequestCode::PopMessage,
            Some(finalized_pop_head()),
            FixtureBody::Bytes(b"pop-heap-body".to_vec()),
            FixtureBuilder::PopHeap,
            ExpectedFrame {
                code: ResponseCode::Success,
                remark: Some("wire-equivalence-pop"),
                ext_fields: POP_EXT_FIELDS,
                body: b"pop-heap-body",
            },
        )),
        Arc::new(SemanticFixture::new(
            "pop-segments",
            RequestCode::PopMessage,
            Some(finalized_pop_head()),
            FixtureBody::Segments(vec![b"pop-segment-a".to_vec(), b"pop-segment-b".to_vec()]),
            FixtureBuilder::PopSegments,
            ExpectedFrame {
                code: ResponseCode::Success,
                remark: Some("wire-equivalence-pop"),
                ext_fields: POP_EXT_FIELDS,
                body: b"pop-segment-apop-segment-b",
            },
        )),
        Arc::new(SemanticFixture::new(
            "query-bytes",
            RequestCode::QueryMessage,
            None,
            FixtureBody::Bytes(b"query-result-body".to_vec()),
            FixtureBuilder::Query,
            ExpectedFrame {
                code: ResponseCode::Success,
                remark: None,
                ext_fields: QUERY_SUCCESS_EXT_FIELDS,
                body: b"query-result-body",
            },
        )),
        Arc::new(SemanticFixture::new(
            "query-empty-error",
            RequestCode::QueryMessage,
            None,
            FixtureBody::Empty,
            FixtureBuilder::Query,
            ExpectedFrame {
                code: ResponseCode::QueryNotFound,
                remark: Some("query message failed, no result returned"),
                ext_fields: QUERY_EMPTY_EXT_FIELDS,
                body: b"",
            },
        )),
        Arc::new(SemanticFixture::new(
            "view-bytes",
            RequestCode::ViewMessageById,
            None,
            FixtureBody::Bytes(b"view-result-body".to_vec()),
            FixtureBuilder::View,
            ExpectedFrame {
                code: ResponseCode::Success,
                remark: None,
                ext_fields: &[],
                body: b"view-result-body",
            },
        )),
        Arc::new(SemanticFixture::new(
            "view-empty-error",
            RequestCode::ViewMessageById,
            None,
            FixtureBody::Empty,
            FixtureBuilder::View,
            ExpectedFrame {
                code: ResponseCode::SystemError,
                remark: Some("can not find message by offset: 41"),
                ext_fields: &[],
                body: b"",
            },
        )),
    ];

    for fixture in fixtures {
        let legacy = capture_legacy_frame(&owner, fixture.as_ref()).await;
        let v2 = capture_v2_frame(&owner, Arc::clone(&fixture)).await;

        assert_eq!(legacy, v2, "{} must be raw-wire equivalent", fixture.label);
        assert_protocol_expectations(&legacy, fixture.as_ref());
        assert_protocol_expectations(&v2, fixture.as_ref());
        assert_eq!(
            2,
            fixture.builds.load(Ordering::SeqCst),
            "{} must build independent affine owners for V1 and V2",
            fixture.label
        );
    }

    let task_report = owner.shutdown_tasks().await;
    assert!(task_report.is_healthy(), "{}", task_report.to_json());
    let final_report = owner.shutdown_background();
    assert!(final_report.is_healthy(), "{}", final_report.to_json());
}
