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

//! gRPC server implementation for OpenRaft network communication

use std::error::Error as StdError;
use std::sync::Arc;

use openraft::raft::AppendError;
use openraft::raft::AppendResult;
use rocketmq_error::Error;
use rocketmq_error::PublicErrorView;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::error;

use rocketmq_model::utils::crc32_utils::crc32;

use crate::error::controller_internal_by;
use crate::error::request_invalid_by;
use crate::openraft::SNAPSHOT_CHUNK_BYTES;
use crate::openraft::SNAPSHOT_MAX_BYTES;
use crate::protobuf::openraft::open_raft_service_server::OpenRaftService;
use crate::protobuf::openraft::OpenRaftAppendRequest;
use crate::protobuf::openraft::OpenRaftAppendResponse;
use crate::protobuf::openraft::OpenRaftLogId as ProtoLogId;
use crate::protobuf::openraft::OpenRaftSnapshotRequest;
use crate::protobuf::openraft::OpenRaftSnapshotResponse;
use crate::protobuf::openraft::OpenRaftVote;
use crate::protobuf::openraft::OpenRaftVoteRequest;
use crate::protobuf::openraft::OpenRaftVoteResponse;
use crate::typ::Raft;
use crate::typ::TypeConfig;

fn grpc_request_invalid_error(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    request_invalid_by(operation, source)
}

fn grpc_internal_error(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    controller_internal_by(operation, source)
}

fn invalid_argument_status(error: &Error) -> Status {
    let view = error
        .public_view()
        .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
    Status::invalid_argument(view.message())
}

fn internal_status(error: &Error) -> Status {
    let view = error
        .public_view()
        .unwrap_or_else(|_| PublicErrorView::descriptor_only(error.descriptor()));
    Status::internal(view.message())
}

fn request_invalid_status(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Status {
    let error = grpc_request_invalid_error(operation, source);
    error!(operation, error = ?error, "OpenRaft gRPC request decoding failed");
    invalid_argument_status(&error)
}

fn operation_internal_status(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Status {
    let error = grpc_internal_error(operation, source);
    error!(operation, error = ?error, "OpenRaft gRPC operation failed");
    internal_status(&error)
}

/// gRPC service implementation for OpenRaft
pub struct GrpcRaftService {
    /// Raft instance
    raft: Arc<Raft>,
}

impl GrpcRaftService {
    /// Create a new gRPC Raft service
    pub fn new(raft: Arc<Raft>) -> Self {
        Self { raft }
    }
}

fn decode_vote(vote: OpenRaftVote) -> crate::typ::Vote {
    if vote.committed {
        crate::typ::Vote::new_committed(vote.term, vote.node_id)
    } else {
        crate::typ::Vote::new(vote.term, vote.node_id)
    }
}

fn encode_vote(vote: crate::typ::Vote) -> OpenRaftVote {
    OpenRaftVote {
        term: vote.leader_id.term,
        node_id: vote.leader_id.node_id,
        committed: vote.committed,
    }
}

fn encode_log_id(log_id: crate::typ::LogId) -> ProtoLogId {
    ProtoLogId {
        term: log_id.leader_id.term,
        node_id: log_id.leader_id.node_id,
        index: log_id.index,
    }
}

/// Decodes a vote request.
///
/// A peer that sends only the legacy `leadership_transfer` flag cannot name the authorizing
/// Leader, so its request is handled as an ordinary election that respects the leader lease.
fn decode_vote_request(req: OpenRaftVoteRequest) -> Result<openraft::raft::VoteRequest<TypeConfig>, Status> {
    let vote = req.vote.ok_or_else(|| Status::invalid_argument("Missing vote"))?;
    let last_log_id = req.last_log_id.map(|id| crate::typ::LogId {
        leader_id: crate::typ::Vote::new(id.term, id.node_id).leader_id,
        index: id.index,
    });
    Ok(openraft::raft::VoteRequest {
        vote: decode_vote(vote),
        last_log_id,
        leadership_transfer_from: req.leadership_transfer_from.map(decode_vote),
    })
}

/// Encodes a follower's AppendEntries result in the existing wire format.
///
/// An accepted request is reported as plain success: the Leader acknowledges the last log id of
/// the request it sent, which is what the follower accepted. The format carries no conflict hint.
fn encode_append_result(request_vote: crate::typ::Vote, result: AppendResult<TypeConfig>) -> OpenRaftAppendResponse {
    match result {
        Ok(_) => OpenRaftAppendResponse {
            vote: Some(encode_vote(request_vote)),
            success: true,
            conflict: None,
            partial_success: None,
            higher_vote: None,
        },
        Err(AppendError::Conflict { log_id, hint: _ }) => OpenRaftAppendResponse {
            vote: Some(encode_vote(request_vote)),
            success: false,
            conflict: Some(encode_log_id(log_id)),
            partial_success: None,
            higher_vote: None,
        },
        Err(AppendError::HigherVote(vote)) => OpenRaftAppendResponse {
            vote: Some(encode_vote(vote)),
            success: false,
            conflict: None,
            partial_success: None,
            higher_vote: Some(encode_vote(vote)),
        },
    }
}

struct SnapshotStreamAssembler {
    vote: Option<crate::typ::Vote>,
    meta: Option<crate::typ::SnapshotMeta>,
    snapshot_id: Option<String>,
    data: Vec<u8>,
    expected_offset: u64,
    expected_total_size: Option<u64>,
    expected_checksum: Option<u32>,
    completed: bool,
}

impl SnapshotStreamAssembler {
    fn new() -> Self {
        Self {
            vote: None,
            meta: None,
            snapshot_id: None,
            data: Vec::new(),
            expected_offset: 0,
            expected_total_size: None,
            expected_checksum: None,
            completed: false,
        }
    }

    fn push(&mut self, chunk: OpenRaftSnapshotRequest) -> Result<(), Status> {
        if self.completed {
            return Err(Status::invalid_argument(
                "Snapshot stream contains data after the final chunk",
            ));
        }
        if chunk.data.len() > SNAPSHOT_CHUNK_BYTES {
            return Err(Status::resource_exhausted(format!(
                "Snapshot chunk size {} exceeds the {} byte limit",
                chunk.data.len(),
                SNAPSHOT_CHUNK_BYTES
            )));
        }
        if chunk.offset != self.expected_offset {
            return Err(Status::invalid_argument(format!(
                "Snapshot chunk offset is not contiguous: expected {}, received {}",
                self.expected_offset, chunk.offset
            )));
        }

        let first_chunk = self.expected_total_size.is_none();
        if first_chunk {
            let total_size = usize::try_from(chunk.total_size)
                .map_err(|_| Status::resource_exhausted("Snapshot size does not fit this platform"))?;
            if total_size > SNAPSHOT_MAX_BYTES {
                return Err(Status::resource_exhausted(format!(
                    "Snapshot size {} exceeds the {} byte limit",
                    chunk.total_size, SNAPSHOT_MAX_BYTES
                )));
            }
            self.expected_total_size = Some(chunk.total_size);
            self.expected_checksum = Some(chunk.checksum);
            self.data.reserve(total_size);
            self.vote =
                Some(decode_vote(chunk.vote.ok_or_else(|| {
                    Status::invalid_argument("First snapshot chunk is missing vote")
                })?));
            let meta = chunk
                .meta
                .ok_or_else(|| Status::invalid_argument("First snapshot chunk is missing metadata"))?;
            let last_membership: crate::typ::StoredMembership = serde_json::from_slice(&meta.last_membership)
                .map_err(|source| request_invalid_status("decode snapshot membership", source))?;
            self.snapshot_id = Some(meta.snapshot_id);
            self.meta = Some(crate::typ::SnapshotMeta {
                last_log_id: meta.last_log_id.map(|id| crate::typ::LogId {
                    leader_id: crate::typ::Vote::new(id.term, id.node_id).leader_id,
                    index: id.index,
                }),
                last_membership,
            });
        } else {
            if chunk.total_size != self.expected_total_size.unwrap_or_default()
                || chunk.checksum != self.expected_checksum.unwrap_or_default()
            {
                return Err(Status::invalid_argument(
                    "Snapshot total size or checksum changed between chunks",
                ));
            }
            if chunk.vote.is_some() || chunk.meta.is_some() {
                return Err(Status::invalid_argument(
                    "Snapshot vote and metadata may only appear in the first chunk",
                ));
            }
        }

        self.expected_offset = self
            .expected_offset
            .checked_add(chunk.data.len() as u64)
            .ok_or_else(|| Status::resource_exhausted("Snapshot offset overflow"))?;
        let received_size = usize::try_from(self.expected_offset)
            .map_err(|_| Status::resource_exhausted("Snapshot offset does not fit this platform"))?;
        if self.expected_offset > self.expected_total_size.unwrap_or_default() || received_size > SNAPSHOT_MAX_BYTES {
            return Err(Status::resource_exhausted(
                "Snapshot payload exceeds its declared or configured size",
            ));
        }
        self.data.extend_from_slice(&chunk.data);
        self.completed = chunk.done;
        Ok(())
    }

    fn finish_snapshot(self) -> Result<(crate::typ::Vote, crate::typ::SnapshotMeta, Vec<u8>), Status> {
        let snapshot_id = self.snapshot_id.clone();
        let (vote, meta, data) = self.finish()?;
        let identity = crate::openraft::inspect_snapshot_payload(&data)
            .map_err(|source| request_invalid_status("validate snapshot payload", source))?;
        if snapshot_id.as_ref() != Some(&identity.snapshot_id) {
            return Err(Status::invalid_argument(
                "Snapshot ID does not match its checksummed payload",
            ));
        }
        Ok((vote, meta, data))
    }

    fn finish(self) -> Result<(crate::typ::Vote, crate::typ::SnapshotMeta, Vec<u8>), Status> {
        if !self.completed {
            return Err(Status::invalid_argument("Snapshot stream ended before the final chunk"));
        }
        let expected_total_size = self.expected_total_size.unwrap_or_default();
        if self.expected_offset != expected_total_size {
            return Err(Status::invalid_argument(format!(
                "Snapshot size mismatch: declared {}, received {}",
                expected_total_size, self.expected_offset
            )));
        }
        let expected_checksum = self.expected_checksum.unwrap_or_default();
        let actual_checksum = crc32(&self.data);
        if actual_checksum != expected_checksum {
            return Err(Status::data_loss(format!(
                "Snapshot checksum mismatch: expected {}, calculated {}",
                expected_checksum, actual_checksum
            )));
        }
        Ok((
            self.vote.ok_or_else(|| Status::invalid_argument("Missing vote"))?,
            self.meta
                .ok_or_else(|| Status::invalid_argument("Missing snapshot meta"))?,
            self.data,
        ))
    }
}

#[tonic::async_trait]
impl OpenRaftService for GrpcRaftService {
    async fn append_entries(
        &self,
        request: Request<OpenRaftAppendRequest>,
    ) -> Result<Response<OpenRaftAppendResponse>, Status> {
        let req = request.into_inner();
        debug!("Received append_entries: {} entries", req.entries.len());

        // Convert protobuf to OpenRaft types
        let vote = req.vote.ok_or_else(|| Status::invalid_argument("Missing vote"))?;
        let raft_vote = decode_vote(vote);

        let prev_log_id = req.prev_log_id.map(|id| crate::typ::LogId {
            leader_id: crate::typ::Vote::new(id.term, id.node_id).leader_id,
            index: id.index,
        });

        let entries = req
            .entries
            .into_iter()
            .map(|entry| {
                let log_id = entry
                    .log_id
                    .ok_or_else(|| Status::invalid_argument("Missing append entry log id"))?;
                let payload: crate::typ::EntryPayload = serde_json::from_slice(&entry.payload)
                    .map_err(|source| request_invalid_status("decode append entry payload", source))?;
                Ok(openraft::Entry {
                    log_id: crate::typ::LogId {
                        leader_id: crate::typ::Vote::new(log_id.term, log_id.node_id).leader_id,
                        index: log_id.index,
                    },
                    payload,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        let leader_commit = req.leader_commit.map(|id| crate::typ::LogId {
            leader_id: crate::typ::Vote::new(id.term, id.node_id).leader_id,
            index: id.index,
        });

        let append_req = openraft::raft::AppendEntriesRequest {
            vote: raft_vote,
            prev_log_id,
            entries,
            leader_commit,
        };

        // Call Raft
        match self.raft.append_entries(append_req).await {
            Ok(result) => Ok(Response::new(encode_append_result(raft_vote, result))),
            Err(e) => {
                error!("AppendEntries failed: {}", e);
                Err(operation_internal_status("append Raft entries", e))
            }
        }
    }

    async fn vote(&self, request: Request<OpenRaftVoteRequest>) -> Result<Response<OpenRaftVoteResponse>, Status> {
        debug!("Received vote request");
        let vote_req = decode_vote_request(request.into_inner())?;

        match self.raft.vote(vote_req).await {
            Ok(resp) => Ok(Response::new(OpenRaftVoteResponse {
                vote: Some(crate::protobuf::openraft::OpenRaftVote {
                    term: resp.vote.leader_id.term,
                    node_id: resp.vote.leader_id.node_id,
                    committed: resp.vote.committed,
                }),
                vote_granted: resp.vote_granted,
                last_log_id: resp.last_log_id.map(|id| crate::protobuf::openraft::OpenRaftLogId {
                    term: id.leader_id.term,
                    node_id: id.leader_id.node_id,
                    index: id.index,
                }),
            })),
            Err(e) => {
                error!("Vote failed: {}", e);
                Err(operation_internal_status("process Raft vote", e))
            }
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<tonic::Streaming<OpenRaftSnapshotRequest>>,
    ) -> Result<Response<OpenRaftSnapshotResponse>, Status> {
        use tokio_stream::StreamExt;

        let mut stream = request.into_inner();
        let mut assembler = SnapshotStreamAssembler::new();

        while let Some(chunk_result) = stream.next().await {
            let chunk =
                chunk_result.map_err(|source| operation_internal_status("receive Raft snapshot stream", source))?;
            assembler.push(chunk)?;
        }
        let (vote, meta, snapshot_data) = assembler.finish_snapshot()?;

        debug!(
            "Received snapshot: size={} bytes, last_log_id={:?}",
            snapshot_data.len(),
            meta.last_log_id
        );

        // Create snapshot with Cursor wrapper
        let snapshot = crate::typ::Snapshot {
            meta,
            snapshot: std::io::Cursor::new(snapshot_data),
        };

        // Install snapshot through Raft using the new API
        match self.raft.install_full_snapshot(vote, snapshot).await {
            Ok(resp) => Ok(Response::new(OpenRaftSnapshotResponse {
                vote: Some(crate::protobuf::openraft::OpenRaftVote {
                    term: resp.vote.leader_id().term,
                    node_id: resp.vote.leader_id().node_id,
                    committed: resp.vote.is_committed(),
                }),
            })),
            Err(e) => {
                error!("InstallSnapshot failed: {}", e);
                Err(operation_internal_status("install Raft snapshot", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::io;

    use super::*;
    use crate::protobuf::openraft::OpenRaftSnapshotMeta;

    fn first_chunk(data: Vec<u8>, total_size: u64, checksum: u32, done: bool) -> OpenRaftSnapshotRequest {
        OpenRaftSnapshotRequest {
            vote: Some(OpenRaftVote {
                term: 1,
                node_id: 1,
                committed: true,
            }),
            meta: Some(OpenRaftSnapshotMeta {
                last_log_id: None,
                snapshot_id: "snapshot-1".to_string(),
                last_membership: serde_json::to_vec(&crate::typ::StoredMembership::default())
                    .expect("serialize membership"),
            }),
            offset: 0,
            data,
            done,
            total_size,
            checksum,
        }
    }

    #[test]
    fn grpc_projection_keeps_typed_sources_internal_and_public_messages_fixed() {
        let request_error = grpc_request_invalid_error(
            "decode test request",
            io::Error::new(io::ErrorKind::InvalidData, "request-source-canary"),
        );
        assert!(request_error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        let request_status = invalid_argument_status(&request_error);
        assert_eq!(request_status.code(), tonic::Code::InvalidArgument);
        assert_eq!(request_status.message(), "Controller request is invalid");
        assert!(!request_status.message().contains("request-source-canary"));

        let internal_error = grpc_internal_error(
            "run test operation",
            io::Error::new(io::ErrorKind::BrokenPipe, "internal-source-canary"),
        );
        assert!(internal_error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some());
        let internal_status = internal_status(&internal_error);
        assert_eq!(internal_status.code(), tonic::Code::Internal);
        assert_eq!(internal_status.message(), "Controller operation failed");
        assert!(!internal_status.message().contains("internal-source-canary"));
    }

    #[tokio::test]
    async fn snapshot_stream_preserves_and_checks_payload_identity() {
        use openraft::storage::RaftSnapshotBuilder;

        let config = crate::ControllerConfigReader::new(crate::ControllerConfig::default());
        let mut state = crate::openraft::StateMachine::new(config);
        let snapshot = state.build_snapshot().await.expect("build snapshot");
        let data = snapshot.snapshot.into_inner();
        let identity = crate::openraft::inspect_snapshot_payload(&data).expect("snapshot identity");

        for matches in [true, false] {
            let mut chunk = first_chunk(data.clone(), data.len() as u64, crc32(&data), true);
            chunk.meta.as_mut().unwrap().snapshot_id = if matches {
                identity.snapshot_id.clone()
            } else {
                "different-snapshot".to_string()
            };
            let mut assembler = SnapshotStreamAssembler::new();
            assembler.push(chunk).expect("accept bounded chunk");
            let result = assembler.finish_snapshot();
            if matches {
                assert_eq!(result.expect("matching identity").2, data);
            } else {
                assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
            }
        }
    }

    fn continuation_chunk(
        offset: u64,
        data: Vec<u8>,
        total_size: u64,
        checksum: u32,
        done: bool,
    ) -> OpenRaftSnapshotRequest {
        OpenRaftSnapshotRequest {
            vote: None,
            meta: None,
            offset,
            data,
            done,
            total_size,
            checksum,
        }
    }

    #[test]
    fn snapshot_stream_accepts_contiguous_checksummed_chunks() {
        let payload = b"controller snapshot".to_vec();
        let checksum = crc32(&payload);
        let mut assembler = SnapshotStreamAssembler::new();
        assembler
            .push(first_chunk(
                payload[..8].to_vec(),
                payload.len() as u64,
                checksum,
                false,
            ))
            .expect("first chunk");
        assembler
            .push(continuation_chunk(
                8,
                payload[8..].to_vec(),
                payload.len() as u64,
                checksum,
                true,
            ))
            .expect("final chunk");

        let (_, _, assembled) = assembler.finish().expect("complete snapshot");
        assert_eq!(assembled, payload);
    }

    #[test]
    fn snapshot_stream_rejects_offset_gap() {
        let payload = b"snapshot".to_vec();
        let checksum = crc32(&payload);
        let mut assembler = SnapshotStreamAssembler::new();
        assembler
            .push(first_chunk(
                payload[..4].to_vec(),
                payload.len() as u64,
                checksum,
                false,
            ))
            .expect("first chunk");

        let error = assembler
            .push(continuation_chunk(
                5,
                payload[4..].to_vec(),
                payload.len() as u64,
                checksum,
                true,
            ))
            .expect_err("offset gap must fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn snapshot_stream_rejects_corrupted_payload() {
        let payload = b"snapshot".to_vec();
        let mut assembler = SnapshotStreamAssembler::new();
        assembler
            .push(first_chunk(
                payload.clone(),
                payload.len() as u64,
                crc32(b"different"),
                true,
            ))
            .expect("chunk framing");

        let error = assembler.finish().expect_err("checksum mismatch must fail");
        assert_eq!(error.code(), tonic::Code::DataLoss);
    }

    #[test]
    fn snapshot_stream_rejects_declared_oversize_before_allocating() {
        let mut assembler = SnapshotStreamAssembler::new();
        let error = assembler
            .push(first_chunk(Vec::new(), (SNAPSHOT_MAX_BYTES + 1) as u64, 0, true))
            .expect_err("oversized snapshot must fail");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(assembler.data.is_empty());
    }

    fn vote_request(leadership_transfer: bool, from: Option<OpenRaftVote>) -> OpenRaftVoteRequest {
        OpenRaftVoteRequest {
            vote: Some(encode_vote(crate::typ::Vote::new(2, 2))),
            last_log_id: Some(ProtoLogId {
                term: 1,
                node_id: 1,
                index: 7,
            }),
            leadership_transfer,
            leadership_transfer_from: from,
        }
    }

    #[test]
    fn vote_request_uses_only_the_authorizing_leader_vote() {
        let leader = crate::typ::Vote::new_committed(1, 1);
        let transfer = decode_vote_request(vote_request(true, Some(encode_vote(leader)))).expect("transfer vote");
        assert_eq!(transfer.vote, crate::typ::Vote::new(2, 2));
        assert_eq!(transfer.last_log_id.map(|id| id.index), Some(7));
        assert_eq!(transfer.leadership_transfer_from, Some(leader));

        let legacy = decode_vote_request(vote_request(true, None)).expect("legacy vote");
        assert_eq!(legacy.leadership_transfer_from, None);

        let mut missing_vote = vote_request(false, None);
        missing_vote.vote = None;
        let error = decode_vote_request(missing_vote).expect_err("missing vote must fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn append_result_keeps_the_existing_wire_format() {
        let request_vote = crate::typ::Vote::new_committed(1, 1);
        let log_id = crate::typ::LogId {
            leader_id: crate::typ::Vote::new(1, 1).leader_id,
            index: 3,
        };

        let accepted = encode_append_result(request_vote, Ok(Some(log_id)));
        assert!(accepted.success);
        assert_eq!(accepted.vote, Some(encode_vote(request_vote)));
        assert_eq!(
            (accepted.conflict, accepted.partial_success, accepted.higher_vote),
            (None, None, None)
        );

        let conflict = encode_append_result(request_vote, Err(AppendError::Conflict { log_id, hint: None }));
        assert!(!conflict.success);
        assert_eq!(conflict.conflict, Some(encode_log_id(log_id)));
        assert_eq!(conflict.higher_vote, None);

        let higher = crate::typ::Vote::new(2, 3);
        let rejected = encode_append_result(request_vote, Err(AppendError::HigherVote(higher)));
        assert!(!rejected.success);
        assert_eq!(rejected.vote, Some(encode_vote(higher)));
        assert_eq!(rejected.higher_vote, Some(encode_vote(higher)));
    }
}
