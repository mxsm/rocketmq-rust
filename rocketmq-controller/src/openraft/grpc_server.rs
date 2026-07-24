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

use std::sync::Arc;

use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::error;

use rocketmq_common::utils::crc32_utils::crc32;

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

struct SnapshotStreamAssembler {
    vote: Option<crate::typ::Vote>,
    meta: Option<crate::typ::SnapshotMeta>,
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
            let last_membership: crate::typ::StoredMembership =
                serde_json::from_slice(&meta.last_membership).map_err(|error| {
                    Status::invalid_argument(format!("Failed to deserialize snapshot membership: {error}"))
                })?;
            self.meta = Some(crate::typ::SnapshotMeta {
                last_log_id: meta.last_log_id.map(|id| crate::typ::LogId {
                    leader_id: crate::typ::Vote::new(id.term, id.node_id).leader_id,
                    index: id.index,
                }),
                last_membership,
                snapshot_id: meta.snapshot_id,
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
        let response_conflict_log_id = prev_log_id;

        let entries = req
            .entries
            .into_iter()
            .map(|entry| {
                let log_id = entry
                    .log_id
                    .ok_or_else(|| Status::invalid_argument("Missing append entry log id"))?;
                let payload: crate::typ::EntryPayload = serde_json::from_slice(&entry.payload).map_err(|e| {
                    Status::invalid_argument(format!("Failed to deserialize append entry payload: {}", e))
                })?;
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
            Ok(resp) => Ok(Response::new(match resp {
                openraft::raft::AppendEntriesResponse::Success => OpenRaftAppendResponse {
                    vote: Some(encode_vote(raft_vote)),
                    success: true,
                    conflict: None,
                    partial_success: None,
                    higher_vote: None,
                },
                openraft::raft::AppendEntriesResponse::PartialSuccess(log_id) => OpenRaftAppendResponse {
                    vote: Some(encode_vote(raft_vote)),
                    success: false,
                    conflict: None,
                    partial_success: log_id.map(encode_log_id),
                    higher_vote: None,
                },
                openraft::raft::AppendEntriesResponse::Conflict => OpenRaftAppendResponse {
                    vote: Some(encode_vote(raft_vote)),
                    success: false,
                    conflict: response_conflict_log_id.map(encode_log_id),
                    partial_success: None,
                    higher_vote: None,
                },
                openraft::raft::AppendEntriesResponse::HigherVote(vote) => OpenRaftAppendResponse {
                    vote: Some(encode_vote(vote)),
                    success: false,
                    conflict: None,
                    partial_success: None,
                    higher_vote: Some(encode_vote(vote)),
                },
            })),
            Err(e) => {
                error!("AppendEntries failed: {}", e);
                Err(Status::internal(format!("Raft error: {}", e)))
            }
        }
    }

    async fn vote(&self, request: Request<OpenRaftVoteRequest>) -> Result<Response<OpenRaftVoteResponse>, Status> {
        let req = request.into_inner();
        debug!("Received vote request");

        let vote = req.vote.ok_or_else(|| Status::invalid_argument("Missing vote"))?;
        let raft_vote = decode_vote(vote);

        let last_log_id = req.last_log_id.map(|id| crate::typ::LogId {
            leader_id: crate::typ::Vote::new(id.term, id.node_id).leader_id,
            index: id.index,
        });

        let vote_req = openraft::raft::VoteRequest {
            vote: raft_vote,
            last_log_id,
        };

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
                Err(Status::internal(format!("Raft error: {}", e)))
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
            let chunk = chunk_result.map_err(|e| Status::internal(format!("Stream error: {}", e)))?;
            assembler.push(chunk)?;
        }
        let (vote, meta, snapshot_data) = assembler.finish()?;

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
                Err(Status::internal(format!("Raft error: {}", e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
}
