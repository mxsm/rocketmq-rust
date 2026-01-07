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

use crate::protobuf::openraft::open_raft_service_server::OpenRaftService;
use crate::protobuf::openraft::OpenRaftAppendRequest;
use crate::protobuf::openraft::OpenRaftAppendResponse;
use crate::protobuf::openraft::OpenRaftSnapshotRequest;
use crate::protobuf::openraft::OpenRaftSnapshotResponse;
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

        let raft_vote = openraft::Vote::new(vote.term, vote.node_id);

        let prev_log_id = req.prev_log_id.map(|id| crate::typ::LogId {
            leader_id: crate::typ::Vote::new(id.leader_id, id.leader_id).leader_id,
            index: id.index,
        });

        let entries: Vec<_> = req
            .entries
            .into_iter()
            .filter_map(|e| {
                let log_id = e.log_id?;
                let payload: crate::typ::ControllerRequest = serde_json::from_slice(&e.payload).ok()?;
                Some(openraft::Entry {
                    log_id: crate::typ::LogId {
                        leader_id: crate::typ::Vote::new(log_id.leader_id, log_id.leader_id).leader_id,
                        index: log_id.index,
                    },
                    payload: openraft::EntryPayload::Normal(payload),
                })
            })
            .collect();

        let leader_commit = req.leader_commit.map(|id| crate::typ::LogId {
            leader_id: crate::typ::Vote::new(id.leader_id, id.leader_id).leader_id,
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
            Ok(resp) => {
                let success = matches!(resp, openraft::raft::AppendEntriesResponse::Success);
                Ok(Response::new(OpenRaftAppendResponse {
                    vote: Some(crate::protobuf::openraft::OpenRaftVote {
                        term: raft_vote.leader_id.term,
                        node_id: raft_vote.leader_id.node_id,
                        committed: raft_vote.committed,
                    }),
                    success,
                    conflict: None,
                }))
            }
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

        let raft_vote = openraft::Vote::new(vote.term, vote.node_id);

        let last_log_id = req.last_log_id.map(|id| crate::typ::LogId {
            leader_id: crate::typ::Vote::new(id.leader_id, id.leader_id).leader_id,
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
                    leader_id: id.leader_id.node_id,
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

        let mut vote: Option<crate::typ::Vote> = None;
        let mut snapshot_meta: Option<openraft::SnapshotMeta<crate::typ::TypeConfig>> = None;
        let mut snapshot_data = Vec::new();

        // Receive all chunks
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

            // Extract vote and meta from first chunk
            if vote.is_none() {
                if let Some(v) = chunk.vote {
                    vote = Some(openraft::Vote::new(v.term, v.node_id));
                }
            }

            if snapshot_meta.is_none() {
                if let Some(meta) = chunk.meta {
                    let last_membership: openraft::StoredMembership<crate::typ::TypeConfig> =
                        serde_json::from_slice(&meta.last_membership).map_err(|e| {
                            Status::invalid_argument(format!("Failed to deserialize membership: {}", e))
                        })?;

                    snapshot_meta = Some(openraft::SnapshotMeta {
                        last_log_id: meta.last_log_id.map(|id| crate::typ::LogId {
                            leader_id: crate::typ::Vote::new(id.leader_id, id.leader_id).leader_id,
                            index: id.index,
                        }),
                        last_membership,
                        snapshot_id: meta.snapshot_id,
                    });
                }
            }

            // Append data chunk
            snapshot_data.extend_from_slice(&chunk.data);

            if chunk.done {
                break;
            }
        }

        let vote = vote.ok_or_else(|| Status::invalid_argument("Missing vote"))?;
        let meta = snapshot_meta.ok_or_else(|| Status::invalid_argument("Missing snapshot meta"))?;

        debug!(
            "Received snapshot: size={} bytes, last_log_id={:?}",
            snapshot_data.len(),
            meta.last_log_id
        );

        // Create snapshot with Cursor wrapper
        let snapshot = openraft::Snapshot {
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
