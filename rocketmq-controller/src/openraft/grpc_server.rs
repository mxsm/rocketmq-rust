//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

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
        let vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("Missing vote"))?;

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
                let payload: crate::typ::ControllerRequest =
                    serde_json::from_slice(&e.payload).ok()?;
                Some(openraft::Entry {
                    log_id: crate::typ::LogId {
                        leader_id: crate::typ::Vote::new(log_id.leader_id, log_id.leader_id)
                            .leader_id,
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

    async fn vote(
        &self,
        request: Request<OpenRaftVoteRequest>,
    ) -> Result<Response<OpenRaftVoteResponse>, Status> {
        let req = request.into_inner();
        debug!("Received vote request");

        let vote = req
            .vote
            .ok_or_else(|| Status::invalid_argument("Missing vote"))?;

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
                last_log_id: resp
                    .last_log_id
                    .map(|id| crate::protobuf::openraft::OpenRaftLogId {
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
        _request: Request<tonic::Streaming<OpenRaftSnapshotRequest>>,
    ) -> Result<Response<OpenRaftSnapshotResponse>, Status> {
        // TODO: Implement snapshot streaming
        Err(Status::unimplemented("InstallSnapshot not yet implemented"))
    }
}
