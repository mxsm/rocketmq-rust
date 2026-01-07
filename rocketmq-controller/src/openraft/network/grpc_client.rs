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

//! gRPC client implementation for OpenRaft network communication

use std::future::Future;
use std::time::Duration;

use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::v2::RaftNetworkV2;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::OptionalSend;
use openraft::Snapshot;
use openraft::Vote;
use tonic::transport::Channel;
use tonic::Request;
use tracing::debug;
use tracing::error;

use crate::protobuf::openraft::open_raft_service_client::OpenRaftServiceClient;
use crate::protobuf::openraft::OpenRaftAppendRequest;
use crate::protobuf::openraft::OpenRaftVoteRequest as ProtoVoteRequest;
use crate::typ::NodeId;
use crate::typ::TypeConfig;

/// gRPC-based network client for OpenRaft
#[derive(Clone)]
pub struct GrpcNetworkClient {
    /// Target node ID
    target: NodeId,

    /// Target address
    target_addr: String,

    /// gRPC client
    client: Option<OpenRaftServiceClient<Channel>>,

    /// Connection timeout
    timeout: Duration,
}

impl GrpcNetworkClient {
    /// Create a new gRPC network client
    pub fn new(target: NodeId, target_addr: String) -> Self {
        Self {
            target,
            target_addr,
            client: None,
            timeout: Duration::from_secs(5),
        }
    }

    /// Connect to the target node
    async fn connect(&mut self) -> Result<(), NetworkError<TypeConfig>> {
        if self.client.is_some() {
            return Ok(());
        }

        debug!("Connecting to node {} at {}", self.target, self.target_addr);

        let endpoint = format!("http://{}", self.target_addr);
        let channel = Channel::from_shared(endpoint)
            .map_err(|e| NetworkError::new(&std::io::Error::new(std::io::ErrorKind::InvalidInput, e)))?
            .timeout(self.timeout)
            .connect()
            .await
            .map_err(|e| {
                error!("Failed to connect to {}: {}", self.target_addr, e);
                NetworkError::new(&std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))
            })?;

        self.client = Some(OpenRaftServiceClient::new(channel));
        debug!("Connected to node {} at {}", self.target, self.target_addr);
        Ok(())
    }

    /// Get or create the client
    async fn get_client(&mut self) -> Result<&mut OpenRaftServiceClient<Channel>, NetworkError<TypeConfig>> {
        self.connect().await?;
        self.client.as_mut().ok_or_else(|| {
            NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Client not connected",
            ))
        })
    }
}

impl RaftNetworkV2<TypeConfig> for GrpcNetworkClient {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        debug!(
            "Sending append_entries to node {}: prev_log_id={:?}, entries={}",
            self.target,
            req.prev_log_id,
            req.entries.len()
        );

        // Convert to protobuf request
        let proto_req = OpenRaftAppendRequest {
            vote: Some(crate::protobuf::openraft::OpenRaftVote {
                term: req.vote.leader_id().term,
                node_id: req.vote.leader_id().node_id,
                committed: req.vote.committed,
            }),
            prev_log_id: req.prev_log_id.map(|id| crate::protobuf::openraft::OpenRaftLogId {
                leader_id: id.leader_id.node_id,
                index: id.index,
            }),
            entries: req
                .entries
                .iter()
                .map(|e| crate::protobuf::openraft::OpenRaftLogEntry {
                    log_id: Some(crate::protobuf::openraft::OpenRaftLogId {
                        leader_id: e.log_id.leader_id.node_id,
                        index: e.log_id.index,
                    }),
                    payload: serde_json::to_vec(&e.payload).unwrap_or_default(),
                })
                .collect(),
            leader_commit: req.leader_commit.map(|id| crate::protobuf::openraft::OpenRaftLogId {
                leader_id: id.leader_id.node_id,
                index: id.index,
            }),
        };

        let client = self.get_client().await.map_err(RPCError::Network)?;
        let response = client.append_entries(Request::new(proto_req)).await.map_err(|e| {
            error!("AppendEntries RPC failed: {}", e);
            RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
        })?;

        let proto_resp = response.into_inner();

        // Convert protobuf response back to OpenRaft types
        if proto_resp.success {
            Ok(AppendEntriesResponse::Success)
        } else {
            Ok(AppendEntriesResponse::Conflict)
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        debug!(
            "Sending full_snapshot to node {}: vote={:?}, meta={:?}",
            self.target, vote, snapshot.meta
        );

        let client = self.get_client().await.map_err(StreamingError::Network)?;

        // Serialize snapshot metadata
        let last_membership = serde_json::to_vec(&snapshot.meta.last_membership).map_err(|e| {
            StreamingError::Network(NetworkError::new(&std::io::Error::other(format!(
                "Failed to serialize membership: {}",
                e
            ))))
        })?;

        // Extract snapshot data from Cursor
        let snapshot_data = snapshot.snapshot.into_inner();

        // For streaming API, we need to send data in chunks
        let request = crate::protobuf::openraft::OpenRaftSnapshotRequest {
            vote: Some(crate::protobuf::openraft::OpenRaftVote {
                term: vote.leader_id().term,
                node_id: vote.leader_id().node_id,
                committed: vote.is_committed(),
            }),
            meta: Some(crate::protobuf::openraft::OpenRaftSnapshotMeta {
                last_log_id: snapshot
                    .meta
                    .last_log_id
                    .map(|id| crate::protobuf::openraft::OpenRaftLogId {
                        leader_id: id.leader_id.node_id,
                        index: id.index,
                    }),
                snapshot_id: snapshot.meta.snapshot_id.clone(),
                last_membership: last_membership.clone(),
            }),
            offset: 0,
            data: snapshot_data,
            done: true,
        };

        let stream = tokio_stream::iter(vec![request]);

        // Send streaming request
        let response = client.install_snapshot(Request::new(stream)).await.map_err(|e| {
            error!("InstallSnapshot RPC failed: {}", e);
            StreamingError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
        })?;

        let proto_resp = response.into_inner();

        // Parse vote from response
        let vote = proto_resp
            .vote
            .map(|v| Vote::new(v.term, crate::typ::NodeId::from(v.node_id)))
            .unwrap_or_else(|| Vote::new(0, 0));

        Ok(SnapshotResponse { vote })
    }

    async fn vote(
        &mut self,
        req: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        debug!(
            "Sending vote request to node {}: vote={:?}, last_log_id={:?}",
            self.target, req.vote, req.last_log_id
        );

        let proto_req = ProtoVoteRequest {
            vote: Some(crate::protobuf::openraft::OpenRaftVote {
                term: req.vote.leader_id().term,
                node_id: req.vote.leader_id().node_id,
                committed: req.vote.is_committed(),
            }),
            last_log_id: req.last_log_id.map(|id| crate::protobuf::openraft::OpenRaftLogId {
                leader_id: id.leader_id.node_id,
                index: id.index,
            }),
        };

        let client = self.get_client().await.map_err(RPCError::Network)?;
        let response = client.vote(Request::new(proto_req)).await.map_err(|e| {
            error!("Vote RPC failed: {}", e);
            RPCError::Network(NetworkError::new(&std::io::Error::other(e.to_string())))
        })?;

        let proto_resp = response.into_inner();

        // Parse vote from response
        let vote = proto_resp
            .vote
            .map(|v| Vote::new(v.term, crate::typ::NodeId::from(v.node_id)))
            .unwrap_or_else(|| Vote::new(0, 0));

        Ok(VoteResponse {
            vote,
            vote_granted: proto_resp.vote_granted,
            last_log_id: None, // Simplified - the vote response doesn't always need the last_log_id
        })
    }
}
