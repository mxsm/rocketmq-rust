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

use std::error::Error as StdError;
use std::future::Future;
use std::time::Duration;

use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::network::v2::RaftNetworkV2;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendError;
use openraft::raft::AppendResult;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::OptionalSend;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::Request;
use tracing::debug;
use tracing::error;

use rocketmq_model::utils::crc32_utils::crc32;

use crate::openraft::SNAPSHOT_CHUNK_BYTES;
use crate::openraft::SNAPSHOT_MAX_BYTES;
use crate::protobuf::openraft::open_raft_service_client::OpenRaftServiceClient;
use crate::protobuf::openraft::OpenRaftAppendRequest;
use crate::protobuf::openraft::OpenRaftAppendResponse;
use crate::protobuf::openraft::OpenRaftVote as ProtoVote;
use crate::protobuf::openraft::OpenRaftVoteRequest as ProtoVoteRequest;
use crate::typ::LogId;
use crate::typ::NodeId;
use crate::typ::Snapshot;
use crate::typ::TypeConfig;
use crate::typ::Vote;

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

fn decode_vote(vote: ProtoVote) -> Vote {
    if vote.committed {
        Vote::new_committed(vote.term, crate::typ::NodeId::from(vote.node_id))
    } else {
        Vote::new(vote.term, crate::typ::NodeId::from(vote.node_id))
    }
}

fn encode_vote(vote: Vote) -> ProtoVote {
    ProtoVote {
        term: vote.leader_id().term,
        node_id: vote.leader_id().node_id,
        committed: vote.is_committed(),
    }
}

fn decode_log_id(log_id: crate::protobuf::openraft::OpenRaftLogId) -> crate::typ::LogId {
    crate::typ::LogId {
        leader_id: crate::typ::Vote::new(log_id.term, log_id.node_id).leader_id,
        index: log_id.index,
    }
}

/// Decodes a follower's AppendEntries reply for the request with `prev_log_id` and `last_log_id`.
///
/// Plain success acknowledges the whole request. A partial success, sent only by older peers,
/// acknowledges the log id it carries. A conflict always refers to the request's `prev_log_id`;
/// a conflict reply to a request without one breaks the protocol and is returned as a network
/// error instead of being passed to OpenRaft.
fn decode_append_response(
    response: OpenRaftAppendResponse,
    prev_log_id: Option<LogId>,
    last_log_id: Option<LogId>,
) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
    if let Some(higher_vote) = response.higher_vote {
        Ok(Err(AppendError::HigherVote(decode_vote(higher_vote))))
    } else if let Some(partial_success) = response.partial_success {
        Ok(Ok(Some(decode_log_id(partial_success))))
    } else if response.success {
        Ok(Ok(last_log_id))
    } else if let Some(log_id) = prev_log_id {
        Ok(Err(AppendError::Conflict { log_id, hint: None }))
    } else {
        Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "AppendEntries conflict reply for a request without prev_log_id",
        ))))
    }
}

/// Encodes a vote request, also setting the legacy `leadership_transfer` flag for older peers.
fn encode_vote_request(req: &VoteRequest<TypeConfig>) -> ProtoVoteRequest {
    ProtoVoteRequest {
        vote: Some(encode_vote(req.vote)),
        last_log_id: req.last_log_id.map(|id| crate::protobuf::openraft::OpenRaftLogId {
            term: id.leader_id.term,
            node_id: id.leader_id.node_id,
            index: id.index,
        }),
        leadership_transfer: req.leadership_transfer_from.is_some(),
        leadership_transfer_from: req.leadership_transfer_from.map(encode_vote),
    }
}

fn network_error<E>(source: &E) -> NetworkError<TypeConfig>
where
    E: StdError + 'static,
{
    NetworkError::new(source)
}

impl RaftNetworkV2<TypeConfig> for GrpcNetworkClient {
    type SnapshotData = std::io::Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        debug!(
            "Sending append_entries to node {}: prev_log_id={:?}, entries={}",
            self.target,
            req.prev_log_id,
            req.entries.len()
        );
        let prev_log_id = req.prev_log_id;
        let last_log_id = req.entries.last().map(|entry| entry.log_id).or(prev_log_id);

        // Convert to protobuf request
        let entries = req
            .entries
            .iter()
            .map(|entry| {
                serde_json::to_vec(&entry.payload)
                    .map(|payload| crate::protobuf::openraft::OpenRaftLogEntry {
                        log_id: Some(crate::protobuf::openraft::OpenRaftLogId {
                            term: entry.log_id.leader_id.term,
                            node_id: entry.log_id.leader_id.node_id,
                            index: entry.log_id.index,
                        }),
                        payload,
                    })
                    .map_err(|error| RPCError::Network(network_error(&error)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let proto_req = OpenRaftAppendRequest {
            vote: Some(crate::protobuf::openraft::OpenRaftVote {
                term: req.vote.leader_id().term,
                node_id: req.vote.leader_id().node_id,
                committed: req.vote.committed,
            }),
            prev_log_id: req.prev_log_id.map(|id| crate::protobuf::openraft::OpenRaftLogId {
                term: id.leader_id.term,
                node_id: id.leader_id.node_id,
                index: id.index,
            }),
            entries,
            leader_commit: req.leader_commit.map(|id| crate::protobuf::openraft::OpenRaftLogId {
                term: id.leader_id.term,
                node_id: id.leader_id.node_id,
                index: id.index,
            }),
        };

        let response = {
            let client = self.get_client().await.map_err(RPCError::Network)?;
            client.append_entries(Request::new(proto_req)).await
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.client = None;
                error!("AppendEntries RPC failed: {}", error);
                return Err(RPCError::Network(network_error(&error)));
            }
        };

        decode_append_response(response.into_inner(), prev_log_id, last_log_id)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        debug!(
            "Sending full_snapshot to node {}: vote={:?}, meta={:?}",
            self.target, vote, snapshot.meta
        );

        // Serialize snapshot metadata
        let last_membership = serde_json::to_vec(&snapshot.meta.last_membership)
            .map_err(|error| StreamingError::Network(network_error(&error)))?;

        // Extract the bounded snapshot payload from the OpenRaft cursor.
        let snapshot_data = snapshot.snapshot.into_inner();
        if snapshot_data.len() > SNAPSHOT_MAX_BYTES {
            return Err(StreamingError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Snapshot size {} exceeds the {} byte transport limit",
                    snapshot_data.len(),
                    SNAPSHOT_MAX_BYTES
                ),
            ))));
        }
        let snapshot_checksum = crc32(&snapshot_data);
        let snapshot_id = crate::openraft::state_machine::inspect_snapshot_payload(&snapshot_data)
            .map_err(|error| StreamingError::Network(network_error(&error)))?
            .snapshot_id;
        let total_size = snapshot_data.len() as u64;
        let snapshot_meta = snapshot.meta;

        let chunk_count = snapshot_data.len().max(1).div_ceil(SNAPSHOT_CHUNK_BYTES);
        let stream = tokio_stream::iter(0..chunk_count).map(move |chunk_index| {
            let start = chunk_index * SNAPSHOT_CHUNK_BYTES;
            let end = (start + SNAPSHOT_CHUNK_BYTES).min(snapshot_data.len());
            let data = if snapshot_data.is_empty() {
                Vec::new()
            } else {
                snapshot_data[start..end].to_vec()
            };
            crate::protobuf::openraft::OpenRaftSnapshotRequest {
                vote: (chunk_index == 0).then_some(crate::protobuf::openraft::OpenRaftVote {
                    term: vote.leader_id().term,
                    node_id: vote.leader_id().node_id,
                    committed: vote.is_committed(),
                }),
                meta: (chunk_index == 0).then_some(crate::protobuf::openraft::OpenRaftSnapshotMeta {
                    last_log_id: snapshot_meta
                        .last_log_id
                        .map(|id| crate::protobuf::openraft::OpenRaftLogId {
                            term: id.leader_id.term,
                            node_id: id.leader_id.node_id,
                            index: id.index,
                        }),
                    snapshot_id: snapshot_id.clone(),
                    last_membership: last_membership.clone(),
                }),
                offset: start as u64,
                data,
                done: chunk_index + 1 == chunk_count,
                total_size,
                checksum: snapshot_checksum,
            }
        });

        // Send streaming request
        let response = {
            let client = self.get_client().await.map_err(StreamingError::Network)?;
            client.install_snapshot(Request::new(stream)).await
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.client = None;
                error!("InstallSnapshot RPC failed: {}", error);
                return Err(StreamingError::Network(network_error(&error)));
            }
        };

        let proto_resp = response.into_inner();

        // Parse vote from response
        let vote = proto_resp.vote.map(decode_vote).unwrap_or_else(|| Vote::new(0, 0));

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

        let proto_req = encode_vote_request(&req);

        let response = {
            let client = self.get_client().await.map_err(RPCError::Network)?;
            client.vote(Request::new(proto_req)).await
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.client = None;
                error!("Vote RPC failed: {}", error);
                return Err(RPCError::Network(network_error(&error)));
            }
        };

        let proto_resp = response.into_inner();

        // Parse vote from response
        let vote = proto_resp.vote.map(decode_vote).unwrap_or_else(|| Vote::new(0, 0));

        Ok(VoteResponse {
            vote,
            vote_granted: proto_resp.vote_granted,
            last_log_id: None, // Simplified - the vote response doesn't always need the last_log_id
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::net::TcpListener;

    use openraft::network::RPCOption;

    use super::*;

    #[test]
    fn network_error_retains_the_concrete_source_type() {
        let source =
            serde_json::from_slice::<serde_json::Value>(b"{").expect_err("invalid JSON must provide a typed source");
        let error = network_error(&source);
        let retained = error.source().expect("OpenRaft error source");

        assert!(retained.to_string().contains("serde_json::error::Error"));
    }

    #[tokio::test]
    async fn vote_rpc_failure_discards_cached_channel() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind unused test port");
        let address = listener.local_addr().expect("read unused test port");
        drop(listener);

        let endpoint = Channel::from_shared(format!("http://{address}"))
            .expect("valid test endpoint")
            .timeout(Duration::from_millis(100));
        let mut network = GrpcNetworkClient::new(2, address.to_string());
        network.client = Some(OpenRaftServiceClient::new(endpoint.connect_lazy()));

        let result = network
            .vote(
                VoteRequest::new(Vote::new(1, 1), None),
                RPCOption::new(Duration::from_secs(1)),
            )
            .await;

        assert!(result.is_err());
        assert!(network.client.is_none());
    }

    fn log_id(term: u64, node_id: u64, index: u64) -> LogId {
        LogId {
            leader_id: Vote::new(term, node_id).leader_id,
            index,
        }
    }

    fn append_response() -> OpenRaftAppendResponse {
        OpenRaftAppendResponse {
            vote: Some(encode_vote(Vote::new_committed(1, 1))),
            success: false,
            conflict: None,
            partial_success: None,
            higher_vote: None,
        }
    }

    #[test]
    fn append_reply_decodes_against_the_sent_request() {
        let prev = Some(log_id(1, 1, 3));
        let last = Some(log_id(1, 1, 5));

        let success = OpenRaftAppendResponse {
            success: true,
            ..append_response()
        };
        assert_eq!(decode_append_response(success, prev, last).unwrap(), Ok(last));

        let partial = OpenRaftAppendResponse {
            partial_success: Some(crate::protobuf::openraft::OpenRaftLogId {
                term: 1,
                node_id: 1,
                index: 4,
            }),
            ..append_response()
        };
        assert_eq!(
            decode_append_response(partial, prev, last).unwrap(),
            Ok(Some(log_id(1, 1, 4)))
        );

        assert_eq!(
            decode_append_response(append_response(), prev, last).unwrap(),
            Err(AppendError::Conflict {
                log_id: log_id(1, 1, 3),
                hint: None,
            })
        );

        let higher = OpenRaftAppendResponse {
            higher_vote: Some(encode_vote(Vote::new(2, 3))),
            ..append_response()
        };
        assert_eq!(
            decode_append_response(higher, prev, last).unwrap(),
            Err(AppendError::HigherVote(Vote::new(2, 3)))
        );
    }

    #[test]
    fn append_conflict_without_prev_log_id_is_a_network_error() {
        let result = decode_append_response(append_response(), None, Some(log_id(1, 1, 1)));

        assert!(matches!(result, Err(RPCError::Network(_))));
    }

    #[test]
    fn vote_request_carries_the_authorizing_leader_and_the_legacy_flag() {
        let ordinary = encode_vote_request(&VoteRequest::new(Vote::new(2, 2), Some(log_id(1, 1, 7))));
        assert!(!ordinary.leadership_transfer);
        assert_eq!(ordinary.leadership_transfer_from, None);
        assert_eq!(ordinary.last_log_id.map(|id| id.index), Some(7));

        let mut transfer = VoteRequest::new(Vote::new(2, 2), None);
        transfer.leadership_transfer_from = Some(Vote::new_committed(1, 1));
        let encoded = encode_vote_request(&transfer);
        assert!(encoded.leadership_transfer);
        assert_eq!(
            encoded.leadership_transfer_from.map(decode_vote),
            Some(Vote::new_committed(1, 1))
        );
    }
}
