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

#![cfg(feature = "tls")]

//! Bounded traffic through two actual Pod IPs and their Service. This opt-in
//! client uses only public messaging RPCs. It neither mutates configuration nor
//! claims a business recovery window, latency percentile or execution grant.

use bytes::Bytes;
use chrono::Utc;
use rocketmq_proxy::v2;
use rocketmq_proxy::v2::messaging_service_client::MessagingServiceClient;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, future::Future, time::Duration};
use tokio::time::Instant;
use tonic::{transport::Channel, Code};

#[path = "secure_cluster_traffic/recovery.rs"]
mod recovery;
#[path = "secure_cluster_traffic/settings.rs"]
mod settings;
use settings::{EndpointSettings, Settings, TestResult};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const ENDPOINT_TIMEOUT: Duration = Duration::from_secs(45);
const TOPIC: &str = "SRE_PROBE_D21ProxyTraffic";
const GROUP: &str = "SRE_PROBE_D21ProxyTrafficGroup";

#[derive(Serialize)]
struct EndpointReport {
    endpoint_kind: &'static str,
    endpoint_digest: String,
    started_at: chrono::DateTime<Utc>,
    finished_at: Option<chrono::DateTime<Utc>>,
    tls_connected: bool,
    unsigned_request_rejected: bool,
    route_read: bool,
    send_attempts: u32,
    acknowledged_sends: u32,
    receive_calls: u32,
    matched_id_and_body: u32,
    ack_attempts: u32,
    acknowledged_messages: u32,
    last_receive_status: Option<i32>,
    identity_match: Option<IdentityMatch>,
    received_public_id_digest: Option<String>,
    failure: Option<&'static str>,
}

impl EndpointReport {
    fn new(endpoint: &EndpointSettings) -> Self {
        Self {
            endpoint_kind: endpoint.kind,
            endpoint_digest: endpoint.digest(),
            started_at: Utc::now(),
            finished_at: None,
            tls_connected: false,
            unsigned_request_rejected: false,
            route_read: false,
            send_attempts: 0,
            acknowledged_sends: 0,
            receive_calls: 0,
            matched_id_and_body: 0,
            ack_attempts: 0,
            acknowledged_messages: 0,
            last_receive_status: None,
            identity_match: None,
            received_public_id_digest: None,
            failure: None,
        }
    }
}

#[derive(Serialize)]
struct IdentityMatch {
    topic: bool,
    public_id: bool,
    marker: bool,
    body: bool,
    tag: bool,
}

impl IdentityMatch {
    fn matches(&self) -> bool {
        self.topic && self.public_id && self.marker && self.body && self.tag
    }
}

struct ProbeIdentity {
    id: String,
    body: Bytes,
    tag: String,
}

impl ProbeIdentity {
    fn new(run_id: &str, kind: &str, legacy_tag: bool) -> Self {
        let id = format!("{run_id}-{kind}");
        Self {
            body: Bytes::from(format!("d21-bounded-probe:{id}")),
            tag: if legacy_tag {
                format!("d21-{run_id}-{kind}")
            } else {
                format!("d21-{run_id}")
            },
            id,
        }
    }

    fn compare(&self, message: &v2::Message, public_id: &str) -> IdentityMatch {
        let properties = message.system_properties.as_ref();
        IdentityMatch {
            topic: message.topic.as_ref() == Some(&resource(TOPIC)),
            public_id: properties.is_some_and(|value| value.message_id == public_id),
            marker: message.user_properties.get("sre_drill_message_id") == Some(&self.id),
            body: message.body == self.body,
            tag: properties.and_then(|value| value.tag.as_deref()) == Some(self.tag.as_str()),
        }
    }
}

#[derive(Serialize)]
struct PrivateReport {
    schema_version: &'static str,
    evidence_class: &'static str,
    execution_path: &'static str,
    run_id: String,
    mode: &'static str,
    expected_identity_provenance: &'static str,
    endpoints: Vec<EndpointReport>,
    business_recovery: &'static str,
    p99_latency: &'static str,
    production_qualified: bool,
}

/// Root/fixture owner must bootstrap the fixed task topic/group and supply
/// exact private endpoints. A lost send or ACK response fails this case; it is
/// never retried as another write. Only known empty receives are polled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires authorized real TLS cluster Proxies, fixed probe topic/group, and private credentials"]
async fn real_secure_proxy_pods_and_service_send_receive_and_ack_exact_messages() {
    let settings = Settings::from_env().expect("explicit real-Proxy messaging settings are required");
    settings::require_flag("ROCKETMQ_PROXY_LIVE_FRESH_PROBE").expect("separate fresh Send authorization is required");
    let original = recovery::OriginalReport::from_env().expect("the retained original report must be verified first");
    assert_ne!(
        settings.run_id, original.run_id,
        "a fresh probe requires a new run identity"
    );
    let mut output = settings
        .open_report()
        .expect("create a new private report before messaging");
    let mut report = PrivateReport {
        schema_version: "rocketmq.proxy-secure-traffic-drill.v2",
        evidence_class: "disposable_target_traffic",
        execution_path: "public_messaging",
        run_id: settings.run_id.clone(),
        mode: "fresh_roundtrip",
        expected_identity_provenance: "acknowledged_send_response",
        endpoints: Vec::new(),
        business_recovery: "not_observed",
        p99_latency: "not_observed",
        production_qualified: false,
    };
    for endpoint in &settings.endpoints {
        let mut outcome = EndpointReport::new(endpoint);
        outcome.failure = round_trip(&settings, endpoint, &mut outcome).await.err();
        outcome.finished_at = Some(Utc::now());
        report.endpoints.push(outcome);
        if report.endpoints.last().is_some_and(|value| value.failure.is_some()) {
            break;
        }
    }
    serde_json::to_writer_pretty(&mut output, &report)
        .expect("write bounded private counters without message contents");
    output.sync_all().expect("flush private messaging report");
    assert!(
        report.endpoints.len() == 3 && report.endpoints.iter().all(|outcome| outcome.failure.is_none()),
        "secure Proxy traffic failed; inspect private closed-reason counter report; do not repeat unknown writes",
    );
}

async fn connect_and_route(
    settings: &Settings,
    endpoint: &EndpointSettings,
    report: &mut EndpointReport,
    deadline: Instant,
) -> TestResult<(MessagingServiceClient<Channel>, Vec<v2::MessageQueue>, String)> {
    let channel = bounded(deadline, endpoint.connect(&settings.ca_pem), "tls_connect_timeout").await??;
    report.tls_connected = true;
    let mut client = MessagingServiceClient::new(channel)
        .max_decoding_message_size(512 * 1024)
        .max_encoding_message_size(64 * 1024);
    let client_id = format!("d21-{}-{}", settings.run_id, endpoint.kind);
    let unsigned = client.query_route(settings.request(route_request(endpoint), &client_id, false)?);
    match bounded(deadline, unsigned, "unsigned_control_timeout").await? {
        Err(status) if matches!(status.code(), Code::Unauthenticated | Code::PermissionDenied) => {}
        Ok(response) => {
            if !response.into_inner().status.is_some_and(|status| {
                [v2::Code::Unauthorized as i32, v2::Code::Forbidden as i32].contains(&status.code)
            }) {
                return Err("unsigned_message_route_was_not_rejected_by_auth");
            }
        }
        _ => return Err("unsigned_message_route_was_not_rejected_by_auth"),
    }
    report.unsigned_request_rejected = true;
    let response = bounded(
        deadline,
        client.query_route(settings.request(route_request(endpoint), &client_id, true)?),
        "signed_route_timeout",
    )
    .await?
    .map_err(|_| "signed_route_rpc_failed")?
    .into_inner();
    if !ok(&response.status) || response.message_queues.is_empty() || response.message_queues.len() > 32 {
        return Err("fixed_probe_route_unavailable_or_outside_bound");
    }
    let mut queues = Vec::new();
    let mut seen = BTreeSet::new();
    for queue in response.message_queues {
        if queue.topic.as_ref() != Some(&resource(TOPIC))
            || queue.id < 0
            || ![v2::Permission::Read as i32, v2::Permission::ReadWrite as i32].contains(&queue.permission)
            || queue.broker.as_ref().is_none_or(|broker| broker.name.is_empty())
        {
            return Err("fixed_probe_route_contains_invalid_queue");
        }
        let key = (queue.broker.as_ref().map(|broker| broker.name.clone()), queue.id);
        if !seen.insert(key) {
            return Err("fixed_probe_route_has_duplicate_queues");
        }
        queues.push(queue);
    }
    report.route_read = true;
    Ok((client, queues, client_id))
}

async fn round_trip(settings: &Settings, endpoint: &EndpointSettings, report: &mut EndpointReport) -> TestResult<()> {
    let deadline = Instant::now() + ENDPOINT_TIMEOUT;
    let (mut client, queues, client_id) = connect_and_route(settings, endpoint, report, deadline).await?;
    let expected = ProbeIdentity::new(&settings.run_id, endpoint.kind, false);
    let message = v2::Message {
        topic: Some(resource(TOPIC)),
        user_properties: [("sre_drill_message_id".to_owned(), expected.id.clone())]
            .into_iter()
            .collect(),
        system_properties: Some(v2::SystemProperties {
            message_id: expected.id.clone(),
            tag: Some(expected.tag.clone()),
            body_encoding: v2::Encoding::Identity as i32,
            message_type: v2::MessageType::Normal as i32,
            ..Default::default()
        }),
        body: expected.body.clone(),
    };
    report.send_attempts = 1;
    let sent = bounded(
        deadline,
        client.send_message(settings.request(
            v2::SendMessageRequest {
                messages: vec![message],
            },
            &client_id,
            true,
        )?),
        "send_response_lost_do_not_retry",
    )
    .await?
    .map_err(|_| "send_rpc_failed_remote_outcome_unknown_do_not_retry")?
    .into_inner();
    if !ok(&sent.status)
        || sent.entries.len() != 1
        || !ok(&sent.entries[0].status)
        || sent.entries[0].message_id.is_empty()
    {
        return Err("send_status_not_exactly_one_acknowledged_message");
    }
    report.acknowledged_sends = 1;
    // The server's send-result ID is an explicit mapping from our generated
    // client ID. Match that ID, our echoed private marker and body in memory.
    let sent_id = &sent.entries[0].message_id;
    for index in 0..64 {
        let queue = queues[index % queues.len()].clone();
        report.receive_calls += 1;
        let received = receive_one(
            &mut client,
            settings,
            &client_id,
            queue,
            &expected.tag,
            deadline,
            report,
        )
        .await?;
        let Some(message) = received else { continue };
        let properties = message
            .system_properties
            .as_ref()
            .ok_or("received_system_properties_missing")?;
        let identity_match = expected.compare(&message, sent_id);
        let matches = identity_match.matches();
        report.identity_match = Some(identity_match);
        report.received_public_id_digest = Some(settings::digest(properties.message_id.as_bytes()));
        if !matches {
            return Err("received_message_did_not_match_sent_identity_and_body");
        }
        let receipt = properties
            .receipt_handle
            .as_ref()
            .filter(|value| !value.is_empty() && value.len() <= 16 * 1024)
            .ok_or("matching_received_message_has_no_bounded_receipt")?;
        report.matched_id_and_body = 1;
        let ack = v2::AckMessageRequest {
            group: Some(resource(GROUP)),
            topic: Some(resource(TOPIC)),
            entries: vec![v2::AckMessageEntry {
                message_id: sent_id.clone(),
                receipt_handle: receipt.clone(),
                lite_topic: None,
            }],
        };
        report.ack_attempts = 1;
        let acknowledged = bounded(
            deadline,
            client.ack_message(settings.request(ack, &client_id, true)?),
            "ack_response_lost_do_not_retry",
        )
        .await?
        .map_err(|_| "ack_rpc_failed_remote_outcome_unknown_do_not_retry")?
        .into_inner();
        if !ok(&acknowledged.status)
            || acknowledged.entries.len() != 1
            || !ok(&acknowledged.entries[0].status)
            || acknowledged.entries[0].message_id != *sent_id
            || acknowledged.entries[0].receipt_handle != *receipt
        {
            return Err("ack_status_or_original_receipt_binding_invalid");
        }
        report.acknowledged_messages = 1;
        return Ok(());
    }
    Err("matching_message_not_observed_within_bounded_reads")
}

async fn receive_one(
    client: &mut MessagingServiceClient<Channel>,
    settings: &Settings,
    client_id: &str,
    queue: v2::MessageQueue,
    tag: &str,
    deadline: Instant,
    report: &mut EndpointReport,
) -> TestResult<Option<v2::Message>> {
    let request = v2::ReceiveMessageRequest {
        group: Some(resource(GROUP)),
        message_queue: Some(queue),
        filter_expression: Some(v2::FilterExpression {
            r#type: v2::FilterType::Tag as i32,
            expression: tag.to_owned(),
        }),
        batch_size: 1,
        invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
        auto_renew: false,
        long_polling_timeout: Some(prost_types::Duration { seconds: 1, nanos: 0 }),
        attempt_id: None,
    };
    let mut stream = bounded(
        deadline,
        client.receive_message(settings.request(request, client_id, true)?),
        "receive_response_timeout",
    )
    .await?
    .map_err(|_| "receive_rpc_failed")?
    .into_inner();
    let mut message = None;
    let mut status = None;
    for _ in 0..4 {
        let next = bounded(deadline, stream.message(), "receive_stream_timeout")
            .await?
            .map_err(|_| "receive_stream_failed")?;
        let Some(next) = next else {
            let status = status.ok_or("receive_stream_missing_status")?;
            report.last_receive_status = Some(status);
            if status != v2::Code::Ok as i32 && status != v2::Code::MessageNotFound as i32 {
                return Err("receive_status_not_a_known_success_or_empty_result");
            }
            if message.is_some() && status != v2::Code::Ok as i32 {
                return Err("receive_message_has_non_success_status");
            }
            return Ok(message);
        };
        match next.content {
            Some(v2::receive_message_response::Content::Message(value)) if message.is_none() => message = Some(value),
            Some(v2::receive_message_response::Content::Status(value)) if status.is_none() => status = Some(value.code),
            Some(v2::receive_message_response::Content::DeliveryTimestamp(_)) => {}
            _ => return Err("receive_stream_exceeded_single_message_or_status_contract"),
        }
    }
    Err("receive_stream_exceeded_bounded_items")
}

fn route_request(endpoint: &EndpointSettings) -> v2::QueryRouteRequest {
    v2::QueryRouteRequest {
        topic: Some(resource(TOPIC)),
        endpoints: Some(endpoint.proto()),
    }
}

fn resource(name: &str) -> v2::Resource {
    v2::Resource {
        resource_namespace: String::new(),
        name: name.to_owned(),
    }
}

fn ok(status: &Option<v2::Status>) -> bool {
    status.as_ref().is_some_and(|status| status.code == v2::Code::Ok as i32)
}

async fn bounded<T>(deadline: Instant, future: impl Future<Output = T>, reason: &'static str) -> TestResult<T> {
    let deadline = deadline.min(Instant::now() + RPC_TIMEOUT);
    tokio::time::timeout_at(deadline, future).await.map_err(|_| reason)
}
