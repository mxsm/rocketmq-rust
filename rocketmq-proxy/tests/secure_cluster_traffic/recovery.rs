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

use std::{fs::File, io::Read, path::PathBuf};

use super::*;

const KINDS: [&str; 3] = ["pod_a", "pod_b", "service"];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalReport {
    schema_version: String,
    evidence_class: String,
    execution_path: String,
    pub(super) run_id: String,
    endpoints: Vec<OriginalEndpoint>,
    business_recovery: String,
    p99_latency: String,
    production_qualified: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalEndpoint {
    endpoint_kind: String,
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
    failure: Option<String>,
}

impl OriginalReport {
    pub(super) fn from_env() -> TestResult<Self> {
        let path = PathBuf::from(settings::required("ROCKETMQ_PROXY_LIVE_ORIGINAL_REPORT", 4096)?);
        if !path.is_absolute() {
            return Err("absolute_original_report_required");
        }
        let file = File::open(path).map_err(|_| "original_report_unavailable")?;
        if !file
            .metadata()
            .map_err(|_| "original_report_metadata_failed")?
            .is_file()
        {
            return Err("original_report_must_be_regular");
        }
        let mut bytes = Vec::new();
        file.take(64 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "original_report_read_failed")?;
        if bytes.is_empty() || bytes.len() > 64 * 1024 {
            return Err("original_report_size_invalid");
        }
        Self::from_bytes(&bytes)
    }

    fn from_bytes(bytes: &[u8]) -> TestResult<Self> {
        let value: Self = serde_json::from_slice(bytes).map_err(|_| "original_report_shape_invalid")?;
        if value.schema_version != "rocketmq.proxy-secure-traffic-drill.v1"
            || value.evidence_class != "disposable_target_traffic"
            || value.execution_path != "public_messaging"
            || value.production_qualified
            || value.business_recovery != "not_observed"
            || value.p99_latency != "not_observed"
            || !uuid::Uuid::parse_str(&value.run_id).is_ok_and(|id| !id.is_nil() && id.to_string() == value.run_id)
            || value.endpoints.len() != 3
        {
            return Err("original_report_not_the_retained_unqualified_three_send_run");
        }
        let mut kinds = BTreeSet::new();
        for endpoint in &value.endpoints {
            if !KINDS.contains(&endpoint.endpoint_kind.as_str())
                || !kinds.insert(endpoint.endpoint_kind.as_str())
                || endpoint.endpoint_digest.len() != 64
                || !endpoint
                    .endpoint_digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || !endpoint.tls_connected
                || !endpoint.unsigned_request_rejected
                || !endpoint.route_read
                || endpoint.send_attempts != 1
                || endpoint.acknowledged_sends != 1
                || endpoint.receive_calls == 0
                || endpoint.receive_calls > 64
                || endpoint.matched_id_and_body != 0
                || endpoint.ack_attempts != 0
                || endpoint.acknowledged_messages != 0
                || endpoint
                    .finished_at
                    .is_none_or(|finished| finished < endpoint.started_at)
                || !matches!(
                    endpoint.failure.as_deref(),
                    Some(
                        "receive_status_not_a_known_success_or_empty_result"
                            | "received_message_did_not_match_sent_identity_and_body"
                    )
                )
            {
                return Err("original_report_does_not_prove_acknowledged_send_and_zero_ack_attempts");
            }
        }
        Ok(value)
    }
}

#[derive(Serialize)]
struct RecoveredMessage {
    original_endpoint_kind: &'static str,
    expected_public_id_digest: String,
    identity_match: Option<IdentityMatch>,
    ack_attempts: u32,
    acknowledged_messages: u32,
}

#[derive(Serialize)]
struct RecoveryReport {
    schema_version: &'static str,
    mode: &'static str,
    expected_identity_provenance: &'static str,
    run_id: String,
    original_send_count: u32,
    send_attempts: u32,
    endpoint: EndpointReport,
    messages: Vec<RecoveredMessage>,
    failure: Option<&'static str>,
    production_qualified: bool,
}

/// Recover only the three acknowledged old requests. No branch sends a message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit receive-only recovery authorization, original private report and actual TLS task Service"]
async fn recover_original_secure_proxy_messages_without_sending() {
    let settings = Settings::from_env().expect("explicit task settings are required");
    settings::require_flag("ROCKETMQ_PROXY_LIVE_RECOVER_ORIGINAL")
        .expect("explicit receive-only authorization is required");
    let original = OriginalReport::from_env().expect("the original acknowledged-send/zero-ACK report is required");
    assert_eq!(
        settings.run_id, original.run_id,
        "recovery keeps the original immutable request identity"
    );
    let mut output = settings.open_report().expect("create a new private recovery report");
    let endpoint = settings
        .endpoints
        .iter()
        .find(|endpoint| endpoint.kind == "service")
        .expect("fixed task Service");
    let expected = KINDS.map(|kind| ProbeIdentity::new(&original.run_id, kind, true));
    let mut report = RecoveryReport {
        schema_version: "rocketmq.proxy-secure-traffic-recovery.v1",
        mode: "old_send_new_artifact_receive_only",
        expected_identity_provenance: "request_derived",
        run_id: original.run_id,
        original_send_count: 3,
        send_attempts: 0,
        endpoint: EndpointReport::new(endpoint),
        messages: KINDS
            .iter()
            .zip(&expected)
            .map(|(kind, identity)| RecoveredMessage {
                original_endpoint_kind: kind,
                expected_public_id_digest: settings::digest(identity.id.as_bytes()),
                identity_match: None,
                ack_attempts: 0,
                acknowledged_messages: 0,
            })
            .collect(),
        failure: None,
        production_qualified: false,
    };
    report.failure = recover(&settings, endpoint, &expected, &mut report).await.err();
    report.endpoint.finished_at = Some(Utc::now());
    serde_json::to_writer_pretty(&mut output, &report).expect("write only bounded recovery match facts and counters");
    output.sync_all().expect("flush private recovery facts");
    assert!(
        report.failure.is_none() && report.messages.iter().all(|message| message.acknowledged_messages == 1),
        "receive-only recovery failed; stop and inspect private closed-reason facts without repeating unknown ACKs"
    );
}

async fn recover(
    settings: &Settings,
    endpoint: &EndpointSettings,
    expected: &[ProbeIdentity; 3],
    report: &mut RecoveryReport,
) -> TestResult<()> {
    let deadline = Instant::now() + ENDPOINT_TIMEOUT;
    let (mut client, queues, client_id) = connect_and_route(settings, endpoint, &mut report.endpoint, deadline).await?;
    let tags = expected
        .iter()
        .map(|identity| identity.tag.as_str())
        .collect::<Vec<_>>()
        .join("||");
    for attempt in 0..64 {
        report.endpoint.receive_calls += 1;
        let Some(message) = receive_one(
            &mut client,
            settings,
            &client_id,
            queues[attempt % queues.len()].clone(),
            &tags,
            deadline,
            &mut report.endpoint,
        )
        .await?
        else {
            continue;
        };
        let properties = message
            .system_properties
            .as_ref()
            .ok_or("received_system_properties_missing")?;
        let index = expected
            .iter()
            .position(|identity| identity.id == properties.message_id)
            .ok_or("received_identity_is_not_one_of_the_original_requests")?;
        let recovered = &mut report.messages[index];
        if recovered.ack_attempts != 0 {
            return Err("original_identity_was_already_acknowledged_do_not_repeat");
        }
        let identity_match = expected[index].compare(&message, &expected[index].id);
        let matches = identity_match.matches();
        recovered.identity_match = Some(identity_match);
        if !matches {
            return Err("original_message_body_marker_or_tag_did_not_match");
        }
        let receipt = properties
            .receipt_handle
            .as_ref()
            .filter(|value| !value.is_empty() && value.len() <= 16 * 1024)
            .ok_or("matching_received_message_has_no_bounded_receipt")?;
        let request = v2::AckMessageRequest {
            group: Some(resource(GROUP)),
            topic: Some(resource(TOPIC)),
            entries: vec![v2::AckMessageEntry {
                message_id: properties.message_id.clone(),
                receipt_handle: receipt.clone(),
                lite_topic: None,
            }],
        };
        recovered.ack_attempts = 1;
        let acknowledged = bounded(
            deadline,
            client.ack_message(settings.request(request, &client_id, true)?),
            "ack_response_lost_do_not_retry",
        )
        .await?
        .map_err(|_| "ack_rpc_failed_remote_outcome_unknown_do_not_retry")?
        .into_inner();
        if !ok(&acknowledged.status)
            || acknowledged.entries.len() != 1
            || !ok(&acknowledged.entries[0].status)
            || acknowledged.entries[0].message_id != properties.message_id
            || acknowledged.entries[0].receipt_handle != *receipt
        {
            return Err("ack_status_or_original_receipt_binding_invalid");
        }
        recovered.acknowledged_messages = 1;
        if report.messages.iter().all(|message| message.acknowledged_messages == 1) {
            return Ok(());
        }
    }
    Err("original_messages_not_observed_within_bounded_reads")
}

#[test]
fn original_report_rejects_uncertain_send_or_any_prior_ack() {
    let run_id = "9e5e7008-493a-4c4f-9488-b3ad2b13d509";
    let endpoint = |kind| serde_json::json!({"endpoint_kind":kind,"endpoint_digest":"a".repeat(64),"started_at":"2026-11-16T00:00:00Z","finished_at":"2026-11-16T00:00:01Z","tls_connected":true,"unsigned_request_rejected":true,"route_read":true,"send_attempts":1,"acknowledged_sends":1,"receive_calls":1,"matched_id_and_body":0,"ack_attempts":0,"acknowledged_messages":0,"failure":"received_message_did_not_match_sent_identity_and_body"});
    let valid = serde_json::json!({"schema_version":"rocketmq.proxy-secure-traffic-drill.v1","evidence_class":"disposable_target_traffic","execution_path":"public_messaging","run_id":run_id,"endpoints":KINDS.map(endpoint),"business_recovery":"not_observed","p99_latency":"not_observed","production_qualified":false});
    assert!(OriginalReport::from_bytes(&serde_json::to_vec(&valid).unwrap()).is_ok());
    for (key, value) in [
        ("acknowledged_sends", 0),
        ("send_attempts", 2),
        ("ack_attempts", 1),
        ("acknowledged_messages", 1),
    ] {
        let mut invalid = valid.clone();
        invalid["endpoints"][0][key] = value.into();
        assert!(OriginalReport::from_bytes(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    let mut duplicate = valid.clone();
    duplicate["endpoints"][1]["endpoint_kind"] = "pod_a".into();
    assert!(OriginalReport::from_bytes(&serde_json::to_vec(&duplicate).unwrap()).is_err());
}
