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

use super::*;

#[test]
fn malformed_hydration_does_not_invent_queued_work_or_zero_revision_running_state() {
    let context = context();
    let created = InvestigationRun::create(&context, &create_request(), None, at())
        .unwrap()
        .run;
    for variant in 0..3 {
        let mut run = created.clone();
        match variant {
            0 => run.supplied_evidence_ids = vec![evidence(700)],
            1 => {
                run.status = InvestigationRunStatus::Running;
                run.supplied_evidence_ids = vec![EvidenceId::from_uuid(Uuid::nil())];
            }
            _ => run.status = InvestigationRunStatus::Running,
        }
        assert!(run.validate().is_err());
    }
}

#[test]
fn counters_are_lossless_strings_and_exhaustion_is_typed_and_atomic() {
    for counter in [0, 9_007_199_254_740_993, u64::MAX] {
        let value = InvestigationRevision::from_u64(counter);
        let wire = serde_json::to_string(&value).unwrap();
        assert_eq!(wire, format!("\"{counter}\""));
        assert_eq!(serde_json::from_str::<InvestigationRevision>(&wire).unwrap(), value);
    }
    for invalid in ["0", "\"01\"", "\"-1\"", "\"18446744073709551616\""] {
        assert!(serde_json::from_str::<InvestigationRevision>(invalid).is_err());
    }
    let (context, mut run) = fixture();
    run.revision = InvestigationRevision::from_u64(u64::MAX);
    let request = command(&run, InvestigationCommand::Pause {}, "overflow");
    let unchanged = run.clone();
    assert_eq!(
        run.apply_command(&context, &request, None, at()).unwrap_err(),
        InvestigationRunFailure::CounterExhausted
    );
    assert_eq!(run, unchanged);
    run.revision = InvestigationRevision::from_u64(1);
    run.control_epoch = InvestigationControlEpoch::from_u64(u64::MAX);
    let request = command(&run, InvestigationCommand::Cancel {}, "epoch-overflow");
    let unchanged = run.clone();
    assert_eq!(
        run.apply_command(&context, &request, None, at()).unwrap_err(),
        InvestigationRunFailure::CounterExhausted
    );
    assert_eq!(run, unchanged);
}

#[test]
fn unknown_mutating_payloads_and_actor_claims_are_not_part_of_the_contract() {
    let (_, run) = fixture();
    let request = command(&run, InvestigationCommand::Pause {}, "pause");
    let mut wire = serde_json::to_value(&request).unwrap();
    wire["actor_subject"] = json!("administrator");
    assert!(serde_json::from_value::<InvestigationCommandRequest>(wire).is_err());
    let mut wire = serde_json::to_value(&request).unwrap();
    wire["command"] = json!({"kind":"shell","command":"write-to-target"});
    assert!(serde_json::from_value::<InvestigationCommandRequest>(wire).is_err());
    let mut wire = serde_json::to_value(&request).unwrap();
    wire["command"]["url"] = json!("https://untrusted.invalid");
    assert!(serde_json::from_value::<InvestigationCommandRequest>(wire).is_err());
    let error = InvestigationRunFailure::ScopeMismatch;
    assert!(!format!("{error:?} {error}").contains("administrator"));
}

#[test]
fn old_investigation_and_incident_values_keep_their_existing_semantics() {
    assert!(serde_json::from_value::<InvestigationStatus>(json!("paused")).is_err());
    assert!(serde_json::from_value::<IncidentStatus>(json!("cancelled")).is_err());
    assert_eq!(
        serde_json::to_value(InvestigationStatus::NeedsEvidence).unwrap(),
        json!("needs_evidence")
    );
    assert_eq!(
        serde_json::to_value(IncidentStatus::Monitoring).unwrap(),
        json!("monitoring")
    );
    assert_eq!(
        serde_json::to_value(InvestigationRunStatus::Pausing).unwrap(),
        json!("pausing")
    );
}

#[test]
fn malformed_create_requests_have_typed_failures_without_external_work() {
    let context = context();
    for variant in 0..7 {
        let mut request = create_request();
        match variant {
            0 => request.budget.max_steps = 0,
            1 => request.deadline = at(),
            2 => request.schema_version = "unsupported.v2".into(),
            3 => request.idempotency_key = "unsafe whitespace".into(),
            4 => request.run_id = InvestigationRunId::from_uuid(Uuid::nil()),
            5 => request.scope.time_range.end = request.scope.time_range.start - Duration::seconds(1),
            _ => request.packs[0].pack_version = "latest".into(),
        }
        assert_eq!(
            InvestigationRun::create(&context, &request, None, at()).unwrap_err(),
            InvestigationRunFailure::InvalidInput
        );
    }
}
