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
fn reports_cannot_overwrite_case_annotations_existing_outputs_or_link_aliases() {
    let directory = std::env::temp_dir().join(format!(
        "sre-investigation-replay-preserve-{}",
        InvestigationRunId::new()
    ));
    std::fs::create_dir(&directory).unwrap();
    let case_path = directory.join("case.json");
    let annotations_path = directory.join("annotations.json");
    let existing_output = directory.join("report.json");
    let case_bytes = serde_json::to_vec(&case()).unwrap();
    std::fs::write(&case_path, &case_bytes).unwrap();
    std::fs::write(&annotations_path, b"[]").unwrap();
    std::fs::write(&existing_output, b"previous-report").unwrap();
    for target in [&case_path, &annotations_path, &existing_output] {
        assert_eq!(
            replay_files(&case_path, Some(&annotations_path), target).unwrap_err(),
            InvestigationReplayFileRejection::OutputUnavailable
        );
    }
    let hard_link = directory.join("case-hard-link.json");
    std::fs::hard_link(&case_path, &hard_link).unwrap();
    assert_eq!(
        replay_files(&case_path, Some(&annotations_path), &hard_link).unwrap_err(),
        InvestigationReplayFileRejection::OutputUnavailable
    );
    #[cfg(unix)]
    {
        let symlink = directory.join("annotation-link.json");
        std::os::unix::fs::symlink(&annotations_path, &symlink).unwrap();
        assert_eq!(
            replay_files(&case_path, Some(&annotations_path), &symlink).unwrap_err(),
            InvestigationReplayFileRejection::OutputUnavailable
        );
    }
    assert_eq!(std::fs::read(&case_path).unwrap(), case_bytes);
    assert_eq!(std::fs::read(&annotations_path).unwrap(), b"[]");
    assert_eq!(std::fs::read(&existing_output).unwrap(), b"previous-report");
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn bounded_file_replay_exports_observations_but_no_evidence_body_or_invented_adjudication() {
    let directory = std::env::temp_dir().join(format!("sre-investigation-replay-{}", InvestigationRunId::new()));
    std::fs::create_dir(&directory).unwrap();
    let case_path = directory.join("case.json");
    let output = directory.join("report.json");
    std::fs::write(&case_path, serde_json::to_vec(&case()).unwrap()).unwrap();
    replay_files(&case_path, None, &output).unwrap();
    let report: serde_json::Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    assert_eq!(report["adjudication"]["multi_pack"]["kind"], "unadjudicated");
    assert_eq!(report["replay"]["multi_pack"]["read_callbacks"], 3);
    assert_eq!(report["replay"]["multi_pack"]["snapshot"]["status"], "completed");
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains("\"storage\":\"inline\"")
    );
    std::fs::write(&case_path, b"{\"raw-secret-input\":true}").unwrap();
    assert_eq!(
        replay_files(&case_path, None, &output).unwrap_err(),
        InvestigationReplayFileRejection::InvalidJson
    );
    std::fs::write(&case_path, vec![b' '; 16 * 1024 * 1024 + 1]).unwrap();
    assert_eq!(
        replay_files(&case_path, None, &output).unwrap_err(),
        InvestigationReplayFileRejection::InputTooLarge
    );
    std::fs::remove_dir_all(directory).unwrap();
}
