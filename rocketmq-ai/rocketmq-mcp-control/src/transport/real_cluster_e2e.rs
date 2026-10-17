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

mod process;
mod protocol;
mod scenario;

use std::fmt;

const E2E_OPT_IN_ENV: &str = "ROCKETMQ_MCP_CONTROL_REAL_CLUSTER_E2E";
const NAMESRV_BIN_ENV: &str = "ROCKETMQ_MCP_CONTROL_E2E_NAMESRV_BIN";
const BROKER_BIN_ENV: &str = "ROCKETMQ_MCP_CONTROL_E2E_BROKER_BIN";
const MESSAGE_BODY: &str = "isolated-e2e-payload";
const EXPECTED_CALLS: usize = 22;
const EXPECTED_AUDIT_RECORDS: usize = 44;
const _: [(); EXPECTED_AUDIT_RECORDS] = [(); EXPECTED_CALLS * 2];

type E2eResult<T> = Result<T, E2eError>;

#[derive(Debug)]
struct E2eError(String);

impl E2eError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for E2eError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for E2eError {}

trait E2eContext<T> {
    fn e2e(self, context: &str) -> E2eResult<T>;
}

impl<T, E> E2eContext<T> for Result<T, E>
where
    E: fmt::Display,
{
    fn e2e(self, context: &str) -> E2eResult<T> {
        self.map_err(|error| E2eError::new(format!("{context}: {error}")))
    }
}

fn ensure(condition: bool, message: impl Into<String>) -> E2eResult<()> {
    if condition {
        Ok(())
    } else {
        Err(E2eError::new(message))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit opt-in: run scripts/run_real_cluster_e2e.ps1"]
async fn real_cluster_tls_mcp_mutations_restore_and_reap_every_owned_resource() {
    if std::env::var(E2E_OPT_IN_ENV).as_deref() != Ok("1") {
        panic!("set {E2E_OPT_IN_ENV}=1 through scripts/run_real_cluster_e2e.ps1");
    }

    let mut harness = match scenario::E2eHarness::start().await {
        Ok(harness) => harness,
        Err(error) => panic!("real-cluster E2E setup failed: {error}"),
    };
    let scenario_result = harness.exercise().await;
    let scenario_diagnostics = scenario_result
        .as_ref()
        .err()
        .map(|_| harness.sanitized_process_diagnostics());
    let cleanup_result = harness.cleanup().await;
    match (scenario_result, cleanup_result) {
        (Ok(()), Ok(evidence)) => {
            assert!(evidence.cluster_root_removed, "ephemeral cluster root was not removed");
            assert!(evidence.children_reaped, "an owned child process was not reaped");
            assert!(evidence.broker_config_restored, "Broker configuration was not restored");
            assert!(
                evidence.consumer_request_mode_restored,
                "Consumer request mode was not restored"
            );
            let repeated = harness
                .cleanup()
                .await
                .unwrap_or_else(|error| panic!("idempotent cleanup repeat failed: {error}"));
            assert_eq!(repeated, evidence, "cleanup repeat changed its evidence");
        }
        (Err(scenario), Ok(_)) => panic!(
            "real-cluster E2E scenario failed after successful cleanup: {scenario}; {}",
            scenario_diagnostics.as_deref().unwrap_or("diagnostics unavailable")
        ),
        (Ok(()), Err(cleanup)) => panic!("real-cluster E2E cleanup failed: {cleanup}"),
        (Err(scenario), Err(cleanup)) => {
            panic!(
                "real-cluster E2E scenario failed: {scenario}; cleanup also failed: {cleanup}; {}",
                scenario_diagnostics.as_deref().unwrap_or("diagnostics unavailable")
            )
        }
    }
}
