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

use super::support::*;
use super::*;
use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;

#[test]
fn initial_executor_runs_every_durability_boundary_in_frozen_order() {
    let mut backend = ModelInitialBootstrapBackend::new(inventory(&bootstrap_snapshot()));
    let completed = execute_initial_bootstrap(foundation(&store_meta()), &mut backend)
        .expect("canonical initial bootstrap completes");

    assert_eq!(completed.store_uuid, store_uuid());
    assert_eq!(completed.witness_sequence, 3);
    assert_eq!(backend.executed_action_count(), 41);
    backend.assert_frozen_order();
}

#[test]
fn every_executor_boundary_failure_requires_reinspection_and_resumes_exactly_once() {
    let action_count = ModelInitialBootstrapBackend::expected_action_count();
    for failure_index in 0..action_count {
        let mut backend = ModelInitialBootstrapBackend::new(inventory(&bootstrap_snapshot()));
        backend.fail_after_action(failure_index);

        execute_initial_bootstrap(foundation(&store_meta()), &mut backend)
            .expect_err("injected boundary must stop the current executor run");
        let actions_after_failure = backend.executed_action_count();
        backend.clear_failure();
        let completed = execute_initial_bootstrap(foundation(&store_meta()), &mut backend)
            .expect("reinspection resumes the exact durable frontier");

        assert_eq!(completed.witness_sequence, 3);
        assert!(backend.executed_action_count() >= actions_after_failure);
        backend.assert_each_action_at_most_once();
    }
}

#[test]
fn durable_unit_machine_writes_and_reclassifies_the_exact_frame_ack_seal_protocol() {
    let plan = initial_store_plan();
    let mut machine = DurableUnitMachine::new(ModelLedgerIo::empty());

    loop {
        let progress = machine
            .inspect(BootstrapRecord::StoreInitialized, &plan.store_initialized)
            .expect("model ledger remains canonical");
        if progress == DurableUnitProgress::Committed {
            break;
        }
        let BootstrapDecision::Execute(BootstrapAction::AdvanceUnit { record, step }) =
            plan.decide_store_initialized(progress)
        else {
            panic!("unit must advance until committed");
        };
        machine
            .advance(record, &plan.store_initialized, step)
            .expect("planned unit operation succeeds");
    }

    assert_eq!(
        machine.io_for_test().log().len() as u64,
        plan.store_initialized.sealed_log_length
    );
    let slot = &machine.io_for_test().acknowledgement()[..104];
    assert_eq!(slot, plan.store_initialized.acknowledgement_slot);
}
