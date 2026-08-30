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

use parking_lot::Mutex;
use rocketmq_protocol::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_transport::api::DeferredAdmissionAcquireErrorKind;

use super::*;
use crate::long_polling::pop_lite_deferred::index::PopLiteIndexErrorKind;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredPrepareError;
use crate::long_polling::pop_lite_deferred::prepare::PreparedPopLiteRegistration;
use crate::processor::pop_lite_message_processor::core::PopLiteCoreResult;
use crate::processor::pop_lite_message_processor::response::compose_pop_lite_response_plan;
use crate::processor::pop_lite_message_processor::response::PopLiteResponseKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapacityFailure {
    Index(PopLiteIndexErrorKind),
    Admission(DeferredAdmissionAcquireErrorKind),
}

fn capacity_failure(error: PopLiteDeferredPrepareError) -> CapacityFailure {
    match error {
        PopLiteDeferredPrepareError::Index(error) => CapacityFailure::Index(error.kind()),
        PopLiteDeferredPrepareError::Admission(error) => CapacityFailure::Admission(error.kind()),
        error => panic!("unexpected PopLite capacity failure: {error:?}"),
    }
}

fn polling_full_reply(request: &RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    let request_header = request
        .command()
        .decode_command_custom_header::<PopLiteMessageRequestHeader>()?;
    compose_pop_lite_response_plan(
        &application_remoting_command_factory(),
        &request_header,
        PopLiteCoreResult {
            body: None,
            fetched_count: 0,
            order_count_info: None,
        },
        PopLiteResponseKind::PollingFull,
    )
    .map(HandlerOutcome::Reply)
}

fn held_reply() -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    ResponsePlan::command(RemotingCommand::create_response_command_with_code(
        ResponseCode::Success,
    ))
    .map(HandlerOutcome::Reply)
    .map_err(|error| RocketMQError::illegal_argument(error.to_string()))
}

#[derive(Clone)]
struct CapacityWireProcessor {
    service: Arc<PopLiteDeferredService>,
    held: Arc<Mutex<Vec<PreparedPopLiteRegistration>>>,
    failures: Arc<Mutex<Vec<CapacityFailure>>>,
}

impl RequestProcessor for CapacityWireProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match self.service.prepare(request, PopLiteRetainedEstimate::default()) {
            Ok(prepared) => {
                self.held.lock().push(prepared);
                held_reply()
            }
            Err(error) => {
                self.failures.lock().push(capacity_failure(error));
                polling_full_reply(request)
            }
        }
    }
}

fn matrix_service(
    controller: &AdmissionController,
    index_limits: PopLiteIndexLimits,
    wait_limits: DeferredWaitLimits,
) -> Arc<PopLiteDeferredService> {
    let admission = DeferredAdmission::try_configure(controller, wait_limits).expect("matrix deferred admission");
    Arc::new(PopLiteDeferredService::new(
        admission,
        index_limits,
        LiteEventDispatcher::default(),
        DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
        Duration::from_secs(30),
        nonzero(4),
    ))
}

struct CapacityCase {
    index_limits: PopLiteIndexLimits,
    wait_limits: DeferredWaitLimits,
    clients: &'static [&'static str],
    expected: CapacityFailure,
}

#[tokio::test]
async fn pop_lite_deferred_capacity_matrix_writes_exact_polling_full_frame() {
    let cases = [
        CapacityCase {
            index_limits: PopLiteIndexLimits::new(nonzero(1), nonzero(4), nonzero(4)),
            wait_limits: DeferredWaitLimits::new(4, 4 * 1024 * 1024),
            clients: &["global-a", "global-b"],
            expected: CapacityFailure::Index(PopLiteIndexErrorKind::GlobalCapacity),
        },
        CapacityCase {
            index_limits: PopLiteIndexLimits::new(nonzero(4), nonzero(1), nonzero(4)),
            wait_limits: DeferredWaitLimits::new(4, 4 * 1024 * 1024),
            clients: &["client-a", "client-b"],
            expected: CapacityFailure::Index(PopLiteIndexErrorKind::ClientCapacity),
        },
        CapacityCase {
            index_limits: PopLiteIndexLimits::new(nonzero(4), nonzero(4), nonzero(1)),
            wait_limits: DeferredWaitLimits::new(4, 4 * 1024 * 1024),
            clients: &["per-client", "per-client"],
            expected: CapacityFailure::Index(PopLiteIndexErrorKind::PerClientCapacity),
        },
        CapacityCase {
            index_limits: PopLiteIndexLimits::new(nonzero(4), nonzero(4), nonzero(4)),
            wait_limits: DeferredWaitLimits::new(1, 4 * 1024 * 1024),
            clients: &["wait-count-a", "wait-count-b"],
            expected: CapacityFailure::Admission(DeferredAdmissionAcquireErrorKind::WaiterCapacityExhausted),
        },
        CapacityCase {
            index_limits: PopLiteIndexLimits::new(nonzero(4), nonzero(4), nonzero(4)),
            wait_limits: DeferredWaitLimits::new(4, 1),
            clients: &["wait-bytes"],
            expected: CapacityFailure::Admission(DeferredAdmissionAcquireErrorKind::RetainedByteCapacityExhausted),
        },
    ];

    for (case_index, case) in cases.into_iter().enumerate() {
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = matrix_service(controller.as_ref(), case.index_limits, case.wait_limits);
        let held = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(Mutex::new(Vec::new()));
        let (mut client, running) = start_server(
            CapacityWireProcessor {
                service: Arc::clone(&service),
                held: Arc::clone(&held),
                failures: Arc::clone(&failures),
            },
            controller,
        )
        .await;

        for (request_index, client_id) in case.clients.iter().enumerate() {
            let opaque = 20_000 + (case_index as i32 * 10) + request_index as i32;
            client
                .send_command(request_command_for(client_id, opaque, 60_000))
                .await
                .expect("send PopLite capacity request");
            let mut response = client
                .receive_command()
                .await
                .expect("capacity connection remains open")
                .expect("capacity response frame");
            assert_eq!(response.opaque(), opaque);
            if request_index + 1 == case.clients.len() {
                assert_eq!(response.code(), ResponseCode::PollingFull as i32);
                assert_eq!(
                    response.remark().map(CheetahString::as_str),
                    Some("POP_LITE_POLLING_FULL")
                );
                assert!(response.body().is_none());
                response.make_custom_header_to_net();
                let header = response
                    .decode_command_custom_header::<PopLiteMessageResponseHeader>()
                    .expect("decode PollingFull response header");
                assert!(header.pop_time > 0);
                assert_eq!(header.invisible_time, 30_000);
                assert_eq!(header.revive_qid, POP_ORDER_REVIVE_QUEUE);
                assert_eq!(header.start_offset_info, None);
                assert_eq!(header.msg_offset_info, None);
                assert_eq!(header.order_count_info, None);
            } else {
                assert_eq!(response.code(), ResponseCode::Success as i32);
            }
        }

        assert_eq!(failures.lock().as_slice(), &[case.expected]);
        let rejected = service.resource_snapshot();
        match case.expected {
            CapacityFailure::Index(_) => assert_eq!(rejected.admission.rejected_count(), 0),
            CapacityFailure::Admission(_) => assert_eq!(rejected.admission.rejected_count(), 1),
        }
        if case.clients.len() > 1 {
            assert_eq!(rejected.prepared_registrations, 1);
            assert_eq!(rejected.index.reserved, 1);
            assert_eq!(rejected.admission.waiting_count(), 1);
            assert!(rejected.admission.retained_bytes() > 0);
        } else {
            assert_eq!(rejected.prepared_registrations, 0);
            assert_eq!(rejected.index.reserved, 0);
            assert_eq!(rejected.admission.waiting_count(), 0);
            assert_eq!(rejected.admission.retained_bytes(), 0);
        }

        held.lock().clear();
        let terminal = service.resource_snapshot();
        assert_eq!(terminal.admission.waiting_count(), 0);
        assert_eq!(terminal.admission.retained_bytes(), 0);
        assert_eq!(terminal.index.live, 0);
        assert_eq!(terminal.index.reserved, 0);
        assert_eq!(terminal.index.candidates, 0);
        assert_eq!(terminal.index.clients, 0);
        assert_eq!(terminal.index.oldest_waiter_age, None);
        assert_eq!(terminal.prepared_registrations, 0);
        assert_eq!(terminal.resume_execution_count, 0);
        assert_eq!(terminal.resume_execution_bytes, 0);
        running.finish().await;
    }
}
