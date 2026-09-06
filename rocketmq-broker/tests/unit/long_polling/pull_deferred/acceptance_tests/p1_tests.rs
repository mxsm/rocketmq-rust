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
use crate::long_polling::pull_deferred::service::PullDeferredRegisterRejection;

fn prepared(
    service: &PullDeferredService,
    request: &RemotingRequest,
    timing: PullSuspendTiming,
    retained: PullRetainedEstimate,
) -> Result<PullDeferredPrepareOutcome, super::PullDeferredPrepareError> {
    let header = request
        .command()
        .decode_command_custom_header::<PullMessageRequestHeader>()
        .expect("valid Pull pre-take test header");
    let criteria = PullMatchCriteria::new(
        header.topic.clone(),
        header.queue_id,
        header.queue_offset,
        SubscriptionData::default(),
        Arc::new(MatchAll),
    );
    let fallback = RemotingResponse::command(RemotingCommand::create_response_command_with_code(
        ResponseCode::PullNotFound,
    ))
    .expect("valid PullNotFound fallback");
    service.prepare(request, criteria, fallback, timing, retained)
}

#[derive(Default)]
struct ProvenanceState {
    prepared: Option<PreparedPullRegistration>,
}

#[derive(Clone)]
struct ProvenanceProcessor {
    service: Arc<PullDeferredService>,
    state: Arc<Mutex<ProvenanceState>>,
}

impl RequestProcessor for ProvenanceProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let prior = self.state.lock().prepared.take();
        if let Some(prior) = prior {
            let rejection = match self.service.register(prior, request) {
                Ok(PullDeferredRegisterOutcome::Rejected(rejection)) => *rejection,
                Ok(PullDeferredRegisterOutcome::Registered(_)) | Err(_) => {
                    panic!("prepared Pull proof is bound to its original request/session")
                }
            };
            assert_eq!(rejection.kind(), PullDeferredRegisterRejectionKind::ProvenanceMismatch);
            let candidate = match rejection.into_candidate() {
                Ok(candidate) => candidate,
                Err(PullDeferredRegisterRejection::PreTake { .. }) => {
                    panic!("pre-take rejection must return its affine candidate")
                }
                Err(PullDeferredRegisterRejection::Expiry { .. }) => {
                    panic!("provenance mismatch cannot become an expiry rejection")
                }
                Err(PullDeferredRegisterRejection::RegistryRejected) => {
                    panic!("provenance mismatch cannot become a registry rejection")
                }
            };
            assert_eq!(candidate.request().request_code(), RequestCode::PullMessage);
            assert_eq!(candidate.criteria().physical_topic().as_str(), "TopicA");
            assert_eq!(candidate.criteria().pull_from_offset(), 7);
            assert_eq!(candidate.timing().effective_timeout_millis(), TEST_TIMEOUT_MILLIS);
            assert_eq!(candidate.retained(), PullRetainedEstimate::new(17, 23));
            let fallback = candidate.into_fallback();
            assert_eq!(fallback.response_code(), ResponseCode::PullNotFound as i32);
            assert_eq!(fallback.body_len(), 0);
            return success_reply();
        }
        let header = request
            .command()
            .decode_command_custom_header::<PullMessageRequestHeader>()?;
        let registration = match prepared(
            &self.service,
            request,
            PullSuspendTiming::new(
                current_millis(),
                tokio::time::Instant::now(),
                header.suspend_timeout_millis,
            ),
            PullRetainedEstimate::new(17, 23),
        )
        .map_err(|error| RocketMQError::illegal_argument(error.to_string()))?
        {
            PullDeferredPrepareOutcome::Prepared(prepared) => prepared,
            PullDeferredPrepareOutcome::Rejected(_) => {
                return Err(RocketMQError::illegal_argument("unexpected Pull preparation rejection"));
            }
        };
        self.state.lock().prepared = Some(registration);
        success_reply()
    }
}

#[tokio::test]
async fn cross_request_and_session_provenance_fails_before_responder_take() {
    let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let service = service_with_limits(controller.as_ref(), 2, 16 * 1024 * 1024, 2, 2);
    let state = Arc::new(Mutex::new(ProvenanceState::default()));
    let processor = ProvenanceProcessor {
        service: Arc::clone(&service),
        state: Arc::clone(&state),
    };
    let (mut first, address, running) = start_server(processor, Arc::clone(&controller)).await;
    first
        .send_command(request_command(41))
        .await
        .expect("send provenance source");
    assert_eq!(
        first
            .receive_command()
            .await
            .expect("source connection")
            .expect("source inline response")
            .code(),
        ResponseCode::Success as i32
    );
    assert_eq!(service.index_snapshot().reserved(), 1);
    assert_eq!(service.admission_snapshot().waiting_count(), 1);

    let mut second = Connection::new(TcpStream::connect(address).await.expect("connect second Pull session"));
    second
        .send_command(request_command(42))
        .await
        .expect("send provenance target from another session");
    let response = second
        .receive_command()
        .await
        .expect("target connection")
        .expect("target request retains its inline responder");
    assert_eq!(response.code(), ResponseCode::Success as i32);
    assert_eq!(response.opaque(), 42);
    assert!(state.lock().prepared.is_none());
    assert_released(&service);

    drop(second);
    running.finish().await;
}

#[derive(Clone)]
struct PreTakeProbeProcessor {
    service: Arc<PullDeferredService>,
    held: Arc<Mutex<Vec<PreparedPullRegistration>>>,
    observed: Arc<Mutex<Vec<PullDeferredPrepareRejectionKind>>>,
    zero_timeout: bool,
    retained: PullRetainedEstimate,
}

impl RequestProcessor for PreTakeProbeProcessor {
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let header = request
            .command()
            .decode_command_custom_header::<PullMessageRequestHeader>()?;
        let timeout = if self.zero_timeout {
            0
        } else {
            header.suspend_timeout_millis
        };
        match prepared(
            &self.service,
            request,
            PullSuspendTiming::new(current_millis(), tokio::time::Instant::now(), timeout),
            self.retained,
        ) {
            Ok(PullDeferredPrepareOutcome::Prepared(prepared)) => {
                self.held.lock().push(prepared);
                success_reply()
            }
            Ok(PullDeferredPrepareOutcome::Rejected(rejection)) => {
                self.observed.lock().push(rejection.kind());
                Ok(HandlerOutcome::Reply(rejection.into_fallback()))
            }
            Err(error) => Err(RocketMQError::illegal_argument(error.to_string())),
        }
    }
}

async fn exercise_second_pre_take_rejection(
    service: Arc<PullDeferredService>,
    controller: Arc<AdmissionController>,
    expected: PullDeferredPrepareRejectionKind,
) {
    let held = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let processor = PreTakeProbeProcessor {
        service: Arc::clone(&service),
        held: Arc::clone(&held),
        observed: Arc::clone(&observed),
        zero_timeout: false,
        retained: PullRetainedEstimate::default(),
    };
    let (mut client, _address, running) = start_server(processor, controller).await;
    for opaque in [51, 52] {
        client
            .send_command(request_command(opaque))
            .await
            .expect("send Pull pre-take probe");
        let response = client
            .receive_command()
            .await
            .expect("pre-take probe connection")
            .expect("inline pre-take probe response");
        assert_eq!(response.opaque(), opaque);
    }
    assert_eq!(&*observed.lock(), &[expected]);
    assert_eq!(held.lock().len(), 1);
    held.lock().clear();
    assert_released(&service);
    running.finish().await;
}

#[tokio::test]
async fn index_and_wait_capacity_reject_before_responder_transfer() {
    let index_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let index_service = service_with_limits(index_controller.as_ref(), 2, 16 * 1024 * 1024, 1, 1);
    exercise_second_pre_take_rejection(
        index_service,
        index_controller,
        PullDeferredPrepareRejectionKind::IndexCapacity,
    )
    .await;

    let wait_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
    let wait_service = service_with_limits(wait_controller.as_ref(), 1, 16 * 1024 * 1024, 2, 2);
    exercise_second_pre_take_rejection(
        wait_service,
        wait_controller,
        PullDeferredPrepareRejectionKind::AdmissionCapacity,
    )
    .await;
}

#[tokio::test]
async fn retained_bytes_and_inclusive_zero_deadline_fallback_inline() {
    for (retained_bytes, zero_timeout, expected, opaque) in [
        (1, false, PullDeferredPrepareRejectionKind::AdmissionCapacity, 61),
        (
            16 * 1024 * 1024,
            true,
            PullDeferredPrepareRejectionKind::DeadlineElapsed,
            62,
        ),
    ] {
        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = service_with_limits(controller.as_ref(), 1, retained_bytes, 1, 1);
        let held = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let processor = PreTakeProbeProcessor {
            service: Arc::clone(&service),
            held,
            observed: Arc::clone(&observed),
            zero_timeout,
            retained: PullRetainedEstimate::default(),
        };
        let (mut client, _address, running) = start_server(processor, Arc::clone(&controller)).await;
        client
            .send_command(request_command(opaque))
            .await
            .expect("send Pull pre-take deadline/byte probe");
        let response = client
            .receive_command()
            .await
            .expect("probe connection")
            .expect("pre-take fallback response");
        assert_eq!(response.opaque(), opaque);
        assert_eq!(response.code(), ResponseCode::PullNotFound as i32);
        assert_eq!(&*observed.lock(), &[expected]);
        assert_released(&service);
        running.finish().await;
    }
}
