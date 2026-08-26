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

#![cfg(feature = "test-support")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::RuntimeContext;
use rocketmq_security_api::Principal;
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::AuthorizedCommandDispatcher;
use rocketmq_transport::api::v1::Channel;
use rocketmq_transport::api::v1::ConnectionHandlerContext;
use rocketmq_transport::api::v1::RPCHook;
use rocketmq_transport::api::v1::RequestContext;
use rocketmq_transport::api::v1::RequestOrdering;
use rocketmq_transport::api::v1::RequestOrderingKey;
use rocketmq_transport::api::v1::RequestProcessor;
use rocketmq_transport::api::v1::ResponseWriteObservation;
use rocketmq_transport::api::v1::ResponseWriteOutcome;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::TransportSecurity;
use rocketmq_transport::api::v1::TransportServer;
use rocketmq_transport::api::v1::TransportTelemetry;
use rocketmq_transport::test_support::Connection;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

const REJECT: i32 = 19_748;
const STANDARD: i32 = 19_749;
const ONEWAY: i32 = 19_750;
const NONE: i32 = 19_751;
const DIRECT_WRITE: i32 = 19_752;
const SENTINEL: i32 = 19_753;
const ORIGINAL_OPAQUE: i32 = 74;
const MUTATED_OPAQUE: i32 = 8_074;
const PROCESSOR_RESPONSE_OPAQUE: i32 = 9_074;
const ORDERING_KEY: RequestOrderingKey = RequestOrderingKey::new(9748);

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Ordering {
        code: i32,
        opaque: i32,
    },
    Reject {
        code: i32,
    },
    Before {
        code: i32,
        opaque: i32,
    },
    Process {
        code: i32,
        opaque: i32,
    },
    After {
        code: i32,
        request_opaque: i32,
        response_opaque: i32,
    },
    Observe {
        request_code: i32,
        response_code: i32,
        outcome: ResponseWriteOutcome,
    },
}

impl Event {
    fn request_code(&self) -> i32 {
        match self {
            Self::Ordering { code, .. }
            | Self::Reject { code }
            | Self::Before { code, .. }
            | Self::Process { code, .. }
            | Self::After { code, .. } => *code,
            Self::Observe { request_code, .. } => *request_code,
        }
    }
}

#[derive(Clone, Default)]
struct EventLog(Arc<Mutex<Vec<Event>>>);

impl EventLog {
    fn push(&self, event: Event) {
        self.0
            .lock()
            .expect("event log lock should not be poisoned")
            .push(event);
    }

    fn snapshot(&self) -> Vec<Event> {
        self.0.lock().expect("event log lock should not be poisoned").clone()
    }
}

#[derive(Clone)]
struct ContractProcessor {
    events: EventLog,
}

impl RequestProcessor for ContractProcessor {
    async fn process_request(
        &mut self,
        _channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.events.push(Event::Process {
            code: request.code(),
            opaque: request.opaque(),
        });
        match request.code() {
            NONE => Ok(None),
            DIRECT_WRITE => {
                ctx.write_response(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque())
                        .set_body(b"direct".to_vec()),
                )
                .await;
                Ok(None)
            }
            _ => Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                    .set_opaque(PROCESSOR_RESPONSE_OPAQUE),
            )),
        }
    }

    fn reject_request(&self, code: i32) -> (bool, Option<RemotingCommand>) {
        if code == REJECT {
            self.events.push(Event::Reject { code });
            (
                true,
                Some(RemotingCommand::create_response_command_with_code(
                    ResponseCode::SystemBusy,
                )),
            )
        } else {
            (false, None)
        }
    }

    fn request_ordering(&self, request: &RemotingCommand) -> RequestOrdering {
        self.events.push(Event::Ordering {
            code: request.code(),
            opaque: request.opaque(),
        });
        RequestOrdering::Ordered(ORDERING_KEY)
    }

    fn observe_response_write(&self, observation: ResponseWriteObservation) {
        self.events.push(Event::Observe {
            request_code: observation.request_code,
            response_code: observation.response_code,
            outcome: observation.outcome,
        });
    }
}

struct ContractHook {
    events: EventLog,
}

impl RPCHook for ContractHook {
    fn do_before_request(
        &self,
        _remote_addr: SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.events.push(Event::Before {
            code: request.code(),
            opaque: request.opaque(),
        });
        if request.code() == STANDARD {
            request.set_opaque_mut(MUTATED_OPAQUE);
        }
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        request: &RemotingCommand,
        response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.events.push(Event::After {
            code: request.code(),
            request_opaque: request.opaque(),
            response_opaque: response.opaque(),
        });
        Ok(())
    }
}

struct Fixture {
    service: ChildServiceContext,
    processor: ContractProcessor,
    dispatcher: Arc<AuthorizedCommandDispatcher<ContractProcessor>>,
    security: Arc<TransportSecurity>,
    events: EventLog,
}

fn fixture(runtime: &RuntimeContext, name: &'static str) -> Fixture {
    let service = runtime.service_context(name);
    let process_budget = service.process_budget();
    let events = EventLog::default();
    let processor = ContractProcessor { events: events.clone() };
    let security = Arc::new(TransportSecurity::development_insecure_loopback(None, None));
    let admission = Arc::new(
        AdmissionController::try_new_with_budget(AdmissionLimits::default(), &process_budget)
            .expect("contract-test admission limits should be valid"),
    );
    let dispatcher = Arc::new(
        AuthorizedCommandDispatcher::try_new(
            processor.clone(),
            vec![Arc::new(ContractHook { events: events.clone() })],
            &process_budget,
            TransportTelemetry::noop(),
            Arc::clone(&security),
            admission,
        )
        .expect("contract-test dispatcher should fit the process budget"),
    );
    Fixture {
        service,
        processor,
        dispatcher,
        security,
        events,
    }
}

fn request(code: i32, opaque: i32) -> RemotingCommand {
    RemotingCommand::create_remoting_command(code).set_opaque(opaque)
}

async fn dispatch_embedded(fixture: &Fixture, command: RemotingCommand) -> RemotingCommand {
    fixture
        .dispatcher
        .dispatch_embedded(
            fixture.service.task_group(),
            RequestContext::try_embedded(Some(Principal::new("contract-test")), None)
                .expect("embedded test identity should be valid"),
            command,
        )
        .await
        .expect("embedded dispatch should produce a response")
}

async fn run_network(
    fixture: &Fixture,
    commands: Vec<RemotingCommand>,
    expected_frames: usize,
) -> Vec<RemotingCommand> {
    let config = Arc::new(ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: 0,
        ..ServerConfig::default()
    });
    let mut server = TransportServer::new(config, fixture.service.component("network"))
        .with_transport_security(Arc::clone(&fixture.security), Some(Principal::new("contract-test")))
        .with_authorized_dispatcher(Arc::clone(&fixture.dispatcher));
    let (startup_tx, startup_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let processor = fixture.processor.clone();
    let server_task = tokio::spawn(async move {
        server
            .run_with_shutdown_report_and_startup(
                processor,
                None,
                async {
                    let _ = shutdown_rx.await;
                },
                startup_tx,
            )
            .await
    });
    let address = startup_rx
        .await
        .expect("server should report startup")
        .expect("server should bind to an ephemeral port");
    let mut connection = Connection::new(TcpStream::connect(address).await.expect("client should connect"));
    for command in commands {
        connection
            .send_command(command)
            .await
            .expect("request frame should be written");
    }

    let mut responses = Vec::new();
    let initial_reads = tokio::time::timeout(Duration::from_secs(2), async {
        for _ in 0..expected_frames {
            let response = connection
                .receive_command()
                .await
                .expect("server should keep the session open")
                .expect("response frame should decode");
            responses.push(response);
        }
    })
    .await;

    let _ = shutdown_tx.send(());
    let report = tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("server should shut down before the contract-test deadline")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.is_healthy(), "{}", report.to_json());

    while let Some(response) = connection.receive_command().await {
        responses.push(response.expect("response frame should decode during shutdown"));
    }
    initial_reads.expect("expected response frames should arrive before shutdown");
    responses
}

fn successful_response_events(code: i32, ingress_opaque: i32, processed_opaque: i32) -> Vec<Event> {
    vec![
        Event::Ordering {
            code,
            opaque: ingress_opaque,
        },
        Event::Before {
            code,
            opaque: ingress_opaque,
        },
        Event::Process {
            code,
            opaque: processed_opaque,
        },
        Event::After {
            code,
            request_opaque: processed_opaque,
            response_opaque: PROCESSOR_RESPONSE_OPAQUE,
        },
        Event::Observe {
            request_code: code,
            response_code: ResponseCode::Success.to_i32(),
            outcome: ResponseWriteOutcome::Sent,
        },
    ]
}

#[tokio::test]
async fn standard_dispatch_preserves_v1_ordering_and_opaque_for_embedded_and_network_adapters() {
    let runtime = RuntimeContext::from_current("v1-standard-processor-contract");

    let embedded = fixture(&runtime, "embedded");
    let embedded_response = dispatch_embedded(&embedded, request(STANDARD, ORIGINAL_OPAQUE)).await;
    assert_eq!(embedded_response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(
        embedded.events.snapshot(),
        successful_response_events(STANDARD, ORIGINAL_OPAQUE, MUTATED_OPAQUE)
    );

    let network = fixture(&runtime, "network");
    let responses = run_network(&network, vec![request(STANDARD, ORIGINAL_OPAQUE)], 1).await;
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].opaque(), ORIGINAL_OPAQUE);
    assert_eq!(
        network.events.snapshot(),
        successful_response_events(STANDARD, ORIGINAL_OPAQUE, MUTATED_OPAQUE)
    );
}

#[tokio::test]
async fn reject_short_circuits_hooks_and_processor_for_embedded_and_network_adapters() {
    let runtime = RuntimeContext::from_current("v1-reject-processor-contract");
    let expected = vec![
        Event::Ordering {
            code: REJECT,
            opaque: ORIGINAL_OPAQUE,
        },
        Event::Reject { code: REJECT },
        Event::Observe {
            request_code: REJECT,
            response_code: ResponseCode::SystemBusy.to_i32(),
            outcome: ResponseWriteOutcome::Sent,
        },
    ];

    let embedded = fixture(&runtime, "embedded");
    let embedded_response = dispatch_embedded(&embedded, request(REJECT, ORIGINAL_OPAQUE)).await;
    assert_eq!(embedded_response.opaque(), ORIGINAL_OPAQUE);
    assert_eq!(embedded.events.snapshot(), expected);

    let network = fixture(&runtime, "network");
    let responses = run_network(&network, vec![request(REJECT, ORIGINAL_OPAQUE)], 1).await;
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].opaque(), ORIGINAL_OPAQUE);
    assert_eq!(network.events.snapshot(), expected);
}

#[tokio::test]
async fn network_oneway_and_none_results_do_not_emit_central_responses_or_observations() {
    let runtime = RuntimeContext::from_current("v1-no-central-response-contract");

    let oneway = fixture(&runtime, "oneway");
    let responses = run_network(
        &oneway,
        vec![request(ONEWAY, 1).mark_oneway_rpc(), request(SENTINEL, 2)],
        1,
    )
    .await;
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].opaque(), 2);
    let events = oneway.events.snapshot();
    let oneway_events = events
        .iter()
        .filter(|event| event.request_code() == ONEWAY)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        oneway_events,
        vec![
            Event::Ordering {
                code: ONEWAY,
                opaque: 1,
            },
            Event::Before {
                code: ONEWAY,
                opaque: 1,
            },
            Event::Process {
                code: ONEWAY,
                opaque: 1,
            },
            Event::After {
                code: ONEWAY,
                request_opaque: 1,
                response_opaque: PROCESSOR_RESPONSE_OPAQUE,
            },
        ]
    );
    assert!(!oneway_events.iter().any(|event| matches!(
        event,
        Event::Observe {
            request_code: ONEWAY,
            ..
        }
    )));
    let sentinel_events = events
        .iter()
        .filter(|event| event.request_code() == SENTINEL)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(sentinel_events, successful_response_events(SENTINEL, 2, 2));

    let none = fixture(&runtime, "none");
    let responses = run_network(&none, vec![request(NONE, 3), request(SENTINEL, 4)], 1).await;
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].opaque(), 4);
    let events = none.events.snapshot();
    let none_events = events
        .iter()
        .filter(|event| event.request_code() == NONE)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        none_events,
        vec![
            Event::Ordering { code: NONE, opaque: 3 },
            Event::Before { code: NONE, opaque: 3 },
            Event::Process { code: NONE, opaque: 3 },
        ]
    );
    assert!(!none_events.iter().any(|event| matches!(
        event,
        Event::After { code: NONE, .. } | Event::Observe { request_code: NONE, .. }
    )));
    let sentinel_events = events
        .iter()
        .filter(|event| event.request_code() == SENTINEL)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(sentinel_events, successful_response_events(SENTINEL, 4, 4));
}

#[tokio::test]
async fn direct_context_write_followed_by_none_emits_exactly_one_processor_frame() {
    let runtime = RuntimeContext::from_current("v1-direct-write-processor-contract");
    let fixture = fixture(&runtime, "network");
    let responses = run_network(&fixture, vec![request(DIRECT_WRITE, 5), request(SENTINEL, 6)], 2).await;

    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0].opaque(), 5);
    assert_eq!(
        responses[0].body().map(|body| body.as_ref()),
        Some(b"direct".as_slice())
    );
    assert_eq!(responses[1].opaque(), 6);
    assert!(!fixture.events.snapshot().iter().any(|event| matches!(
        event,
        Event::Observe {
            request_code: DIRECT_WRITE,
            ..
        }
    )));
}
