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

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_security_api::Principal;
use rocketmq_transport::api::v2::AdmissionController;
use rocketmq_transport::api::v2::AdmissionLimits;
use rocketmq_transport::api::v2::AuthorizedCommandDispatcherV2;
use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::ResponsePlan;
use rocketmq_transport::api::v2::TransportSecurity;

#[path = "support/criterion_profile.rs"]
mod criterion_profile;

use criterion_profile::apply_remoting_command_baseline_profile;

#[derive(Clone, Copy)]
struct InlineReplyProcessor;

impl RequestProcessorV2 for InlineReplyProcessor {
    async fn process(&mut self, _request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(ResponsePlan::empty_response(0)))
    }
}

fn request(body_bytes: usize) -> RemotingCommand {
    RemotingCommand::create_remoting_command(39)
        .set_opaque(811)
        .set_body(vec![0x5a; body_bytes])
}

fn benchmark_processor_dispatch(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("processor-dispatch benchmark runtime");
    let (runtime_context, service, dispatcher) = runtime.block_on(async {
        let runtime_context = RuntimeContext::from_current("remoting-v2-processor-dispatch-benchmark");
        let service = runtime_context.service_context("dispatcher");
        let dispatcher = Arc::new(AuthorizedCommandDispatcherV2::new(
            InlineReplyProcessor,
            Vec::new(),
            Arc::new(TransportSecurity::development_insecure_loopback(None, None)),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        ));
        (runtime_context, service, dispatcher)
    });

    let principal = Principal::new("benchmark-broker-proxy");
    let mut group = criterion.benchmark_group("transport_processor_dispatch");
    apply_remoting_command_baseline_profile(&mut group);
    for body_bytes in [0, 128, 4 * 1024] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("legacy_materialized_contract_reference", body_bytes),
            &body_bytes,
            |bencher, body_bytes| {
                bencher.iter_batched(
                    || request(*body_bytes),
                    |mut command| {
                        let opaque = command.opaque();
                        let response_body = command
                            .take_body()
                            .map(|body| bytes::Bytes::copy_from_slice(body.as_ref()));
                        let response = match response_body {
                            Some(body) => RemotingCommand::create_success_response_command()
                                .set_opaque(opaque)
                                .set_body(body),
                            None => RemotingCommand::create_success_response_command().set_opaque(opaque),
                        };
                        black_box(response);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("canonical_v2_embedded_inline", body_bytes),
            &body_bytes,
            |bencher, body_bytes| {
                bencher.to_async(&runtime).iter_batched(
                    || request(*body_bytes),
                    |command| {
                        let dispatcher = Arc::clone(&dispatcher);
                        let task_group = service.task_group().clone();
                        let principal = principal.clone();
                        async move {
                            let outcome = dispatcher
                                .dispatch_embedded_v2(&task_group, principal, None, command)
                                .await
                                .expect("canonical embedded dispatch");
                            match outcome {
                                EmbeddedDispatchOutcome::Reply(plan) => {
                                    black_box((plan.response_code(), plan.body_len()));
                                }
                                other => panic!("inline benchmark returned an unexpected outcome: {other:?}"),
                            }
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();

    runtime.block_on(async {
        drop(dispatcher);
        drop(service);
        let report = runtime_context.shutdown_tasks(Duration::from_secs(3)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
}

criterion_group!(benches, benchmark_processor_dispatch);
criterion_main!(benches);
