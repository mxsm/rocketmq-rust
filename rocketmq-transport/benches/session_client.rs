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
use std::net::SocketAddr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::AdmissionController;
use rocketmq_transport::AdmissionLimits;
use rocketmq_transport::Connection;
use rocketmq_transport::DefaultRemotingRequestProcessor;
use rocketmq_transport::RemotingClient;
use rocketmq_transport::RemotingService;
use rocketmq_transport::RocketmqDefaultClient;
use rocketmq_transport::TokioClientConfig;
use rocketmq_transport::TransportClient;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

async fn echo_server(single_request: bool) -> (SocketAddr, Arc<AtomicUsize>, CancellationToken, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("benchmark listener");
    let address = listener.local_addr().expect("benchmark listener address");
    let accepts = Arc::new(AtomicUsize::new(0));
    let accept_counter = accepts.clone();
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.clone();
    let server = tokio::spawn(async move {
        let mut sessions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                () = server_cancellation.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((socket, _)) = accepted else {
                        break;
                    };
                    accept_counter.fetch_add(1, Ordering::Relaxed);
                    sessions.spawn(async move {
                        let mut connection = Connection::new(socket);
                        while let Some(Ok(request)) = connection.receive_command().await {
                            let response = RemotingCommand::create_response_command().set_opaque(request.opaque());
                            if connection.send_command(response).await.is_err() {
                                break;
                            }
                            if single_request {
                                break;
                            }
                        }
                    });
                }
            }
        }
        sessions.abort_all();
        while sessions.join_next().await.is_some() {}
    });
    (address, accepts, cancellation, server)
}

fn benchmark_session_client(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let (runtime_context, one_shot, persistent, baseline_server, candidate_server, target, accepts) =
        runtime.block_on(async {
            let (baseline_addr, baseline_accepts, baseline_cancel, baseline_task) = echo_server(true).await;
            let (candidate_addr, candidate_accepts, candidate_cancel, candidate_task) = echo_server(false).await;
            let runtime_context = RuntimeContext::from_current("transport-session-client-benchmark");
            let one_shot = TransportClient::new(
                runtime_context.service_context("one-shot-client"),
                Arc::new(AdmissionController::new(AdmissionLimits::default())),
            );
            let persistent = Arc::new(RocketmqDefaultClient::new(
                Arc::new(TokioClientConfig::default()),
                DefaultRemotingRequestProcessor,
                runtime_context.service_context("persistent-client"),
            ));
            let target = CheetahString::from_string(candidate_addr.to_string());
            persistent
                .invoke_request(
                    Some(&target),
                    RemotingCommand::create_remoting_command(10_100).set_opaque(0),
                    3_000,
                )
                .await
                .expect("warm persistent connection");
            (
                runtime_context,
                (one_shot, baseline_addr),
                persistent,
                (baseline_cancel, baseline_task),
                (candidate_cancel, candidate_task),
                target,
                (baseline_accepts, candidate_accepts),
            )
        });

    let next_opaque = AtomicI32::new(1);
    let mut group = c.benchmark_group("session_client_128b");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    let one_shot_client = &one_shot.0;
    let one_shot_address = one_shot.1;
    group.bench_function("one_shot_connect_per_rpc", |benchmark| {
        benchmark.to_async(&runtime).iter(|| {
            let opaque = next_opaque.fetch_add(1, Ordering::Relaxed);
            async move {
                let response = one_shot_client
                    .invoke(
                        one_shot_address,
                        RemotingCommand::create_remoting_command(10_100)
                            .set_opaque(opaque)
                            .set_body(vec![0x5a; 128]),
                        rocketmq_transport::RequestDeadline::after(Duration::from_secs(3)),
                    )
                    .await
                    .expect("one-shot benchmark request");
                black_box(response);
            }
        });
    });
    group.bench_function("persistent_session", |benchmark| {
        benchmark.to_async(&runtime).iter(|| {
            let client = persistent.clone();
            let target = target.clone();
            let opaque = next_opaque.fetch_add(1, Ordering::Relaxed);
            async move {
                let response = client
                    .invoke_request(
                        Some(&target),
                        RemotingCommand::create_remoting_command(10_100)
                            .set_opaque(opaque)
                            .set_body(vec![0x5a; 128]),
                        3_000,
                    )
                    .await
                    .expect("persistent benchmark request");
                black_box(response);
            }
        });
    });
    group.finish();

    runtime.block_on(async {
        persistent.shutdown();
        baseline_server.0.cancel();
        candidate_server.0.cancel();
        baseline_server.1.await.expect("baseline echo server");
        candidate_server.1.await.expect("candidate echo server");
    });
    black_box(accepts.0.load(Ordering::Relaxed));
    black_box(accepts.1.load(Ordering::Relaxed));
    drop(one_shot);
    drop(persistent);
    runtime.block_on(async {
        let report = runtime_context.shutdown_tasks(Duration::from_secs(1)).await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
}

criterion_group!(benches, benchmark_session_client);
criterion_main!(benches);
