// Copyright 2023 The RocketMQ Rust Authors
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

use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use rocketmq_remoting::remoting_server::rocketmq_tokio_server::run_with_report;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_runtime::ShutdownReport;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

#[derive(Debug)]
struct RemotingLifecycleOutput {
    connection_count: usize,
    elapsed: Duration,
    report: ShutdownReport,
}

fn run_remoting_lifecycle(connection_count: usize) -> RemotingLifecycleOutput {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .thread_name("rocketmq-remoting-lifecycle-bench")
        .enable_all()
        .build()
        .expect("remoting lifecycle benchmark runtime should start");

    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("benchmark listener should bind");
        let addr = listener.local_addr().expect("benchmark listener should expose address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let started_at = Instant::now();
        let server = tokio::spawn(run_with_report(
            listener,
            async {
                let _ = shutdown_rx.await;
            },
            DefaultRemotingRequestProcessor,
            None,
            Vec::new(),
            None,
        ));

        let mut clients = Vec::with_capacity(connection_count);
        for _ in 0..connection_count {
            clients.push(TcpStream::connect(addr).await.expect("benchmark client should connect"));
        }
        drop(clients);

        let _ = shutdown_tx.send(());
        let report = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("remoting server should drain before timeout")
            .expect("remoting server task should not panic")
            .expect("remoting server should return shutdown report");
        assert!(report.is_healthy(), "{}", report.to_json());

        RemotingLifecycleOutput {
            connection_count,
            elapsed: started_at.elapsed(),
            report,
        }
    })
}

fn write_remoting_lifecycle_report_artifact() {
    let output = run_remoting_lifecycle(64);
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("rocketmq-remoting should live below workspace root")
        .to_path_buf();
    let output_dir = workspace_root.join("target/runtime-baseline/prototype");
    fs::create_dir_all(&output_dir).expect("runtime benchmark artifact directory should be created");

    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis();
    let payload = serde_json::json!({
        "case": "remoting_connection_lifecycle",
        "generated_at_unix_ms": generated_at_unix_ms,
        "connection_count": output.connection_count,
        "elapsed_ms": output.elapsed.as_millis(),
        "elapsed_us": output.elapsed.as_micros(),
        "healthy": output.report.is_healthy(),
        "shutdown_report": output.report,
    });
    let path = output_dir.join("remoting-connection-lifecycle-report.json");
    fs::write(
        path,
        serde_json::to_vec_pretty(&payload).expect("remoting lifecycle benchmark artifact should serialize"),
    )
    .expect("remoting lifecycle benchmark artifact should be written");
}

fn bench_remoting_connection_lifecycle(criterion: &mut Criterion) {
    write_remoting_lifecycle_report_artifact();

    let mut group = criterion.benchmark_group("remoting_connection_lifecycle");
    for connection_count in [16usize, 64] {
        group.bench_with_input(
            BenchmarkId::new("open_close_shutdown", connection_count),
            &connection_count,
            |bencher, connection_count| {
                bencher.iter(|| {
                    let output = run_remoting_lifecycle(black_box(*connection_count));
                    black_box(output.elapsed);
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1));
    targets = bench_remoting_connection_lifecycle
}
criterion_main!(benches);
