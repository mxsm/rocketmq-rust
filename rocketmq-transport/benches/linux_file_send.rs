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

#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
mod linux_bench {
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use criterion::BenchmarkId;
    use criterion::Criterion;
    use criterion::Throughput;
    use rocketmq_protocol::protocol::RemotingCommand;
    use rocketmq_runtime::BlockingExecutor;
    use rocketmq_runtime::RuntimeConfig;
    use rocketmq_runtime::RuntimeOwner;
    use rocketmq_transport::api::v1::FileRegion;
    use rocketmq_transport::api::v1::FileTransferMode;
    use rocketmq_transport::api::v1::RequestDeadline;
    use rocketmq_transport::benchmark_support::Connection;
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind loopback");
        let address = listener.local_addr().expect("listener address");
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        (client.expect("connect loopback"), accepted.expect("accept loopback").0)
    }

    fn make_region(size: usize) -> FileRegion {
        let mut file = tempfile::tempfile().expect("temporary benchmark file");
        let block = vec![0x5A; 64 * 1024];
        let mut remaining = size;
        while remaining != 0 {
            let count = remaining.min(block.len());
            file.write_all(&block[..count]).expect("write benchmark file");
            remaining -= count;
        }
        file.flush().expect("flush benchmark file");
        FileRegion::try_new(Arc::new(file), 0, size as u64).expect("valid benchmark region")
    }

    async fn run_iterations(
        iterations: u64,
        region: FileRegion,
        blocking: BlockingExecutor,
        mode: FileTransferMode,
    ) -> Duration {
        let (client, accepted) = tcp_pair().await;
        let mut sender = Connection::new(client).with_file_region_io(blocking, mode);
        let mut receiver = Connection::new(accepted);
        let started = Instant::now();
        let send = async {
            for opaque in 0..iterations {
                sender
                    .send_file_region_command(
                        RemotingCommand::create_remoting_command(501).set_opaque(opaque as i32),
                        region.clone(),
                        RequestDeadline::after(Duration::from_secs(30)),
                    )
                    .await
                    .expect("file frame send");
            }
        };
        let receive = async {
            for _ in 0..iterations {
                receiver
                    .receive_command()
                    .await
                    .expect("peer frame")
                    .expect("decode peer frame");
            }
        };
        tokio::join!(send, receive);
        started.elapsed()
    }

    pub fn benchmark(c: &mut Criterion) {
        let owner = RuntimeOwner::new(RuntimeConfig::default()).expect("benchmark runtime owner");
        let blocking = owner
            .root_context()
            .component("linux-file-send-bench")
            .storage_io()
            .clone();
        let mut group = c.benchmark_group("linux_file_send_loopback");
        group.sample_size(10);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(5));

        for size in [64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
            let region = make_region(size);
            group.throughput(Throughput::Bytes(size as u64));
            for (name, mode) in [
                ("portable", FileTransferMode::Portable),
                ("sendfile", FileTransferMode::Sendfile),
            ] {
                group.bench_with_input(BenchmarkId::new(name, size), &size, |bencher, _| {
                    bencher.iter_custom(|iterations| {
                        owner.block_on(run_iterations(iterations, region.clone(), blocking.clone(), mode))
                    });
                });
            }
        }
        group.finish();
    }
}

#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
criterion::criterion_group!(benches, linux_bench::benchmark);
#[cfg(all(target_os = "linux", feature = "linux-sendfile"))]
criterion::criterion_main!(benches);

#[cfg(not(all(target_os = "linux", feature = "linux-sendfile")))]
fn main() {}
