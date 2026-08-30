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

#![recursion_limit = "256"]
#![cfg(windows)]

use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::process::Child;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::ConsumeConcurrentlyContext;
use rocketmq_client_rust::ConsumeConcurrentlyStatus;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::DefaultMQPushConsumer;
use rocketmq_client_rust::MQPushConsumer;
use rocketmq_client_rust::MessageListenerConcurrently;
use rocketmq_error::RocketMQResult;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_namesrv::bootstrap::Builder as NameServerBuilder;
use rocketmq_namesrv::NamesrvConfig;
use rocketmq_observability::TelemetryHandle;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_transport::api::ServerConfig;
use tokio::sync::oneshot;

const CHILD_MODE_ENV: &str = "ROCKETMQ_CLIENT_STACK_TEST_CHILD";
const PRODUCER_CHILD_MODE_ENV: &str = "ROCKETMQ_CLIENT_PRODUCER_STACK_TEST_CHILD";
const NAMESRV_ADDR_ENV: &str = "ROCKETMQ_CLIENT_STACK_TEST_NAMESRV_ADDR";
const STARTUP_MARKER: &str = "CLIENT_STACK_STARTUP_OK";
const SHUTDOWN_MARKER: &str = "CLIENT_STACK_SHUTDOWN_OK";
const PRODUCER_STARTUP_MARKER: &str = "CLIENT_PRODUCER_STACK_STARTUP_OK";
const PRODUCER_SHUTDOWN_MARKER: &str = "CLIENT_PRODUCER_STACK_SHUTDOWN_OK";
const WINDOWS_MAIN_STACK_SIZE: usize = 1024 * 1024;
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

struct ClientProcess {
    child: Option<Child>,
}

impl ClientProcess {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("consumer process should be present")
    }

    fn wait_with_output(mut self) -> Output {
        self.child
            .take()
            .expect("consumer process should be present")
            .wait_with_output()
            .expect("collect consumer process output")
    }

    fn terminate_with_output(mut self) -> Output {
        let mut child = self.child.take().expect("consumer process should be present");
        if child.try_wait().expect("inspect consumer process status").is_none() {
            child.kill().expect("terminate consumer process");
        }
        child.wait_with_output().expect("collect consumer process output")
    }
}

impl Drop for ClientProcess {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
        }
        let _ = child.wait();
    }
}

struct NoopMessageListener;

impl MessageListenerConcurrently for NoopMessageListener {
    fn consume_message(
        &self,
        _messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> RocketMQResult<ConsumeConcurrentlyStatus> {
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

fn available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve NameServer listener")
        .local_addr()
        .expect("NameServer listener address")
        .port()
}

fn process_output(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

async fn start_namesrv(root: &std::path::Path, port: u16) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let namesrv_root = root.join("namesrv");
    std::fs::create_dir_all(&namesrv_root).expect("create NameServer root");
    let namesrv_config = NamesrvConfig {
        rocketmq_home: root.to_string_lossy().into_owned(),
        kv_config_path: namesrv_root.join("kvConfig.json").to_string_lossy().into_owned(),
        config_store_path: namesrv_root.join("namesrv.properties").to_string_lossy().into_owned(),
        ..NamesrvConfig::default()
    };
    let server_config = ServerConfig {
        bind_address: "127.0.0.1".to_owned(),
        listen_port: u32::from(port),
        ..ServerConfig::default()
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service_context = RuntimeContext::from_current("client-process-stack-test").service_context("namesrv");
    let handle = tokio::spawn(async move {
        NameServerBuilder::new(service_context, TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .build()
            .boot_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("run test NameServer");
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "NameServer did not start listening");
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    (shutdown_tx, handle)
}

fn run_consumer_child() {
    let namesrv_addr = std::env::var(NAMESRV_ADDR_ENV).expect("child NameServer address should be present");
    let thread = std::thread::Builder::new()
        .name("rocketmq-client-stack-probe".to_owned())
        .stack_size(WINDOWS_MAIN_STACK_SIZE)
        .spawn(move || {
            let owner = RuntimeOwner::new(RuntimeConfig {
                worker_threads: 2,
                thread_name: "rocketmq-client-stack-test".to_owned(),
                ..RuntimeConfig::default()
            })
            .expect("create client process runtime");
            let client_runtime = ClientRuntime::try_new(
                owner.root_context().component("client"),
                ClientRuntimeConfig::default(),
                TelemetryHandle::noop(),
            )
            .expect("create client runtime");
            owner.block_on(run_consumer_startup(Arc::clone(&client_runtime), namesrv_addr));
            let report = owner.block_on(client_runtime.shutdown());
            assert!(report.is_healthy(), "{}", report.to_json());
            owner
                .shutdown_runtime_blocking()
                .expect("consumer process runtime should stop cleanly");
        })
        .expect("start 1 MiB consumer startup thread");

    thread.join().expect("consumer startup thread should not panic");
}

fn run_producer_child() {
    let namesrv_addr = std::env::var(NAMESRV_ADDR_ENV).expect("child NameServer address should be present");
    let thread = std::thread::Builder::new()
        .name("rocketmq-client-producer-stack-probe".to_owned())
        .stack_size(WINDOWS_MAIN_STACK_SIZE)
        .spawn(move || {
            let owner = RuntimeOwner::new(RuntimeConfig {
                worker_threads: 2,
                thread_name: "rocketmq-client-producer-stack-test".to_owned(),
                ..RuntimeConfig::default()
            })
            .expect("create producer client process runtime");
            let client_runtime = ClientRuntime::try_new(
                owner.root_context().component("client"),
                ClientRuntimeConfig::default(),
                TelemetryHandle::noop(),
            )
            .expect("create producer client runtime");
            owner.block_on(run_producer_startup(Arc::clone(&client_runtime), namesrv_addr));
            let report = owner.block_on(client_runtime.shutdown());
            assert!(report.is_healthy(), "{}", report.to_json());
            owner
                .shutdown_runtime_blocking()
                .expect("producer process runtime should stop cleanly");
        })
        .expect("start 1 MiB producer startup thread");

    thread.join().expect("producer startup thread should not panic");
}

async fn run_consumer_startup(client_runtime: Arc<ClientRuntime>, namesrv_addr: String) {
    let mut consumer = DefaultMQPushConsumer::builder(client_runtime)
        .consumer_group(format!("windows_stack_test_{}", std::process::id()))
        .name_server_addr(namesrv_addr)
        .build();
    consumer
        .subscribe("WindowsStackOverflowTest", "*")
        .await
        .expect("subscribe test topic");
    consumer.register_message_listener_concurrently(NoopMessageListener);
    consumer.start().await.expect("start test consumer");
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("{STARTUP_MARKER}");
    std::io::stdout().flush().expect("flush startup marker");

    consumer.shutdown().await;
    println!("{SHUTDOWN_MARKER}");
    std::io::stdout().flush().expect("flush shutdown marker");
}

async fn run_producer_startup(client_runtime: Arc<ClientRuntime>, namesrv_addr: String) {
    let mut producer = DefaultMQProducer::builder(client_runtime)
        .producer_group(format!("windows_producer_stack_test_{}", std::process::id()))
        .name_server_addr(namesrv_addr)
        .build();
    producer.start().await.expect("start test producer");

    println!("{PRODUCER_STARTUP_MARKER}");
    std::io::stdout().flush().expect("flush producer startup marker");

    producer.shutdown().await;
    println!("{PRODUCER_SHUTDOWN_MARKER}");
    std::io::stdout().flush().expect("flush producer shutdown marker");
}

async fn assert_client_process_startup(
    child_mode_env: &'static str,
    test_name: &'static str,
    startup_marker: &'static str,
    shutdown_marker: &'static str,
    client_kind: &'static str,
) {
    let root = tempfile::tempdir().expect("create client process root");
    let namesrv_port = available_port();
    let namesrv_addr = format!("127.0.0.1:{namesrv_port}");
    let (namesrv_shutdown, namesrv_handle) = start_namesrv(root.path(), namesrv_port).await;
    let child = Command::new(std::env::current_exe().expect("resolve client stack test executable"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(child_mode_env, "1")
        .env(NAMESRV_ADDR_ENV, &namesrv_addr)
        .env("RUST_LOG", "warn")
        .env_remove("NAMESRV_ADDR")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("start {client_kind} process: {error}"));
    let mut client = ClientProcess::new(child);

    let deadline = Instant::now() + STARTUP_TIMEOUT;
    let output = loop {
        if client
            .child_mut()
            .try_wait()
            .expect("inspect client startup status")
            .is_some()
        {
            break client.wait_with_output();
        }
        if Instant::now() >= deadline {
            let output = client.terminate_with_output();
            panic!("{client_kind} startup timed out:\n{}", process_output(&output));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    assert!(
        output.status.success(),
        "{client_kind} exited before startup completed:\n{}",
        process_output(&output)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(startup_marker),
        "{client_kind} did not emit startup marker:\n{}",
        process_output(&output)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(shutdown_marker),
        "{client_kind} did not complete clean shutdown:\n{}",
        process_output(&output)
    );

    namesrv_shutdown.send(()).expect("request NameServer shutdown");
    tokio::time::timeout(Duration::from_secs(10), namesrv_handle)
        .await
        .expect("NameServer shutdown should be bounded")
        .expect("NameServer task should not panic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windows_consumer_starts_without_stack_overflow() {
    if std::env::var_os(CHILD_MODE_ENV).is_some() {
        run_consumer_child();
        return;
    }

    assert_client_process_startup(
        CHILD_MODE_ENV,
        "windows_consumer_starts_without_stack_overflow",
        STARTUP_MARKER,
        SHUTDOWN_MARKER,
        "Consumer",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windows_producer_starts_without_stack_overflow() {
    if std::env::var_os(PRODUCER_CHILD_MODE_ENV).is_some() {
        run_producer_child();
        return;
    }

    assert_client_process_startup(
        PRODUCER_CHILD_MODE_ENV,
        "windows_producer_starts_without_stack_overflow",
        PRODUCER_STARTUP_MARKER,
        PRODUCER_SHUTDOWN_MARKER,
        "Producer",
    )
    .await;
}
