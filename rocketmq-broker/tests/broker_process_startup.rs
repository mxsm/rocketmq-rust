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

#![recursion_limit = "512"]
#![cfg(windows)]

use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::process::Child;
use std::process::Command;
use std::process::Output;
use std::process::Stdio;
use std::time::Duration;
use std::time::Instant;

use rocketmq_namesrv::bootstrap::Builder as NameServerBuilder;
use rocketmq_namesrv::NamesrvConfig;
use rocketmq_runtime::RuntimeContext;
use rocketmq_transport::api::v1::ServerConfig;
use tokio::sync::oneshot;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

struct BrokerProcess {
    child: Option<Child>,
}

impl BrokerProcess {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("broker process should be present")
    }

    fn wait_with_output(mut self) -> Output {
        self.child
            .take()
            .expect("broker process should be present")
            .wait_with_output()
            .expect("collect broker process output")
    }

    fn terminate_with_output(mut self) -> Output {
        let mut child = self.child.take().expect("broker process should be present");
        if child.try_wait().expect("inspect broker process status").is_none() {
            child.kill().expect("terminate broker process");
        }
        child.wait_with_output().expect("collect broker process output")
    }
}

impl Drop for BrokerProcess {
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

fn available_ports() -> (u16, u16, u16, u16) {
    for _ in 0..100 {
        let broker = TcpListener::bind("127.0.0.1:0").expect("reserve broker listener");
        let broker_port = broker.local_addr().expect("broker listener address").port();
        let Some(fast_port) = broker_port.checked_sub(2) else {
            continue;
        };
        let Ok(fast) = TcpListener::bind(("127.0.0.1", fast_port)) else {
            continue;
        };
        let ha = TcpListener::bind("127.0.0.1:0").expect("reserve HA listener");
        let health = TcpListener::bind("127.0.0.1:0").expect("reserve health listener");
        let namesrv = TcpListener::bind("127.0.0.1:0").expect("reserve NameServer listener");
        let ha_port = ha.local_addr().expect("HA listener address").port();
        let health_port = health.local_addr().expect("health listener address").port();
        let namesrv_port = namesrv.local_addr().expect("NameServer listener address").port();

        drop(namesrv);
        drop(health);
        drop(ha);
        drop(fast);
        drop(broker);
        return (broker_port, ha_port, health_port, namesrv_port);
    }
    panic!("unable to reserve Broker listener ports");
}

fn probe(addr: SocketAddr, path: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(250))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
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
    let service_context = RuntimeContext::from_current("broker-process-stack-test").service_context("namesrv");
    let handle = tokio::spawn(async move {
        NameServerBuilder::new(service_context, rocketmq_observability::TelemetryHandle::noop())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windows_broker_reaches_readiness_without_main_stack_overflow() {
    let root = tempfile::tempdir().expect("create broker process root");
    let store_root = root.path().join("store");
    let config_path = root.path().join("broker.toml");
    let store_root_config = store_root.to_string_lossy().replace('\\', "/");
    let (broker_port, ha_port, health_port, namesrv_port) = available_ports();
    let health_addr = SocketAddr::from(([127, 0, 0, 1], health_port));
    let (namesrv_shutdown, namesrv_handle) = start_namesrv(root.path(), namesrv_port).await;
    let config = format!(
        r#"[broker]
brokerIp1 = "127.0.0.1"
listenPort = {broker_port}
namesrvAddr = "127.0.0.1:{namesrv_port}"
storePathRootDir = "{store_root_config}"

[broker.brokerServerConfig]
bindAddress = "127.0.0.1"

[broker.brokerIdentity]
brokerName = "broker-process-stack-test"
brokerClusterName = "DefaultCluster"
brokerId = 0

[store]
brokerRole = "ASYNC_MASTER"
flushDiskType = "ASYNC_FLUSH"
haListenAddress = "127.0.0.1"
haListenPort = {ha_port}
storePathRootDir = "{store_root_config}"
"#
    );
    std::fs::write(&config_path, config).expect("write broker process config");

    let child = Command::new(env!("CARGO_BIN_EXE_rocketmq-broker-rust"))
        .arg("-c")
        .arg(&config_path)
        .env("ROCKETMQ_HOME", root.path())
        .env("ROCKETMQ_HEALTH_BIND_ADDR", health_addr.to_string())
        .env("ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS", "5")
        .env("RUST_LOG", "warn")
        .env_remove("NAMESRV_ADDR")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start broker process");
    let mut broker = BrokerProcess::new(child);

    let startup_deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if broker
            .child_mut()
            .try_wait()
            .expect("inspect broker startup status")
            .is_some()
        {
            let output = broker.wait_with_output();
            panic!("Broker exited before readiness:\n{}", process_output(&output));
        }
        if probe(health_addr, "/readyz").is_ok_and(|response| response.starts_with("HTTP/1.1 200")) {
            break;
        }
        if Instant::now() >= startup_deadline {
            let output = broker.terminate_with_output();
            panic!("Broker did not become ready:\n{}", process_output(&output));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    let drain_response = probe(health_addr, "/drainz").expect("request broker drain");
    assert!(drain_response.starts_with("HTTP/1.1 200"), "{drain_response}");

    let shutdown_deadline = Instant::now() + SHUTDOWN_TIMEOUT;
    loop {
        if broker
            .child_mut()
            .try_wait()
            .expect("inspect broker shutdown status")
            .is_some()
        {
            let output = broker.wait_with_output();
            assert!(
                output.status.success(),
                "Broker shutdown failed:\n{}",
                process_output(&output)
            );
            break;
        }
        if Instant::now() >= shutdown_deadline {
            let output = broker.terminate_with_output();
            panic!("Broker did not stop after drain:\n{}", process_output(&output));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    namesrv_shutdown.send(()).expect("request NameServer shutdown");
    tokio::time::timeout(Duration::from_secs(10), namesrv_handle)
        .await
        .expect("NameServer shutdown should be bounded")
        .expect("NameServer task should not panic");
}
