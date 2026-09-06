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

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RouteWorkloadManifest {
    pub(crate) schema_version: u32,
    pub(crate) seed: u64,
    pub(crate) profiles: Vec<RouteWorkloadProfile>,
}

impl RouteWorkloadManifest {
    pub(crate) fn profile(&self, name: &str) -> Option<&RouteWorkloadProfile> {
        self.profiles.iter().find(|profile| profile.name == name)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RouteWorkloadProfile {
    pub(crate) name: String,
    pub(crate) topics: usize,
    pub(crate) brokers: usize,
    pub(crate) route_width: usize,
    pub(crate) connections: usize,
    pub(crate) operations: usize,
    pub(crate) write_percent: u8,
    pub(crate) zone_percent: u8,
    pub(crate) standard_json_percent: u8,
}

impl RouteWorkloadProfile {
    pub(crate) fn trace(&self, seed: u64) -> Vec<WorkloadTraceEntry> {
        let mut state = seed.max(1);
        (0..self.operations)
            .map(|index| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let topic_index = state as usize % self.topics;
                state = state.rotate_left(19) ^ 0x9e37_79b9_7f4a_7c15;
                let broker_index = state as usize % self.brokers;
                WorkloadTraceEntry {
                    operation: if percentage_slot(index, seed, self.write_percent) {
                        WorkloadOperation::RegistrationWrite
                    } else {
                        WorkloadOperation::RouteRead
                    },
                    topic_index,
                    broker_index,
                    zone: percentage_slot(index, seed.rotate_left(11), self.zone_percent),
                    standard_json: percentage_slot(index, seed.rotate_left(29), self.standard_json_percent),
                }
            })
            .collect()
    }
}

const fn percentage_slot(index: usize, seed: u64, percent: u8) -> bool {
    ((index + seed as usize) % 100) < percent as usize
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkloadTraceEntry {
    pub(crate) operation: WorkloadOperation,
    pub(crate) topic_index: usize,
    pub(crate) broker_index: usize,
    pub(crate) zone: bool,
    pub(crate) standard_json: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkloadOperation {
    RouteRead,
    RegistrationWrite,
}

pub(crate) fn load_workload_manifest(path: &Path) -> Result<RouteWorkloadManifest> {
    let bytes = fs::read(path).with_context(|| format!("read route workload manifest {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse route workload manifest {}", path.display()))
}

pub(crate) fn validate_workload_manifest(manifest: &RouteWorkloadManifest) -> Result<()> {
    if manifest.schema_version != 1 {
        bail!("unsupported route workload schema version {}", manifest.schema_version);
    }
    if manifest.seed == 0 {
        bail!("route workload seed must be non-zero");
    }
    if manifest.profiles.is_empty() {
        bail!("route workload manifest must contain at least one profile");
    }

    let mut names = BTreeSet::new();
    for profile in &manifest.profiles {
        if !names.insert(profile.name.as_str()) {
            bail!("duplicate route workload profile '{}'", profile.name);
        }
        if profile.topics == 0
            || profile.brokers == 0
            || profile.connections == 0
            || profile.operations == 0
            || profile.route_width == 0
            || profile.route_width > profile.brokers
        {
            bail!(
                "profile '{}' contains a zero or invalid topology dimension",
                profile.name
            );
        }
        for (label, value) in [
            ("writePercent", profile.write_percent),
            ("zonePercent", profile.zone_percent),
            ("standardJsonPercent", profile.standard_json_percent),
        ] {
            if value > 100 {
                bail!("profile '{}' has {label}={value}, expected 0..=100", profile.name);
            }
        }
        if !profile.operations.is_multiple_of(100) {
            bail!(
                "profile '{}' operations must be divisible by 100 for exact mixes",
                profile.name
            );
        }
    }

    let production_profiles = manifest.profiles.iter().filter(|profile| profile.brokers == 1_000);
    let topic_counts = production_profiles
        .clone()
        .map(|profile| profile.topics)
        .collect::<BTreeSet<_>>();
    let route_widths = production_profiles
        .clone()
        .map(|profile| profile.route_width)
        .collect::<BTreeSet<_>>();
    let connections = production_profiles
        .clone()
        .map(|profile| profile.connections)
        .collect::<BTreeSet<_>>();
    let zone_percents = production_profiles
        .clone()
        .map(|profile| profile.zone_percent)
        .collect::<BTreeSet<_>>();
    if ![20_000, 100_000].into_iter().all(|value| topic_counts.contains(&value))
        || ![1, 4, 16].into_iter().all(|value| route_widths.contains(&value))
        || ![64, 256, 1_024].into_iter().all(|value| connections.contains(&value))
        || ![0, 10].into_iter().all(|value| zone_percents.contains(&value))
        || !production_profiles.clone().all(|profile| profile.write_percent == 5)
        || !production_profiles
            .clone()
            .all(|profile| profile.standard_json_percent > 0 && profile.standard_json_percent < 100)
    {
        bail!("production route workload profiles do not cover the required topology and request dimensions");
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Criterion uses the network half while manifest integration tests reuse only the workload contract"
)]
pub(crate) mod network {
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicI32;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use anyhow::bail;
    use anyhow::Context;
    use anyhow::Result;
    use cheetah_string::CheetahString;
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::mix_all;
    use rocketmq_model::utils::crc32_utils;
    use rocketmq_model::version::RocketMqVersion;
    use rocketmq_namesrv::bootstrap::Builder;
    use rocketmq_namesrv::NamesrvConfig;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
    use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
    use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::config_header::GetNamesrvConfigRequestHeader;
    use rocketmq_protocol::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_transport::api::DefaultRequestProcessor;
    use rocketmq_transport::api::OutboundRequestOutcome;
    use rocketmq_transport::api::ServerConfig;
    use rocketmq_transport::api::TransportClient;
    use rocketmq_transport::api::TransportClientConfig;
    use serde::Serialize;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    use super::RouteWorkloadProfile;
    use super::WorkloadOperation;
    use super::WorkloadTraceEntry;

    const REQUEST_TIMEOUT_MILLIS: u64 = 10_000;

    pub(crate) struct OwnedNameServer {
        pub(crate) endpoint: CheetahString,
        shutdown: Option<oneshot::Sender<()>>,
        task: JoinHandle<rocketmq_error::RocketMQResult<()>>,
        runtime_context: RuntimeContext,
    }

    impl OwnedNameServer {
        pub(crate) async fn start() -> Result<Self> {
            let port = reserve_local_port()?;
            let endpoint = CheetahString::from_string(format!("127.0.0.1:{port}"));
            let artifact_dir = benchmark_runtime_dir(port);
            std::fs::create_dir_all(&artifact_dir)
                .with_context(|| format!("create benchmark runtime directory {}", artifact_dir.display()))?;
            let namesrv_config = NamesrvConfig {
                kv_config_path: artifact_dir.join("kvConfig.json").to_string_lossy().into_owned(),
                config_store_path: artifact_dir
                    .join("rocketmq-namesrv.properties")
                    .to_string_lossy()
                    .into_owned(),
                ..NamesrvConfig::default()
            };
            let runtime_context = RuntimeContext::from_current(format!("namesrv-route-e2e-server-{port}"));
            let bootstrap = Builder::new(
                runtime_context.service_context("namesrv"),
                rocketmq_observability::TelemetryHandle::noop(),
            )
            .set_name_server_config(namesrv_config)
            .set_server_config(ServerConfig {
                listen_port: port as u32,
                bind_address: "127.0.0.1".to_string(),
                ..ServerConfig::default()
            })
            .build();
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                bootstrap
                    .boot_with_shutdown(async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });
            Ok(Self {
                endpoint,
                shutdown: Some(shutdown_tx),
                task,
                runtime_context,
            })
        }

        pub(crate) async fn shutdown(mut self) -> Result<()> {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            let result = tokio::time::timeout(Duration::from_secs(20), &mut self.task)
                .await
                .context("wait for benchmark NameServer shutdown")?
                .context("benchmark NameServer task panicked")?;
            result.context("benchmark NameServer shutdown failed")?;
            self.runtime_context
                .shutdown_tasks(Duration::from_secs(5))
                .await
                .assert_no_task_leak()
                .map_err(anyhow::Error::msg)
                .context("benchmark NameServer runtime leaked tasks")?;
            Ok(())
        }
    }

    impl Drop for OwnedNameServer {
        fn drop(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
        }
    }

    pub(crate) struct RouteBenchHarness {
        endpoint: CheetahString,
        clients: Vec<Arc<TransportClient<DefaultRequestProcessor>>>,
        runtime_context: RuntimeContext,
        topics: Vec<CheetahString>,
        registration_requests: Vec<RemotingCommand>,
        next_opaque: AtomicI32,
    }

    #[derive(Clone, Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub(crate) struct RouteTraceMetrics {
        pub(crate) operations: usize,
        pub(crate) route_reads: usize,
        pub(crate) registration_writes: usize,
        pub(crate) zone_reads: usize,
        pub(crate) standard_json_reads: usize,
        pub(crate) errors: usize,
        pub(crate) duration_millis: u128,
        pub(crate) qps: f64,
        pub(crate) p50_micros: u64,
        pub(crate) p95_micros: u64,
        pub(crate) p99_micros: u64,
        pub(crate) p999_micros: u64,
        pub(crate) response_bytes: u64,
        pub(crate) allocation_bytes_per_operation: Option<f64>,
    }

    #[derive(Clone, Copy, Debug)]
    struct RequestSample {
        elapsed_micros: u64,
        response_bytes: usize,
        success: bool,
    }

    impl RouteBenchHarness {
        pub(crate) async fn connect(endpoint: CheetahString, connections: usize) -> Result<Self> {
            let runtime_context = RuntimeContext::from_current("namesrv-route-e2e-client");
            let mut clients = Vec::new();
            clients
                .try_reserve(connections)
                .context("reserve route benchmark transport clients")?;
            for index in 0..connections {
                let client = Arc::new(
                    TransportClient::builder(
                        Arc::new(TransportClientConfig::default()),
                        DefaultRequestProcessor,
                        runtime_context.service_context(
                            rocketmq_runtime::ScopeId::try_new(format!("client-{index}"))
                                .expect("the benchmark client scope has a fixed nonblank prefix"),
                        ),
                    )
                    .build()
                    .context("build route benchmark transport client")?,
                );
                client.update_name_server_address_list(vec![endpoint.clone()]).await;
                client.start().await.context("start route benchmark transport client")?;
                clients.push(client);
            }
            let harness = Self {
                endpoint,
                clients,
                runtime_context,
                topics: Vec::new(),
                registration_requests: Vec::new(),
                next_opaque: AtomicI32::new(1),
            };
            harness.wait_until_ready().await?;
            Ok(harness)
        }

        pub(crate) async fn prepare_topology(&mut self, profile: &RouteWorkloadProfile) -> Result<()> {
            self.topics = (0..profile.topics)
                .map(|index| CheetahString::from_string(format!("RouteBenchTopic-{index:06}")))
                .collect();
            let mut broker_topics = vec![Vec::new(); profile.brokers];
            for topic_index in 0..profile.topics {
                for offset in 0..profile.route_width {
                    broker_topics[(topic_index + offset) % profile.brokers].push(topic_index);
                }
            }

            self.registration_requests.clear();
            self.registration_requests
                .try_reserve(profile.brokers)
                .context("reserve benchmark broker registrations")?;
            for (broker_index, topic_indexes) in broker_topics.into_iter().enumerate() {
                let request = self.registration_request(broker_index, &topic_indexes)?;
                let response = self.invoke(broker_index, request.clone()).await?;
                if ResponseCode::from(response.code()) != ResponseCode::Success {
                    bail!(
                        "benchmark broker {broker_index} registration failed with {:?}: {:?}",
                        ResponseCode::from(response.code()),
                        response.remark()
                    );
                }
                self.registration_requests.push(request);
            }
            Ok(())
        }

        pub(crate) async fn run_trace(&self, profile: &RouteWorkloadProfile, seed: u64) -> Result<RouteTraceMetrics> {
            let trace = profile.trace(seed);
            let route_reads = trace
                .iter()
                .filter(|entry| entry.operation == WorkloadOperation::RouteRead)
                .count();
            let registration_writes = trace.len() - route_reads;
            let zone_reads = trace
                .iter()
                .filter(|entry| entry.operation == WorkloadOperation::RouteRead && entry.zone)
                .count();
            let standard_json_reads = trace
                .iter()
                .filter(|entry| entry.operation == WorkloadOperation::RouteRead && entry.standard_json)
                .count();
            let started = Instant::now();
            let mut pending = FuturesUnordered::new();
            let mut trace = trace.into_iter();
            for entry in trace.by_ref().take(profile.connections) {
                pending.push(self.execute(entry));
            }

            let mut latencies = Vec::with_capacity(profile.operations);
            let mut errors = 0;
            let mut response_bytes = 0_u64;
            while let Some(sample) = pending.next().await {
                let sample = sample?;
                latencies.push(sample.elapsed_micros);
                response_bytes = response_bytes.saturating_add(sample.response_bytes as u64);
                errors += usize::from(!sample.success);
                if let Some(entry) = trace.next() {
                    pending.push(self.execute(entry));
                }
            }
            let elapsed = started.elapsed();
            latencies.sort_unstable();
            Ok(RouteTraceMetrics {
                operations: profile.operations,
                route_reads,
                registration_writes,
                zone_reads,
                standard_json_reads,
                errors,
                duration_millis: elapsed.as_millis(),
                qps: profile.operations as f64 / elapsed.as_secs_f64(),
                p50_micros: percentile(&latencies, 500),
                p95_micros: percentile(&latencies, 950),
                p99_micros: percentile(&latencies, 990),
                p999_micros: percentile(&latencies, 999),
                response_bytes,
                allocation_bytes_per_operation: None,
            })
        }

        pub(crate) async fn request_topic(&self, topic_index: usize) -> Result<usize> {
            let entry = WorkloadTraceEntry {
                operation: WorkloadOperation::RouteRead,
                topic_index: topic_index % self.topics.len(),
                broker_index: 0,
                zone: false,
                standard_json: true,
            };
            let sample = self.execute(entry).await?;
            if !sample.success {
                bail!("criterion route request returned a non-success response");
            }
            Ok(sample.response_bytes)
        }

        pub(crate) async fn shutdown(self) -> Result<()> {
            for client in &self.clients {
                client.shutdown();
            }
            self.runtime_context
                .shutdown_tasks(Duration::from_secs(10))
                .await
                .assert_no_task_leak()
                .map_err(anyhow::Error::msg)
                .context("route benchmark client runtime leaked tasks")?;
            Ok(())
        }

        async fn execute(&self, entry: WorkloadTraceEntry) -> Result<RequestSample> {
            let client_index = (entry.topic_index ^ entry.broker_index) % self.clients.len();
            let request = match entry.operation {
                WorkloadOperation::RouteRead => self.route_request(entry),
                WorkloadOperation::RegistrationWrite => {
                    self.registration_requests[entry.broker_index % self.registration_requests.len()].clone()
                }
            };
            let started = Instant::now();
            let response = self.invoke(client_index, request).await?;
            Ok(RequestSample {
                elapsed_micros: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                response_bytes: response.body().map_or(0, |body| body.len()),
                success: ResponseCode::from(response.code()) == ResponseCode::Success,
            })
        }

        async fn invoke(&self, client_index: usize, request: RemotingCommand) -> Result<RemotingCommand> {
            let request = request.set_opaque(self.next_opaque.fetch_add(1, Ordering::Relaxed));
            let outcome = self.clients[client_index % self.clients.len()]
                .invoke_request(Some(&self.endpoint), request, REQUEST_TIMEOUT_MILLIS)
                .await
                .context("execute route benchmark request")?;
            match outcome {
                OutboundRequestOutcome::Response(response) => Ok(response),
                OutboundRequestOutcome::Rejected(rejection) => {
                    bail!("route benchmark request was rejected: {rejection:?}")
                }
                OutboundRequestOutcome::Contract(contract) => {
                    bail!("route benchmark request contract failed: {contract:?}")
                }
            }
        }

        fn route_request(&self, entry: WorkloadTraceEntry) -> RemotingCommand {
            let topic = self.topics[entry.topic_index % self.topics.len()].clone();
            let version = if entry.standard_json {
                RocketMqVersion::V5_0_0 as i32
            } else {
                RocketMqVersion::V4_9_3 as i32
            };
            let mut request = RemotingCommand::create_request_command(
                RequestCode::GetRouteinfoByTopic,
                GetRouteInfoRequestHeader::new(topic, Some(entry.standard_json)),
            )
            .set_version(version);
            request.make_custom_header_to_net();
            if entry.zone {
                request
                    .add_ext_field(mix_all::ZONE_MODE, "true")
                    .add_ext_field(mix_all::ZONE_NAME, "zone-a");
            }
            request
        }

        fn registration_request(&self, broker_index: usize, topic_indexes: &[usize]) -> Result<RemotingCommand> {
            if topic_indexes.is_empty() {
                bail!("benchmark broker {broker_index} owns no topics");
            }
            let broker_name = CheetahString::from_string(format!("route-bench-broker-{broker_index:04}"));
            let broker_addr = CheetahString::from_string(format!("127.0.0.1:{}", 30_000 + broker_index));
            let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
            wrapper.topic_config_serialize_wrapper.data_version.set_counter(1);
            wrapper.topic_config_serialize_wrapper.data_version.set_timestamp(1);
            wrapper.topic_config_serialize_wrapper.data_version.set_state_version(1);
            wrapper
                .topic_config_serialize_wrapper
                .topic_config_table
                .try_reserve(topic_indexes.len())
                .context("reserve benchmark topic configuration")?;
            for &topic_index in topic_indexes {
                let topic = self.topics[topic_index].clone();
                wrapper
                    .topic_config_serialize_wrapper
                    .topic_config_table
                    .insert(topic.clone(), TopicConfig::with_perm(topic, 4, 4, 6));
            }
            let body = RegisterBrokerBody::new(wrapper, Vec::new()).encode(false);
            let mut request = RemotingCommand::create_request_command(
                RequestCode::RegisterBroker,
                RegisterBrokerRequestHeader::new(
                    broker_name,
                    broker_addr.clone(),
                    CheetahString::from_static_str("RouteBenchCluster"),
                    broker_addr,
                    0,
                    Some(120_000),
                    Some(false),
                    false,
                    crc32_utils::crc32(&body),
                ),
            )
            .set_version(RocketMqVersion::V5_0_0 as i32)
            .set_body(body);
            request.make_custom_header_to_net();
            request.add_ext_field(
                mix_all::ZONE_NAME,
                if broker_index.is_multiple_of(2) {
                    "zone-a"
                } else {
                    "zone-b"
                },
            );
            Ok(request)
        }

        async fn wait_until_ready(&self) -> Result<()> {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            loop {
                let mut request = RemotingCommand::create_request_command(
                    RequestCode::GetNamesrvConfig,
                    GetNamesrvConfigRequestHeader::default(),
                );
                request.make_custom_header_to_net();
                if let Ok(response) = self.invoke(0, request).await {
                    if ResponseCode::from(response.code()) == ResponseCode::Success {
                        return Ok(());
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    bail!("NameServer {} did not become ready before timeout", self.endpoint);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    fn percentile(sorted: &[u64], permille: usize) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let rank = (sorted.len() * permille).div_ceil(1_000).saturating_sub(1);
        sorted[rank.min(sorted.len() - 1)]
    }

    fn reserve_local_port() -> Result<u16> {
        Ok(TcpListener::bind("127.0.0.1:0")
            .context("reserve local NameServer benchmark port")?
            .local_addr()
            .context("read reserved NameServer benchmark port")?
            .port())
    }

    fn benchmark_runtime_dir(port: u16) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("rocketmq-namesrv is below the workspace root")
            .join(format!("target/namesrv-bench/runtime-{}-{port}", std::process::id()))
    }
}
