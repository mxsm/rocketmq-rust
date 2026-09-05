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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::RwLock;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
use rocketmq_error::TRANSPORT_CONNECTION_TIMEOUT;
use rocketmq_error::TRANSPORT_DNS_FAILED;
use rocketmq_model::common::mix_all;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
#[cfg(test)]
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DefaultTopAddressing;
use rocketmq_transport::api::OneShotTransportClient;
use rocketmq_transport::api::RequestDeadline;
use rocketmq_transport::api::TransportTelemetry;

use super::lookup_cache::ClusterTestLookupCache;
use super::lookup_cache::LookupCacheConfig;
use super::lookup_cache::LookupCacheKey;
use super::lookup_cache::ResolvedRoute;
use crate::NamesrvConfig;

const ROUTE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
const ROUTE_LOOKUP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) type ClusterTestLookupFuture<'a, T> = Pin<Box<dyn Future<Output = RocketMQResult<T>> + Send + 'a>>;
type EndpointResolveFuture<'a> = ClusterTestLookupFuture<'a, EndpointResolutionOutcome>;

#[derive(Debug)]
enum EndpointResolutionOutcome {
    Resolved(Vec<SocketAddr>),
    Unavailable,
}

#[derive(Debug)]
pub(super) enum RouteLookupOutcome<T> {
    Resolved(T),
    Unavailable,
    Cancelled,
}

pub(crate) trait ClusterTestRouteLookup: Send + Sync {
    fn start(&self) -> ClusterTestLookupFuture<'_, ()>;

    fn lookup_topic_route(&self, topic: &CheetahString) -> ClusterTestLookupFuture<'_, Option<TopicRouteData>>;

    fn shutdown(&self) -> ClusterTestLookupFuture<'_, ()>;
}

trait ClusterTestEndpointResolver: Send + Sync {
    fn resolve(&self, deadline: RequestDeadline) -> EndpointResolveFuture<'_>;
}

struct ProductEnvironmentEndpointResolver {
    addressing: DefaultTopAddressing,
    address_server: CheetahString,
}

impl ProductEnvironmentEndpointResolver {
    fn new(product_env_name: &str) -> Self {
        let address_server = CheetahString::from_string(mix_all::get_ws_addr());
        Self {
            addressing: DefaultTopAddressing::new(address_server.clone(), Some(CheetahString::from(product_env_name))),
            address_server,
        }
    }
}

impl ClusterTestEndpointResolver for ProductEnvironmentEndpointResolver {
    fn resolve(&self, deadline: RequestDeadline) -> EndpointResolveFuture<'_> {
        Box::pin(async move {
            if deadline.is_expired() {
                return Err(route_lookup_timeout(deadline, self.address_server.as_str()));
            }

            let timeout_millis = deadline.remaining().as_millis().min(u128::from(u64::MAX)).max(1) as u64;
            let address_list = tokio::time::timeout_at(
                deadline.instant(),
                self.addressing.fetch_ns_addr_inner_async(true, timeout_millis),
            )
            .await
            .map_err(|source| route_lookup_timeout_caused_by(deadline, self.address_server.as_str(), source))?;

            let Some(address_list) = address_list else {
                return Ok(EndpointResolutionOutcome::Unavailable);
            };

            resolve_socket_addresses(&address_list, deadline).await
        })
    }
}

pub(crate) struct TransportClusterTestRouteLookup {
    resolver: Arc<dyn ClusterTestEndpointResolver>,
    transport: OneShotTransportClient,
    task_group: TaskGroup,
    cached_endpoints: RwLock<CachedEndpoints>,
    endpoint_resolution: tokio::sync::Mutex<()>,
    lookup_cache: ClusterTestLookupCache,
    request_timeout: Duration,
    command_factory: RemotingCommandFactory,
}

#[derive(Default)]
struct CachedEndpoints {
    generation: u64,
    endpoints: Vec<SocketAddr>,
}

impl TransportClusterTestRouteLookup {
    pub(crate) fn new(
        product_env_name: &str,
        service_context: ChildServiceContext,
        telemetry: TransportTelemetry,
        namesrv_config: &NamesrvConfig,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        Self::with_resolver_and_cache(
            service_context,
            Arc::new(ProductEnvironmentEndpointResolver::new(product_env_name)),
            ROUTE_LOOKUP_TIMEOUT,
            telemetry,
            LookupCacheConfig::from_namesrv_config(namesrv_config),
            command_factory,
        )
    }

    #[cfg(test)]
    fn with_resolver(
        service_context: ChildServiceContext,
        resolver: Arc<dyn ClusterTestEndpointResolver>,
        request_timeout: Duration,
        telemetry: TransportTelemetry,
    ) -> Self {
        Self::with_resolver_and_cache(
            service_context,
            resolver,
            request_timeout,
            telemetry,
            LookupCacheConfig::default(),
            application_remoting_command_factory(),
        )
    }

    fn with_resolver_and_cache(
        service_context: ChildServiceContext,
        resolver: Arc<dyn ClusterTestEndpointResolver>,
        request_timeout: Duration,
        telemetry: TransportTelemetry,
        cache_config: LookupCacheConfig,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        let task_group = service_context.task_group().clone();
        let transport = OneShotTransportClient::new(
            service_context.component("transport"),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .with_telemetry(telemetry);
        Self {
            resolver,
            transport,
            task_group,
            cached_endpoints: RwLock::new(CachedEndpoints::default()),
            endpoint_resolution: tokio::sync::Mutex::new(()),
            lookup_cache: ClusterTestLookupCache::new(cache_config),
            request_timeout,
            command_factory,
        }
    }

    async fn lookup_topic_route_until(
        &self,
        topic: &CheetahString,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RouteLookupOutcome<Option<TopicRouteData>>> {
        let (endpoints, endpoint_generation) = match self.resolve_endpoints(deadline).await? {
            RouteLookupOutcome::Resolved(endpoints) => endpoints,
            RouteLookupOutcome::Unavailable => return Ok(RouteLookupOutcome::Unavailable),
            RouteLookupOutcome::Cancelled => return Ok(RouteLookupOutcome::Cancelled),
        };
        let cache_key = LookupCacheKey::new(endpoint_generation, topic.clone());
        self.lookup_cache
            .get_or_resolve(cache_key, || async move {
                self.lookup_endpoints_until(topic, endpoints, endpoint_generation, deadline)
                    .await
            })
            .await
    }

    async fn lookup_endpoints_until(
        &self,
        topic: &CheetahString,
        endpoints: Vec<SocketAddr>,
        endpoint_generation: u64,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RouteLookupOutcome<ResolvedRoute>> {
        let mut last_error = None;

        for endpoint in endpoints {
            if deadline.is_expired() {
                return Err(route_lookup_timeout(deadline, &endpoint.to_string()));
            }

            match self
                .transport
                .invoke(endpoint, route_request(&self.command_factory, topic), deadline)
                .await
            {
                Ok(response) => return decode_route_response(response).map(RouteLookupOutcome::Resolved),
                Err(error) => last_error = Some(error),
            }
        }

        let mut cached = self.cached_endpoints.write();
        if cached.generation == endpoint_generation {
            cached.endpoints.clear();
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(RouteLookupOutcome::Unavailable),
        }
    }

    async fn resolve_endpoints(
        &self,
        deadline: RequestDeadline,
    ) -> RocketMQResult<RouteLookupOutcome<(Vec<SocketAddr>, u64)>> {
        {
            let cached = self.cached_endpoints.read();
            if !cached.endpoints.is_empty() {
                return Ok(RouteLookupOutcome::Resolved((
                    cached.endpoints.clone(),
                    cached.generation,
                )));
            }
        }

        let _resolution = self.endpoint_resolution.lock().await;
        {
            let cached = self.cached_endpoints.read();
            if !cached.endpoints.is_empty() {
                return Ok(RouteLookupOutcome::Resolved((
                    cached.endpoints.clone(),
                    cached.generation,
                )));
            }
        }
        let EndpointResolutionOutcome::Resolved(resolved) = self.resolver.resolve(deadline).await? else {
            return Ok(RouteLookupOutcome::Unavailable);
        };
        if resolved.is_empty() {
            return Ok(RouteLookupOutcome::Unavailable);
        }
        let mut cached = self.cached_endpoints.write();
        cached.generation = cached.generation.wrapping_add(1).max(1);
        cached.endpoints = resolved.clone();
        Ok(RouteLookupOutcome::Resolved((resolved, cached.generation)))
    }
}

impl ClusterTestRouteLookup for TransportClusterTestRouteLookup {
    fn start(&self) -> ClusterTestLookupFuture<'_, ()> {
        Box::pin(async move {
            if self.task_group.cancellation_token().is_cancelled() {
                return Err(route_lookup_cancelled_error());
            }
            Ok(())
        })
    }

    fn lookup_topic_route(&self, topic: &CheetahString) -> ClusterTestLookupFuture<'_, Option<TopicRouteData>> {
        let topic = topic.clone();
        Box::pin(async move {
            let deadline = RequestDeadline::after(self.request_timeout);
            let cancellation = self.task_group.cancellation_token();
            let outcome = tokio::select! {
                biased;
                _ = cancellation.cancelled() => Ok(RouteLookupOutcome::Cancelled),
                result = self.lookup_topic_route_until(&topic, deadline) => result,
            }?;
            match outcome {
                RouteLookupOutcome::Resolved(route) => Ok(route),
                RouteLookupOutcome::Unavailable => Ok(None),
                RouteLookupOutcome::Cancelled => Err(route_lookup_cancelled_error()),
            }
        })
    }

    fn shutdown(&self) -> ClusterTestLookupFuture<'_, ()> {
        Box::pin(async move {
            let report = self
                .task_group
                .shutdown_until(ShutdownDeadline::after(ROUTE_LOOKUP_SHUTDOWN_TIMEOUT))
                .await;
            report.assert_no_task_leak().map_err(route_lookup_shutdown_error)
        })
    }
}

fn route_request(command_factory: &RemotingCommandFactory, topic: &CheetahString) -> RemotingCommand {
    let mut request = command_factory.create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new(topic.clone(), None),
    );
    request.make_custom_header_to_net();
    request
}

fn decode_route_response(response: RemotingCommand) -> RocketMQResult<ResolvedRoute> {
    let code = response.code();
    match ResponseCode::from(code) {
        ResponseCode::Success => {
            let body = response.body().ok_or_else(|| {
                RpcClientError::remote_error(code, "successful route response did not include a body")
            })?;
            let response_bytes = body.len();
            TopicRouteData::decode(body.as_ref()).map(|route| ResolvedRoute {
                route: Some(route),
                response_bytes,
            })
        }
        ResponseCode::TopicNotExist => Ok(ResolvedRoute {
            route: None,
            response_bytes: response.body().map_or(0, bytes::Bytes::len),
        }),
        _ => Err(RpcClientError::remote_error(
            code,
            response.remark().map_or("route lookup failed", CheetahString::as_str),
        )
        .into()),
    }
}

async fn resolve_socket_addresses(
    address_list: &str,
    deadline: RequestDeadline,
) -> RocketMQResult<EndpointResolutionOutcome> {
    let mut resolved = Vec::new();
    let mut last_error = None;

    for endpoint in address_list.split(';').map(str::trim).filter(|item| !item.is_empty()) {
        if let Ok(address) = endpoint.parse::<SocketAddr>() {
            if !resolved.contains(&address) {
                resolved.push(address);
            }
            continue;
        }

        match tokio::time::timeout_at(deadline.instant(), tokio::net::lookup_host(endpoint)).await {
            Ok(Ok(addresses)) => {
                for address in addresses {
                    if !resolved.contains(&address) {
                        resolved.push(address);
                    }
                }
            }
            Ok(Err(error)) => last_error = Some(error),
            Err(source) => return Err(route_lookup_timeout_caused_by(deadline, endpoint, source)),
        }
    }

    if resolved.is_empty() {
        return match last_error {
            Some(source) => Err(route_lookup_dns_failure_from_source(source)),
            None => Ok(EndpointResolutionOutcome::Unavailable),
        };
    }
    Ok(EndpointResolutionOutcome::Resolved(resolved))
}

#[track_caller]
fn route_lookup_timeout(deadline: RequestDeadline, remote_addr: &str) -> RocketMQError {
    let context = ErrorContext::new()
        .with_u64(fields::TIMEOUT_MS, deadline.budget_millis())
        .with_text(fields::REMOTE_ADDR, remote_addr);
    RocketMQError::Network(Arc::new(
        Error::new(&TRANSPORT_CONNECTION_TIMEOUT).with_context(context),
    ))
}

#[track_caller]
fn route_lookup_timeout_caused_by(
    deadline: RequestDeadline,
    remote_addr: &str,
    source: tokio::time::error::Elapsed,
) -> RocketMQError {
    let context = ErrorContext::new()
        .with_u64(fields::TIMEOUT_MS, deadline.budget_millis())
        .with_text(fields::REMOTE_ADDR, remote_addr)
        .with_secret_presence(fields::SOURCE_PRESENT);
    RocketMQError::Network(Arc::new(
        Error::caused_by(&TRANSPORT_CONNECTION_TIMEOUT, source).with_context(context),
    ))
}

#[track_caller]
fn route_lookup_cancelled_error() -> RocketMQError {
    let context = ErrorContext::new().with_text(fields::PHASE, "closed");
    RocketMQError::Network(Arc::new(
        Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED).with_context(context),
    ))
}

#[track_caller]
fn route_lookup_dns_failure_from_source(source: std::io::Error) -> RocketMQError {
    let context = ErrorContext::new()
        .with_secret_presence(fields::HOST_PRESENT)
        .with_secret_presence(fields::SOURCE_PRESENT);
    RocketMQError::Network(Arc::new(
        Error::caused_by(&TRANSPORT_DNS_FAILED, source).with_context(context),
    ))
}

#[derive(Debug)]
struct RouteLookupShutdownFailure(String);

impl std::fmt::Display for RouteLookupShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RouteLookupShutdownFailure {}

#[track_caller]
fn route_lookup_shutdown_error(detail: String) -> RocketMQError {
    let context = ErrorContext::new()
        .with_text(fields::PHASE, "closed")
        .with_secret_presence(fields::SOURCE_PRESENT);
    RocketMQError::Network(Arc::new(
        Error::caused_by(
            &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
            RouteLookupShutdownFailure(detail),
        )
        .with_context(context),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_runtime::ChildServiceContext;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_runtime::ShutdownReport;
    use rocketmq_transport::api::HandlerOutcome;
    use rocketmq_transport::api::RemotingRequest;

    use rocketmq_transport::api::RemotingResponse;
    use rocketmq_transport::api::RequestProcessor;
    use rocketmq_transport::api::ServerConfig;
    use rocketmq_transport::api::TransportError;
    use rocketmq_transport::api::TransportServer;
    use tokio::sync::oneshot;
    use tokio::sync::Notify;

    use super::*;

    #[test]
    fn route_lookup_failures_use_canonical_descriptors_and_preserve_remoting() {
        let timeout = route_lookup_timeout(RequestDeadline::from_timeout_millis(25), "address-server:80");
        assert_eq!(timeout.descriptor().code().as_str(), "transport.connection.timeout");
        assert_eq!(timeout.boundary_view().remoting().code.as_i32(), 2);

        let dns = route_lookup_dns_failure_from_source(std::io::Error::other("resolver unavailable"));
        assert_eq!(dns.descriptor().code().as_str(), "transport.dns.failed");
        assert_eq!(dns.boundary_view().remoting().code.as_i32(), 2);
        let RocketMQError::Network(canonical) = &dns else {
            panic!("DNS failure must use the canonical Network carrier");
        };
        let io_source = std::error::Error::source(canonical.as_ref())
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .expect("DNS failure must retain the physical resolver error");
        assert_eq!(io_source.kind(), std::io::ErrorKind::Other);

        let cancelled = route_lookup_cancelled_error();
        assert_eq!(cancelled.descriptor().code().as_str(), "transport.connection.failed");
        assert_eq!(cancelled.boundary_view().remoting().code.as_i32(), 2);
    }

    #[tokio::test]
    async fn route_lookup_timeout_retains_elapsed_source_when_available() {
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .expect_err("pending future must time out");
        let error =
            route_lookup_timeout_caused_by(RequestDeadline::from_timeout_millis(25), "address-server:80", elapsed);
        let RocketMQError::Network(canonical) = error else {
            panic!("timeout must use the canonical Network carrier");
        };
        assert!(std::error::Error::source(canonical.as_ref())
            .and_then(|source| source.downcast_ref::<tokio::time::error::Elapsed>())
            .is_some());
    }

    #[tokio::test]
    async fn absent_endpoint_discovery_is_a_normal_unavailable_outcome() {
        let outcome = resolve_socket_addresses(" ; ", RequestDeadline::from_timeout_millis(25))
            .await
            .expect("an absent endpoint list is not a DNS failure");

        assert!(matches!(outcome, EndpointResolutionOutcome::Unavailable));
    }

    #[test]
    fn route_request_keeps_lookup_factory_defaults() {
        let factory = rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory::new(
            rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandDefaults::new(
                658,
                rocketmq_protocol::protocol::SerializeType::ROCKETMQ,
            ),
        );

        let request = route_request(&factory, &CheetahString::from("factory-topic"));

        assert_eq!(request.version(), 658);
        assert_eq!(
            request.serialize_type(),
            rocketmq_protocol::protocol::SerializeType::ROCKETMQ
        );
    }

    struct FixedEndpointResolver {
        endpoints: Vec<SocketAddr>,
        calls: AtomicUsize,
    }

    impl FixedEndpointResolver {
        fn new(endpoints: Vec<SocketAddr>) -> Self {
            Self {
                endpoints,
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl ClusterTestEndpointResolver for FixedEndpointResolver {
        fn resolve(&self, _deadline: RequestDeadline) -> EndpointResolveFuture<'_> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let endpoints = self.endpoints.clone();
            Box::pin(async move { Ok(EndpointResolutionOutcome::Resolved(endpoints)) })
        }
    }

    #[derive(Clone)]
    struct RouteProcessor {
        route: TopicRouteData,
    }

    impl RequestProcessor for RouteProcessor {
        async fn process(&mut self, request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
            assert_eq!(request.command().code(), RequestCode::GetRouteinfoByTopic as i32);
            let header = request
                .command()
                .decode_command_custom_header::<GetRouteInfoRequestHeader>()?;
            assert_eq!(header.topic, CheetahString::from("missing-topic"));
            let response = RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                .set_body(self.route.encode()?);
            response_outcome(response)
        }
    }

    #[derive(Clone)]
    struct BlockingRouteProcessor {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl RequestProcessor for BlockingRouteProcessor {
        async fn process(&mut self, _request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
            self.entered.notify_one();
            self.release.notified().await;
            response_outcome(RemotingCommand::create_response_command_with_code(
                ResponseCode::Success,
            ))
        }
    }

    fn response_outcome(response: RemotingCommand) -> RocketMQResult<HandlerOutcome> {
        let response = RemotingResponse::from_command(response).map_err(|error| {
            RocketMQError::response_process_failed("namesrv.route_lookup_test.remoting_response", error.to_string())
        })?;
        Ok(HandlerOutcome::Reply(response))
    }

    struct RunningRouteServer {
        local_addr: SocketAddr,
        shutdown: Option<oneshot::Sender<()>>,
        result: oneshot::Receiver<Result<ShutdownReport, TransportError>>,
    }

    impl RunningRouteServer {
        async fn bind<P>(service: ChildServiceContext, processor: P) -> Self
        where
            P: RequestProcessor + Clone + Sync + 'static,
        {
            let config = Arc::new(ServerConfig {
                bind_address: "127.0.0.1".to_owned(),
                listen_port: 0,
                ..ServerConfig::default()
            });
            let server = TransportServer::new(config, service.component("server"), processor);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let (startup_tx, startup_rx) = oneshot::channel();
            let (result_tx, result_rx) = oneshot::channel();
            service
                .component("runner")
                .spawn_service("namesrv.route-lookup-test-server", async move {
                    let result = server
                        .try_run_with_shutdown_report_and_startup(
                            async move {
                                let _ = shutdown_rx.await;
                            },
                            startup_tx,
                        )
                        .await;
                    let _ = result_tx.send(result);
                })
                .expect("route lookup test server should be lifecycle-owned");
            let local_addr = startup_rx
                .await
                .expect("route lookup startup channel")
                .expect("route lookup server should start");
            Self {
                local_addr,
                shutdown: Some(shutdown_tx),
                result: result_rx,
            }
        }

        fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        async fn shutdown(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            let report = self
                .result
                .await
                .expect("route lookup server report channel")
                .expect("route lookup server should stop cleanly");
            report
                .assert_no_task_leak()
                .expect("route lookup server should not leak tasks");
        }
    }

    struct BlockingResolver {
        entered: Arc<Notify>,
    }

    impl ClusterTestEndpointResolver for BlockingResolver {
        fn resolve(&self, _deadline: RequestDeadline) -> EndpointResolveFuture<'_> {
            Box::pin(async move {
                self.entered.notify_one();
                std::future::pending().await
            })
        }
    }

    fn sample_route() -> TopicRouteData {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(0, CheetahString::from("127.0.0.1:10911"));
        TopicRouteData {
            order_topic_conf: None,
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 4, 4, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                None,
            )],
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        }
    }

    #[tokio::test]
    async fn transport_lookup_decodes_route_and_caches_resolved_endpoints() {
        let runtime = RuntimeContext::from_current("namesrv-route-lookup-success-test");
        let server = RunningRouteServer::bind(
            runtime.service_context("route-server"),
            RouteProcessor { route: sample_route() },
        )
        .await;
        let address = server.local_addr();

        let resolver = Arc::new(FixedEndpointResolver::new(vec![address]));
        let lookup = TransportClusterTestRouteLookup::with_resolver(
            runtime.service_context("route-lookup"),
            resolver.clone(),
            Duration::from_secs(1),
            TransportTelemetry::noop(),
        );
        lookup.start().await.unwrap();

        let first = lookup
            .lookup_topic_route(&CheetahString::from("missing-topic"))
            .await
            .unwrap();
        let second = lookup
            .lookup_topic_route(&CheetahString::from("missing-topic"))
            .await
            .unwrap();
        assert_eq!(first, Some(sample_route()));
        assert_eq!(second, first);
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);

        lookup.shutdown().await.unwrap();
        server.shutdown().await;
        runtime
            .shutdown_tasks(Duration::from_secs(1))
            .await
            .assert_no_task_leak()
            .unwrap();
    }

    #[tokio::test]
    async fn transport_lookup_enforces_request_deadline() {
        let runtime = RuntimeContext::from_current("namesrv-route-lookup-timeout-test");
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let server = RunningRouteServer::bind(
            runtime.service_context("hung-route-server"),
            BlockingRouteProcessor {
                entered: entered.clone(),
                release: release.clone(),
            },
        )
        .await;
        let address = server.local_addr();

        let lookup = Arc::new(TransportClusterTestRouteLookup::with_resolver(
            runtime.service_context("route-lookup"),
            Arc::new(FixedEndpointResolver::new(vec![address])),
            Duration::from_millis(50),
            TransportTelemetry::noop(),
        ));
        let active_lookup = {
            let lookup = lookup.clone();
            tokio::spawn(async move { lookup.lookup_topic_route(&CheetahString::from("missing-topic")).await })
        };
        entered.notified().await;
        let result = active_lookup.await.unwrap();
        assert!(result.is_err(), "a hung route request must honor its deadline");
        release.notify_waiters();

        lookup.shutdown().await.unwrap();
        server.shutdown().await;
        runtime
            .shutdown_tasks(Duration::from_secs(1))
            .await
            .assert_no_task_leak()
            .unwrap();
    }

    #[tokio::test]
    async fn transport_lookup_reports_unreachable_endpoint() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let unreachable = listener.local_addr().unwrap();
        drop(listener);

        let runtime = RuntimeContext::from_current("namesrv-route-lookup-unreachable-test");
        let lookup = TransportClusterTestRouteLookup::with_resolver(
            runtime.service_context("route-lookup"),
            Arc::new(FixedEndpointResolver::new(vec![unreachable])),
            Duration::from_millis(100),
            TransportTelemetry::noop(),
        );
        let result = lookup.lookup_topic_route(&CheetahString::from("missing-topic")).await;
        assert!(result.is_err(), "an unreachable endpoint must return a typed error");

        lookup.shutdown().await.unwrap();
        runtime
            .shutdown_tasks(Duration::from_secs(1))
            .await
            .assert_no_task_leak()
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_an_active_resolution() {
        let runtime = RuntimeContext::from_current("namesrv-route-lookup-shutdown-test");
        let entered = Arc::new(Notify::new());
        let lookup = Arc::new(TransportClusterTestRouteLookup::with_resolver(
            runtime.service_context("route-lookup"),
            Arc::new(BlockingResolver {
                entered: entered.clone(),
            }),
            Duration::from_secs(1),
            TransportTelemetry::noop(),
        ));
        let active_lookup = {
            let lookup = lookup.clone();
            tokio::spawn(async move { lookup.lookup_topic_route(&CheetahString::from("missing-topic")).await })
        };
        entered.notified().await;

        lookup.shutdown().await.unwrap();
        assert!(active_lookup.await.unwrap().is_err());
        assert!(lookup.start().await.is_err());
        runtime
            .shutdown_tasks(Duration::from_secs(1))
            .await
            .assert_no_task_leak()
            .unwrap();
    }
}
