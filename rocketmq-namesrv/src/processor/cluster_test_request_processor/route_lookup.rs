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
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
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
use rocketmq_transport::api::v1::AdmissionController;
use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::DefaultTopAddressing;
use rocketmq_transport::api::v1::OneShotTransportClient;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::TransportTelemetry;

use super::lookup_cache::ClusterTestLookupCache;
use super::lookup_cache::LookupCacheConfig;
use super::lookup_cache::LookupCacheKey;
use super::lookup_cache::ResolvedRoute;
use crate::NamesrvConfig;

const ROUTE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
const ROUTE_LOOKUP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);
const LOOKUP_OWNER: &str = "namesrv.cluster-test-route-lookup";

pub(crate) type ClusterTestLookupFuture<'a, T> = Pin<Box<dyn Future<Output = RocketMQResult<T>> + Send + 'a>>;
type EndpointResolveFuture<'a> = ClusterTestLookupFuture<'a, Vec<SocketAddr>>;

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
}

impl ProductEnvironmentEndpointResolver {
    fn new(product_env_name: &str) -> Self {
        Self {
            addressing: DefaultTopAddressing::new(
                CheetahString::from_string(mix_all::get_ws_addr()),
                Some(CheetahString::from(product_env_name)),
            ),
        }
    }
}

impl ClusterTestEndpointResolver for ProductEnvironmentEndpointResolver {
    fn resolve(&self, deadline: RequestDeadline) -> EndpointResolveFuture<'_> {
        Box::pin(async move {
            if deadline.is_expired() {
                return Err(route_lookup_timeout(deadline));
            }

            let timeout_millis = deadline.remaining().as_millis().min(u128::from(u64::MAX)).max(1) as u64;
            let address_list = tokio::time::timeout_at(
                deadline.instant(),
                self.addressing.fetch_ns_addr_inner_async(true, timeout_millis),
            )
            .await
            .map_err(|_| route_lookup_timeout(deadline))?
            .ok_or_else(|| {
                RocketMQError::network_connection_failed(
                    LOOKUP_OWNER,
                    "product environment returned no name-server endpoints",
                )
            })?;

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
    ) -> RocketMQResult<Option<TopicRouteData>> {
        let (endpoints, endpoint_generation) = self.resolve_endpoints(deadline).await?;
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
    ) -> RocketMQResult<ResolvedRoute> {
        let mut last_error = None;

        for endpoint in endpoints {
            if deadline.is_expired() {
                return Err(route_lookup_timeout(deadline));
            }

            match self
                .transport
                .invoke(endpoint, route_request(&self.command_factory, topic), deadline)
                .await
            {
                Ok(response) => return decode_route_response(response),
                Err(error) => last_error = Some(error),
            }
        }

        let mut cached = self.cached_endpoints.write();
        if cached.generation == endpoint_generation {
            cached.endpoints.clear();
        }
        Err(last_error.unwrap_or_else(|| {
            RocketMQError::network_connection_failed(LOOKUP_OWNER, "no product-environment endpoint was reachable")
        }))
    }

    async fn resolve_endpoints(&self, deadline: RequestDeadline) -> RocketMQResult<(Vec<SocketAddr>, u64)> {
        {
            let cached = self.cached_endpoints.read();
            if !cached.endpoints.is_empty() {
                return Ok((cached.endpoints.clone(), cached.generation));
            }
        }

        let _resolution = self.endpoint_resolution.lock().await;
        {
            let cached = self.cached_endpoints.read();
            if !cached.endpoints.is_empty() {
                return Ok((cached.endpoints.clone(), cached.generation));
            }
        }
        let resolved = self.resolver.resolve(deadline).await?;
        if resolved.is_empty() {
            return Err(RocketMQError::network_connection_failed(
                LOOKUP_OWNER,
                "product environment resolved to an empty endpoint list",
            ));
        }
        let mut cached = self.cached_endpoints.write();
        cached.generation = cached.generation.wrapping_add(1).max(1);
        cached.endpoints = resolved.clone();
        Ok((resolved, cached.generation))
    }
}

impl ClusterTestRouteLookup for TransportClusterTestRouteLookup {
    fn start(&self) -> ClusterTestLookupFuture<'_, ()> {
        Box::pin(async move {
            if self.task_group.cancellation_token().is_cancelled() {
                return Err(route_lookup_cancelled());
            }
            Ok(())
        })
    }

    fn lookup_topic_route(&self, topic: &CheetahString) -> ClusterTestLookupFuture<'_, Option<TopicRouteData>> {
        let topic = topic.clone();
        Box::pin(async move {
            let deadline = RequestDeadline::after(self.request_timeout);
            let cancellation = self.task_group.cancellation_token();
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(route_lookup_cancelled()),
                result = self.lookup_topic_route_until(&topic, deadline) => result,
            }
        })
    }

    fn shutdown(&self) -> ClusterTestLookupFuture<'_, ()> {
        Box::pin(async move {
            let report = self
                .task_group
                .shutdown_until(ShutdownDeadline::after(ROUTE_LOOKUP_SHUTDOWN_TIMEOUT))
                .await;
            report
                .assert_no_task_leak()
                .map_err(|error| RocketMQError::network_connection_failed(LOOKUP_OWNER, error.to_string()))
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

async fn resolve_socket_addresses(address_list: &str, deadline: RequestDeadline) -> RocketMQResult<Vec<SocketAddr>> {
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
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => return Err(route_lookup_timeout(deadline)),
        }
    }

    if resolved.is_empty() {
        return Err(RocketMQError::network_connection_failed(
            LOOKUP_OWNER,
            last_error.unwrap_or_else(|| "product environment returned no valid endpoints".to_string()),
        ));
    }
    Ok(resolved)
}

fn route_lookup_timeout(deadline: RequestDeadline) -> RocketMQError {
    RocketMQError::network_connection_timeout(LOOKUP_OWNER, deadline.budget_millis())
}

fn route_lookup_cancelled() -> RocketMQError {
    RocketMQError::network_connection_failed(LOOKUP_OWNER, "route lookup owner is shutting down")
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
    use rocketmq_transport::api::v1::ServerConfig;
    use rocketmq_transport::api::v1::ServerStartError;
    use rocketmq_transport::api::v2::HandlerOutcome;
    use rocketmq_transport::api::v2::RemotingRequest;
    use rocketmq_transport::api::v2::RequestProcessorV2;
    use rocketmq_transport::api::v2::ResponsePlan;
    use rocketmq_transport::api::v2::TransportServerV2;
    use tokio::sync::oneshot;
    use tokio::sync::Notify;

    use super::*;

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
            Box::pin(async move { Ok(endpoints) })
        }
    }

    #[derive(Clone)]
    struct RouteProcessor {
        route: TopicRouteData,
    }

    impl RequestProcessorV2 for RouteProcessor {
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

    impl RequestProcessorV2 for BlockingRouteProcessor {
        async fn process(&mut self, _request: &mut RemotingRequest) -> RocketMQResult<HandlerOutcome> {
            self.entered.notify_one();
            self.release.notified().await;
            response_outcome(RemotingCommand::create_response_command_with_code(
                ResponseCode::Success,
            ))
        }
    }

    fn response_outcome(response: RemotingCommand) -> RocketMQResult<HandlerOutcome> {
        let plan = ResponsePlan::from_command(response).map_err(|error| {
            RocketMQError::response_process_failed("namesrv.route_lookup_test.response_plan", error.to_string())
        })?;
        Ok(HandlerOutcome::Reply(plan))
    }

    struct RunningV2RouteServer {
        local_addr: SocketAddr,
        shutdown: Option<oneshot::Sender<()>>,
        result: oneshot::Receiver<Result<ShutdownReport, ServerStartError>>,
    }

    impl RunningV2RouteServer {
        async fn bind<P>(service: ChildServiceContext, processor: P) -> Self
        where
            P: RequestProcessorV2 + Clone + Sync + 'static,
        {
            let config = Arc::new(ServerConfig {
                bind_address: "127.0.0.1".to_owned(),
                listen_port: 0,
                ..ServerConfig::default()
            });
            let server = TransportServerV2::new(config, service.component("server"), processor);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let (startup_tx, startup_rx) = oneshot::channel();
            let (result_tx, result_rx) = oneshot::channel();
            service
                .component("runner")
                .spawn_service("namesrv.route-lookup-v2-test-server", async move {
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
                .expect("V2 route lookup test server should be lifecycle-owned");
            let local_addr = startup_rx
                .await
                .expect("V2 route lookup startup channel")
                .expect("V2 route lookup server should start");
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
                .expect("V2 route lookup server report channel")
                .expect("V2 route lookup server should stop cleanly");
            report
                .assert_no_task_leak()
                .expect("V2 route lookup server should not leak tasks");
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
        let server = RunningV2RouteServer::bind(
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
        let server = RunningV2RouteServer::bind(
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
