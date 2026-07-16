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
use rocketmq_common::common::mix_all;
use rocketmq_common::common::namesrv::default_top_addressing::DefaultTopAddressing;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::TaskGroup;
use rocketmq_transport::admission::AdmissionController;
use rocketmq_transport::admission::AdmissionLimits;
use rocketmq_transport::client::TransportClient;

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
    fn resolve(&self, deadline: ShutdownDeadline) -> EndpointResolveFuture<'_>;
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
    fn resolve(&self, deadline: ShutdownDeadline) -> EndpointResolveFuture<'_> {
        Box::pin(async move {
            if deadline.is_expired() {
                return Err(route_lookup_timeout(deadline));
            }

            let timeout_at = tokio::time::Instant::from_std(deadline.instant());
            let timeout_millis = deadline.remaining().as_millis().min(u128::from(u64::MAX)).max(1) as u64;
            let address_list = tokio::time::timeout_at(
                timeout_at,
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
    transport: TransportClient,
    task_group: TaskGroup,
    cached_endpoints: RwLock<Vec<SocketAddr>>,
    request_timeout: Duration,
}

impl TransportClusterTestRouteLookup {
    pub(crate) fn new(product_env_name: &str, service_context: ServiceContext) -> Self {
        Self::with_resolver(
            service_context,
            Arc::new(ProductEnvironmentEndpointResolver::new(product_env_name)),
            ROUTE_LOOKUP_TIMEOUT,
        )
    }

    fn with_resolver(
        service_context: ServiceContext,
        resolver: Arc<dyn ClusterTestEndpointResolver>,
        request_timeout: Duration,
    ) -> Self {
        let task_group = service_context.task_group().clone();
        let transport = TransportClient::new(
            service_context.child("transport"),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        );
        Self {
            resolver,
            transport,
            task_group,
            cached_endpoints: RwLock::new(Vec::new()),
            request_timeout,
        }
    }

    async fn lookup_topic_route_until(
        &self,
        topic: &CheetahString,
        deadline: ShutdownDeadline,
    ) -> RocketMQResult<Option<TopicRouteData>> {
        let endpoints = self.resolve_endpoints(deadline).await?;
        let mut last_error = None;

        for endpoint in endpoints {
            if deadline.is_expired() {
                return Err(route_lookup_timeout(deadline));
            }

            match self.transport.invoke(endpoint, route_request(topic), deadline).await {
                Ok(response) => return decode_route_response(response),
                Err(error) => last_error = Some(error),
            }
        }

        self.cached_endpoints.write().clear();
        Err(last_error.unwrap_or_else(|| {
            RocketMQError::network_connection_failed(LOOKUP_OWNER, "no product-environment endpoint was reachable")
        }))
    }

    async fn resolve_endpoints(&self, deadline: ShutdownDeadline) -> RocketMQResult<Vec<SocketAddr>> {
        let cached = self.cached_endpoints.read().clone();
        if !cached.is_empty() {
            return Ok(cached);
        }

        let resolved = self.resolver.resolve(deadline).await?;
        if resolved.is_empty() {
            return Err(RocketMQError::network_connection_failed(
                LOOKUP_OWNER,
                "product environment resolved to an empty endpoint list",
            ));
        }
        *self.cached_endpoints.write() = resolved.clone();
        Ok(resolved)
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
            let deadline = ShutdownDeadline::after(self.request_timeout);
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

fn route_request(topic: &CheetahString) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new(topic.clone(), None),
    );
    request.make_custom_header_to_net();
    request
}

fn decode_route_response(response: RemotingCommand) -> RocketMQResult<Option<TopicRouteData>> {
    let code = response.code();
    match ResponseCode::from(code) {
        ResponseCode::Success => {
            let body = response.body().ok_or_else(|| {
                RpcClientError::remote_error(code, "successful route response did not include a body")
            })?;
            TopicRouteData::decode(body.as_ref()).map(Some)
        }
        ResponseCode::TopicNotExist => Ok(None),
        _ => Err(RpcClientError::remote_error(
            code,
            response.remark().map_or("route lookup failed", CheetahString::as_str),
        )
        .into()),
    }
}

async fn resolve_socket_addresses(address_list: &str, deadline: ShutdownDeadline) -> RocketMQResult<Vec<SocketAddr>> {
    let timeout_at = tokio::time::Instant::from_std(deadline.instant());
    let mut resolved = Vec::new();
    let mut last_error = None;

    for endpoint in address_list.split(';').map(str::trim).filter(|item| !item.is_empty()) {
        if let Ok(address) = endpoint.parse::<SocketAddr>() {
            if !resolved.contains(&address) {
                resolved.push(address);
            }
            continue;
        }

        match tokio::time::timeout_at(timeout_at, tokio::net::lookup_host(endpoint)).await {
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

fn route_lookup_timeout(deadline: ShutdownDeadline) -> RocketMQError {
    RocketMQError::network_timeout(LOOKUP_OWNER, deadline.remaining())
}

fn route_lookup_cancelled() -> RocketMQError {
    RocketMQError::network_connection_failed(LOOKUP_OWNER, "route lookup owner is shutting down")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;
    use rocketmq_protocol::protocol::RemotingSerializable;
    use rocketmq_runtime::RuntimeContext;
    use rocketmq_transport::server::RequestProcessor;
    use rocketmq_transport::server::TransportServer;
    use rocketmq_transport::server::TransportServerConfig;
    use tokio::sync::Notify;

    use super::*;

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
        fn resolve(&self, _deadline: ShutdownDeadline) -> EndpointResolveFuture<'_> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let endpoints = self.endpoints.clone();
            Box::pin(async move { Ok(endpoints) })
        }
    }

    struct RouteProcessor {
        route: TopicRouteData,
    }

    impl RequestProcessor for RouteProcessor {
        fn process(
            &self,
            request: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
            Box::pin(async move {
                assert_eq!(request.code(), RequestCode::GetRouteinfoByTopic as i32);
                let header = request.decode_command_custom_header::<GetRouteInfoRequestHeader>()?;
                assert_eq!(header.topic, CheetahString::from("missing-topic"));
                Ok(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque())
                        .set_body(self.route.encode()?),
                )
            })
        }
    }

    struct BlockingRouteProcessor {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl RequestProcessor for BlockingRouteProcessor {
        fn process(
            &self,
            request: RemotingCommand,
        ) -> Pin<Box<dyn Future<Output = RocketMQResult<RemotingCommand>> + Send + '_>> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(
                    RemotingCommand::create_response_command_with_code(ResponseCode::Success)
                        .set_opaque(request.opaque()),
                )
            })
        }
    }

    struct BlockingResolver {
        entered: Arc<Notify>,
    }

    impl ClusterTestEndpointResolver for BlockingResolver {
        fn resolve(&self, _deadline: ShutdownDeadline) -> EndpointResolveFuture<'_> {
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
        let server = TransportServer::bind(
            runtime.service_context("route-server"),
            TransportServerConfig::loopback(),
            Arc::new(RouteProcessor { route: sample_route() }),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .await
        .unwrap();
        let address = server.local_addr();
        server.start().unwrap();

        let resolver = Arc::new(FixedEndpointResolver::new(vec![address]));
        let lookup = TransportClusterTestRouteLookup::with_resolver(
            runtime.service_context("route-lookup"),
            resolver.clone(),
            Duration::from_secs(1),
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
        server
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await
            .assert_no_task_leak()
            .unwrap();
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
        let server = TransportServer::bind(
            runtime.service_context("hung-route-server"),
            TransportServerConfig::loopback(),
            Arc::new(BlockingRouteProcessor {
                entered: entered.clone(),
                release: release.clone(),
            }),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .await
        .unwrap();
        let address = server.local_addr();
        server.start().unwrap();

        let lookup = Arc::new(TransportClusterTestRouteLookup::with_resolver(
            runtime.service_context("route-lookup"),
            Arc::new(FixedEndpointResolver::new(vec![address])),
            Duration::from_millis(50),
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
        server
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(1)))
            .await
            .assert_no_task_leak()
            .unwrap();
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
