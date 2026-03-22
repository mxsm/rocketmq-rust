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
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;

use crate::config::ProxyConfig;
use crate::error::ProxyError;
use crate::error::ProxyResult;
use crate::grpc::middleware;
use crate::grpc::service::ProxyGrpcService;
use crate::processor::MessagingProcessor;
use crate::proto::v2::messaging_service_server::MessagingServiceServer;

pub async fn serve<P, F>(config: Arc<ProxyConfig>, service: ProxyGrpcService<P>, shutdown: F) -> ProxyResult<()>
where
    P: MessagingProcessor + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let addr = config.grpc.socket_addr()?;
    let listener = TcpListener::bind(addr).await.map_err(|error| ProxyError::Transport {
        message: format!("proxy gRPC server failed to bind {addr}: {error}"),
    })?;
    let local_addr = listener.local_addr().map_err(|error| ProxyError::Transport {
        message: format!("proxy gRPC server failed to resolve local address for {addr}: {error}"),
    })?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_signal = shutdown_tx.clone();
    let housekeeping_service = service.clone();
    tokio::spawn(async move {
        let mut shutdown_rx = shutdown_rx;
        housekeeping_service
            .run_housekeeping_until(async move {
                loop {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await;
    });
    let service = MessagingServiceServer::new(service)
        .max_decoding_message_size(config.grpc.max_decoding_message_size)
        .max_encoding_message_size(config.grpc.max_encoding_message_size);
    let service = InterceptedService::new(service, middleware::ingress_context_interceptor(local_addr));

    let result = Server::builder()
        .concurrency_limit_per_connection(config.grpc.concurrency_limit_per_connection)
        .add_service(service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            let _ = shutdown_signal.send(true);
        })
        .await;
    let _ = shutdown_tx.send(true);

    result.map_err(|error| ProxyError::Transport {
        message: format!("proxy gRPC server failed: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use hmac::Hmac;
    use hmac::Mac;
    use rocketmq_auth::authentication::enums::user_status::UserStatus;
    use rocketmq_auth::authentication::enums::user_type::UserType;
    use rocketmq_auth::authentication::model::user::User;
    use rocketmq_auth::authorization::enums::decision::Decision;
    use rocketmq_auth::authorization::model::acl::Acl;
    use rocketmq_auth::authorization::model::policy::Policy;
    use rocketmq_auth::authorization::model::resource::Resource;
    use rocketmq_common::common::action::Action;
    use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
    use sha1::Sha1;
    use tokio::sync::oneshot;
    use tonic::metadata::MetadataValue;
    use tonic::Request;

    use super::serve;
    use crate::auth::ProxyAuthRuntime;
    use crate::config::GrpcConfig;
    use crate::config::ProxyAuthConfig;
    use crate::config::ProxyConfig;
    use crate::context::ProxyContext;
    use crate::context::ResolvedEndpoint;
    use crate::error::ProxyResult;
    use crate::grpc::service::ProxyGrpcService;
    use crate::processor::DefaultMessagingProcessor;
    use crate::proto::v2;
    use crate::proto::v2::messaging_service_client::MessagingServiceClient;
    use crate::service::ClusterServiceManager;
    use crate::service::DefaultAssignmentService;
    use crate::service::DefaultConsumerService;
    use crate::service::DefaultMessageService;
    use crate::service::DefaultTransactionService;
    use crate::service::MetadataService;
    use crate::service::ProxyTopicMessageType;
    use crate::service::ResourceIdentity;
    use crate::service::RouteService;
    use crate::service::SubscriptionGroupMetadata;
    use crate::session::ClientSessionRegistry;

    const AUTH_TEST_DATETIME: &str = "20260322T010203Z";

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ObservedRouteContext {
        local_addr: Option<String>,
        remote_addr: Option<String>,
        principal: Option<String>,
    }

    #[derive(Default)]
    struct RecordingRouteService {
        observed: Mutex<Vec<ObservedRouteContext>>,
    }

    impl RecordingRouteService {
        fn take_observed(&self) -> Vec<ObservedRouteContext> {
            self.observed.lock().expect("route service mutex poisoned").clone()
        }
    }

    #[async_trait]
    impl RouteService for RecordingRouteService {
        async fn query_route(
            &self,
            context: &ProxyContext,
            _topic: &ResourceIdentity,
            _endpoints: &[ResolvedEndpoint],
        ) -> ProxyResult<TopicRouteData> {
            self.observed
                .lock()
                .expect("route service mutex poisoned")
                .push(ObservedRouteContext {
                    local_addr: context.local_addr().map(str::to_owned),
                    remote_addr: context.remote_addr().map(str::to_owned),
                    principal: context
                        .authenticated_principal()
                        .map(|principal| principal.username().to_owned()),
                });
            Ok(TopicRouteData::default())
        }
    }

    #[derive(Default)]
    struct StaticMetadataService;

    #[async_trait]
    impl MetadataService for StaticMetadataService {
        async fn topic_message_type(
            &self,
            _context: &ProxyContext,
            _topic: &ResourceIdentity,
        ) -> ProxyResult<ProxyTopicMessageType> {
            Ok(ProxyTopicMessageType::Normal)
        }

        async fn subscription_group(
            &self,
            _context: &ProxyContext,
            _topic: &ResourceIdentity,
            _group: &ResourceIdentity,
        ) -> ProxyResult<Option<SubscriptionGroupMetadata>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn serve_injects_transport_and_auth_context_into_query_route() {
        let route_service = Arc::new(RecordingRouteService::default());
        let metadata_service = Arc::new(StaticMetadataService);
        let manager = Arc::new(ClusterServiceManager::with_services(
            route_service.clone(),
            metadata_service,
            Arc::new(DefaultAssignmentService),
            Arc::new(DefaultMessageService),
            Arc::new(DefaultConsumerService),
            Arc::new(DefaultTransactionService),
        ));
        let processor = Arc::new(DefaultMessagingProcessor::new(manager));

        let port_probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free local port");
        let listen_addr = port_probe.local_addr().expect("discover free local port");
        drop(port_probe);

        let auth_runtime = test_auth_runtime(true, true).await;
        seed_normal_user(&auth_runtime, "alice", "secret").await;
        allow_topic_actions(
            &auth_runtime,
            "alice",
            "TopicA",
            vec![Action::Pub, Action::Sub, Action::Get],
        )
        .await;

        let config = Arc::new(ProxyConfig {
            grpc: GrpcConfig {
                listen_addr: listen_addr.to_string(),
                ..GrpcConfig::default()
            },
            ..ProxyConfig::default()
        });
        let service = ProxyGrpcService::new(config.clone(), processor, ClientSessionRegistry::default())
            .with_auth_runtime(Some(auth_runtime));

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            serve(config, service, async move {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let endpoint = format!("http://{listen_addr}");
        let mut client = connect_with_retry(endpoint.as_str()).await;
        let mut request = Request::new(v2::QueryRouteRequest {
            topic: Some(v2::Resource {
                resource_namespace: String::new(),
                name: "TopicA".to_owned(),
            }),
            endpoints: Some(v2::Endpoints {
                scheme: v2::AddressScheme::IPv4 as i32,
                addresses: vec![v2::Address {
                    host: "127.0.0.1".to_owned(),
                    port: 8081,
                }],
            }),
        });
        apply_auth_headers(&mut request, "client-a", "alice", "secret");

        let response = client
            .query_route(request)
            .await
            .expect("query route should succeed")
            .into_inner();
        assert_eq!(
            response.status.as_ref().map(|status| status.code),
            Some(v2::Code::Ok as i32)
        );

        let _ = shutdown_tx.send(());
        let serve_result = server_handle.await.expect("server task should join");
        assert!(
            serve_result.is_ok(),
            "server should shut down cleanly: {serve_result:?}"
        );

        let observed = route_service.take_observed();
        assert_eq!(observed.len(), 1);
        let expected_local_addr = listen_addr.to_string();
        assert_eq!(observed[0].local_addr.as_deref(), Some(expected_local_addr.as_str()));
        assert_eq!(observed[0].principal.as_deref(), Some("alice"));
        assert!(
            observed[0]
                .remote_addr
                .as_deref()
                .is_some_and(|remote| remote.starts_with("127.0.0.1:")),
            "expected remote address to be recorded, got {:?}",
            observed[0].remote_addr
        );
    }

    async fn connect_with_retry(endpoint: &str) -> MessagingServiceClient<tonic::transport::Channel> {
        let mut last_error = None;
        for _ in 0..20 {
            match MessagingServiceClient::connect(endpoint.to_owned()).await {
                Ok(client) => return client,
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
        panic!("gRPC client failed to connect to {endpoint}: {last_error:?}");
    }

    async fn test_auth_runtime(authentication_enabled: bool, authorization_enabled: bool) -> ProxyAuthRuntime {
        ProxyAuthRuntime::from_proxy_config(&ProxyAuthConfig {
            authentication_enabled,
            authorization_enabled,
            auth_config_path: format!("target/proxy-server-auth-tests-{}", uuid::Uuid::new_v4()),
            ..ProxyAuthConfig::default()
        })
        .await
        .expect("auth runtime should build")
        .expect("auth runtime should be enabled")
    }

    async fn seed_normal_user(auth_runtime: &ProxyAuthRuntime, username: &str, secret: &str) {
        let mut user = User::of_with_type(username, secret, UserType::Normal);
        user.set_user_status(UserStatus::Enable);
        auth_runtime.create_user(user).await.expect("user should be created");
    }

    async fn allow_topic_actions(auth_runtime: &ProxyAuthRuntime, username: &str, topic: &str, actions: Vec<Action>) {
        auth_runtime
            .create_acl(Acl::of(
                username,
                rocketmq_auth::authentication::enums::subject_type::SubjectType::User,
                Policy::of(vec![Resource::of_topic(topic)], actions, None, Decision::Allow),
            ))
            .await
            .expect("topic acl should be created");
    }

    fn apply_auth_headers<T>(request: &mut Request<T>, client_id: &str, username: &str, secret: &str) {
        type HmacSha1 = Hmac<Sha1>;

        let mut mac = HmacSha1::new_from_slice(secret.as_bytes()).expect("test hmac should build");
        mac.update(AUTH_TEST_DATETIME.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());
        let authorization =
            format!("MQv2-HMAC-SHA1 Credential={username}, SignedHeaders=x-mq-date-time, Signature={signature}");

        request.metadata_mut().insert(
            "x-mq-client-id",
            MetadataValue::try_from(client_id).expect("client id metadata"),
        );
        request
            .metadata_mut()
            .insert("x-mq-date-time", MetadataValue::from_static(AUTH_TEST_DATETIME));
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(authorization.as_str()).expect("auth metadata"),
        );
        request
            .metadata_mut()
            .insert("channel-id", MetadataValue::from_static("auth-channel"));
    }
}
