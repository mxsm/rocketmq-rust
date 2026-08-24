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

//! Deliberate, versioned transport entry points.

pub use crate::admission::AdmissionClass;
pub use crate::admission::AdmissionConfigError;
pub use crate::admission::AdmissionController;
pub use crate::admission::AdmissionLimits;
pub use crate::admission::AdmissionResource;
pub use crate::admission::AdmissionScope;
pub use crate::admission::AdmissionSnapshot;
pub use crate::admission::FullPolicy;
pub use crate::admission::ResourceLimit;
pub use crate::admission::ResourceSnapshot;
pub use crate::base::channel_event_listener::ChannelEventListener;
pub use crate::base::connection_net_event::ConnectionNetEvent;
pub use crate::client::OneShotTransportClient;
pub use crate::clients::nameserver_endpoint::diff_name_server_endpoints;
pub use crate::clients::nameserver_endpoint::ConnectTarget;
pub use crate::clients::nameserver_endpoint::NameServerEndpoint;
pub use crate::clients::nameserver_endpoint::NameServerEndpointDiff;
pub use crate::clients::rocketmq_tokio_client::CachedConnectionState;
pub use crate::clients::rocketmq_tokio_client::ClientShutdownReport;
pub use crate::clients::rocketmq_tokio_client::ClientSnapshot;
pub use crate::clients::rocketmq_tokio_client::ClientStartReport;
pub use crate::clients::rocketmq_tokio_client::ConnectionShutdownReport;
pub use crate::clients::rocketmq_tokio_client::PendingUsage;
pub use crate::clients::rocketmq_tokio_client::RemotingClient;
pub use crate::clients::rocketmq_tokio_client::RemotingClientBuilder;
pub use crate::clients::rocketmq_tokio_client::RequestTarget;
pub use crate::clients::rocketmq_tokio_client::SendReceipt;
pub use crate::clients::rocketmq_tokio_client::TransportClient;
pub use crate::clients::rocketmq_tokio_client::TransportClientBuilder;
pub use crate::codec::remoting_command_codec::FrameLimits;
pub use crate::common::heartbeat_v2_result::HeartbeatV2Result;
pub use crate::common::remoting_helper::RemotingHelper;
pub use crate::config::ServerConfig;
pub use crate::config::SocketOptions;
pub use crate::config::TcpKeepaliveConfig;
pub use crate::config::TlsClientAuth;
pub use crate::config::TlsClientConfig;
pub use crate::config::TlsConfig;
pub use crate::config::TlsMode;
pub use crate::config::TlsServerConfig;
pub use crate::config_support::network_util::NetworkUtil;
pub use crate::connection::ConnectionState;
pub use crate::connection_context::ConnectionContext;
pub use crate::deadline::RequestDeadline;
pub use crate::discovery::default_top_addressing::DefaultTopAddressing;
pub use crate::discovery::http_tiny_client::HttpResult;
pub use crate::discovery::http_tiny_client::HttpTinyClient;
pub use crate::discovery::name_server_update_callback::NameServerUpdateCallback;
pub use crate::discovery::top_addressing::TopAddressing;
pub use crate::dispatch::AuthorizedCommandDispatcher;
pub use crate::dispatch::AuthorizedDispatchBoundary;
pub use crate::dispatch::DispatchError;
pub use crate::dispatch::DispatchOutcome;
pub use crate::dispatch::LocalResponseReceiver;
pub use crate::dispatch::RequestContext;
pub use crate::dispatch::RequestContextError;
pub use crate::dispatch::RequestTransport;
pub use crate::dispatch::ResponseSink;
pub use crate::dispatch::ResponseSinkError;
pub use crate::file_region::FileRegion;
pub use crate::file_region::FileRegionLease;
pub use crate::file_region::FileRegionSequence;
pub use crate::file_region::FileTransferMode;
pub use crate::file_region_writer::file_transfer_snapshot;
pub use crate::file_region_writer::FileTransferSnapshot;
pub use crate::net::channel::ArcChannel;
pub use crate::net::channel::Channel;
pub use crate::net::channel::ChannelId;
pub use crate::proxy_protocol::read_proxy_protocol;
pub use crate::proxy_protocol::ProxyProtocolConfig;
pub use crate::proxy_protocol::ProxyProtocolMetadata;
pub use crate::proxy_protocol::UnknownTlvPolicy;
pub use crate::remoting_server::rocketmq_tokio_server::ServerStartError;
pub use crate::remoting_server::rocketmq_tokio_server::TransportServer;
pub use crate::request_ordering::RequestOrdering;
pub use crate::request_ordering::RequestOrderingKey;
pub use crate::request_processor::default_request_processor::DefaultRequestProcessor;
pub use crate::rpc::client_metadata::ClientMetadata;
pub use crate::rpc::rpc_client::RpcClient;
pub use crate::rpc::rpc_client::RpcClientLocal;
pub use crate::rpc::rpc_client_hook::RpcClientHookFn;
pub use crate::rpc::rpc_client_impl::RpcClientImpl;
pub use crate::rpc::rpc_client_utils::RpcClientUtils;
pub use crate::rpc::rpc_request::RpcRequest;
pub use crate::rpc::rpc_request_header::RpcRequestHeader;
pub use crate::rpc::rpc_response::RpcResponse;
pub use crate::rpc::topic_request_header::TopicRequestHeader;
pub use crate::runtime::config::client_config::ConnectConfig;
pub use crate::runtime::config::client_config::GoAwayPolicy;
pub use crate::runtime::config::client_config::MaintenanceConfig;
pub use crate::runtime::config::client_config::TransportClientConfig;
pub use crate::runtime::connection_handler_context::ConnectionHandlerContext;
pub use crate::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
pub use crate::runtime::processor::LocalRequestProcessor;
pub use crate::runtime::processor::RejectRequestResponse;
pub use crate::runtime::processor::RequestProcessor;
pub use crate::runtime::processor::ResponseWriteObservation;
pub use crate::runtime::processor::ResponseWriteOutcome;
pub use crate::runtime::RPCHook;
pub use crate::runtime::RPCHookArc;
pub use crate::security::TransportSecurity;
#[cfg(feature = "socks")]
pub use crate::socks::SocksProxyConfig;
#[cfg(feature = "socks")]
pub use crate::socks::SocksProxyRoute;
pub use crate::telemetry::TransportTelemetry;
#[cfg(feature = "tls")]
pub use crate::tls::build_server_acceptor_exact;
#[cfg(feature = "tls")]
pub use crate::tls::build_server_acceptor_exact_with_alpn;
#[cfg(feature = "tls")]
pub use crate::tls::PrivateKeyLoader;
pub use crate::tls::TlsServerRuntime;
pub use rocketmq_protocol::protocol::RemotingDeserializable;
pub use rocketmq_protocol::protocol::RemotingSerializable;

pub use crate::error_response::apply_error_to_response;
pub use crate::error_response::command_from_error;
pub use crate::error_response::command_from_error_with_factory_and_opaque;
pub use crate::error_response::command_from_error_with_factory_remark_and_opaque;
pub use crate::error_response::command_from_error_with_opaque;
pub use crate::error_response::command_from_error_with_remark;
pub use crate::error_response::command_from_error_with_remark_and_opaque;
pub use crate::error_response::internal_error;
pub use crate::error_response::internal_error_with_factory_and_opaque;
pub use crate::error_response::internal_error_with_opaque;
pub use crate::error_response::invalid_parameter_with_remark;
pub use crate::error_response::invalid_parameter_with_remark_and_opaque;
pub use crate::error_response::no_permission_with_remark;
pub use crate::error_response::no_permission_with_remark_and_opaque;
pub use crate::error_response::query_not_found_with_remark;
pub use crate::error_response::query_not_found_with_remark_and_opaque;
pub use crate::error_response::request_code_not_supported;
pub use crate::error_response::request_code_not_supported_with_factory;
pub use crate::error_response::request_code_not_supported_with_factory_and_opaque;
pub use crate::error_response::request_code_not_supported_with_factory_and_remark;
pub use crate::error_response::request_code_not_supported_with_factory_remark_and_opaque;
pub use crate::error_response::request_code_not_supported_with_opaque;
pub use crate::error_response::request_code_not_supported_with_remark;
pub use crate::error_response::request_code_not_supported_with_remark_and_opaque;
