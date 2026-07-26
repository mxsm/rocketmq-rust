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

//! Bounded TCP/TLS transport ownership boundary.

mod admission;
mod base;
mod buffer;
mod client;
mod clients;
mod codec;
mod common;
mod config;
mod config_support;
mod connection;
mod connection_context;
mod deadline;
mod discovery;
mod error_helpers;
mod error_response;
mod local;
mod net;
mod remoting;
mod remoting_server;
mod request_ordering;
mod request_processor;
mod rpc;
mod runtime;
mod security;
mod server;
mod session_executor;
mod tls;

pub use admission::AdmissionClass;
pub use admission::AdmissionConfigError;
pub use admission::AdmissionController;
pub use admission::AdmissionLimits;
pub use admission::AdmissionResource;
pub use admission::AdmissionScope;
pub use admission::AdmissionSnapshot;
pub use admission::FullPolicy;
pub use admission::ResourceLimit;
pub use base::channel_event_listener::ChannelEventListener;
pub use base::connection_net_event::ConnectionNetEvent;
pub use base::pending_request_table::PendingRequestLimits;
pub use base::pending_request_table::PendingRequestTable;
pub use base::response_future::ResponseFuture;
pub use buffer::ByteBufferPool;
pub use client::connect_with_config;
pub use client::ConnectedTransport;
pub use client::TransportClient;
pub use clients::connection_pool::ConnectionMetrics;
pub use clients::connection_pool::ConnectionPool;
pub use clients::connection_pool::ConnectionPoolCleanupTask;
pub use clients::connection_pool::PoolStats;
pub use clients::connection_pool::PooledConnection;
pub use clients::reconnect::CircuitBreaker;
pub use clients::reconnect::ExponentialBackoff;
pub use clients::rocketmq_tokio_client::RemotingClientShutdownReport;
pub use clients::rocketmq_tokio_client::RocketmqDefaultClient;
pub use clients::Client;
pub use clients::RemotingClient;
pub use codec::remoting_command_codec::FrameLimits;
pub use codec::remoting_command_codec::RemotingCommandCodec;
pub use common::heartbeat_v2_result::HeartbeatV2Result;
pub use common::remoting_helper::RemotingHelper;
pub use config::ServerConfig;
pub use config::TlsClientAuth;
pub use config::TlsClientConfig;
pub use config::TlsConfig;
pub use config::TlsMode;
pub use config::TlsServerConfig;
pub use config_support::network_util::NetworkUtil;
pub use connection::transport_io_snapshot;
pub use connection::Connection;
pub use connection::ConnectionState;
pub use connection::TransportIoSnapshot;
pub use connection_context::ConnectionContext;
pub use deadline::RequestDeadline;
pub use discovery::default_top_addressing::DefaultTopAddressing;
pub use discovery::http_tiny_client::HttpResult;
pub use discovery::http_tiny_client::HttpTinyClient;
pub use discovery::name_server_update_callback::NameServerUpdateCallback;
pub use discovery::top_addressing::TopAddressing;
pub use local::LocalRequestHarness;
pub use net::channel::ArcChannel;
pub use net::channel::Channel;
pub use net::channel::ChannelId;
pub use net::channel::ChannelInner;
pub use remoting::InvokeCallback;
pub use remoting::RemotingService;
pub use remoting_server::rocketmq_tokio_server::run as run_remoting_server;
pub use remoting_server::rocketmq_tokio_server::run_with_report as run_remoting_server_with_report;
pub use remoting_server::rocketmq_tokio_server::run_with_report_with_service_context as run_remoting_server_with_report_with_service_context;
pub use remoting_server::rocketmq_tokio_server::RocketMQServer;
pub use remoting_server::RemotingServer;
pub use request_ordering::RequestOrdering;
pub use request_ordering::RequestOrderingKey;
pub use request_processor::default_request_processor::DefaultRemotingRequestProcessor;
pub use rocketmq_protocol::protocol::RemotingDeserializable;
pub use rocketmq_protocol::protocol::RemotingSerializable;
pub use rpc::client_metadata::ClientMetadata;
pub use rpc::rpc_client::RpcClient;
pub use rpc::rpc_client::RpcClientLocal;
pub use rpc::rpc_client_hook::RpcClientHook;
pub use rpc::rpc_client_hook::RpcClientHookFn;
pub use rpc::rpc_client_impl::RpcClientImpl;
pub use rpc::rpc_client_utils::RpcClientUtils;
pub use rpc::rpc_request::RpcRequest;
pub use rpc::rpc_request_header::RpcRequestHeader;
pub use rpc::rpc_response::RpcResponse;
pub use rpc::topic_request_header::TopicRequestHeader;
pub use runtime::config::client_config::TokioClientConfig;
pub use runtime::connection_handler_context::ConnectionHandlerContext;
pub use runtime::connection_handler_context::ConnectionHandlerContextWrapper;
pub use runtime::processor::LocalRequestProcessor;
pub use runtime::processor::RejectRequestResponse;
pub use runtime::processor::RequestProcessor as RemotingRequestProcessor;
pub use runtime::processor_v2::AdminProcessorExample;
pub use runtime::processor_v2::CoreProcessor;
pub use runtime::processor_v2::CoreProcessorVariant;
pub use runtime::processor_v2::PluginProcessorRegistry;
pub use runtime::processor_v2::ProcessorDispatcher;
pub use runtime::processor_v2::PullMessageProcessorExample;
pub use runtime::processor_v2::RequestProcessorV2;
pub use runtime::processor_v2::SendMessageProcessorExample;
pub use runtime::RPCHook;
pub use runtime::RPCHookArc;
pub use security::TransportSecurity;
pub use server::run_connected_session;
pub use server::ConnectionHandler;
pub use server::RequestProcessor as SessionRequestProcessor;
pub use server::SessionHandle;
pub use server::TransportListener;
pub use server::TransportServer;
pub use server::TransportServerConfig;
pub use tls::tls_disabled_error;
#[cfg(feature = "tls")]
pub use tls::TlsReloadReport;
pub use tls::TlsServerRuntime;

pub use error_helpers::abort_process_error;
pub use error_helpers::channel_recv_failed;
pub use error_helpers::channel_send_failed;
pub use error_helpers::connection_invalid;
pub use error_helpers::decoder_error;
pub use error_helpers::decoding_error;
pub use error_helpers::deserialize_header_error;
pub use error_helpers::encoder_error;
pub use error_helpers::illegal_argument;
pub use error_helpers::io_error;
pub use error_helpers::remote_error;
pub use error_helpers::unsupported_serialize_type;
pub use error_response::apply_error_to_response;
pub use error_response::command_from_error;
pub use error_response::command_from_error_with_opaque;
pub use error_response::command_from_error_with_remark;
pub use error_response::command_from_error_with_remark_and_opaque;
pub use error_response::internal_error;
pub use error_response::internal_error_with_opaque;
pub use error_response::invalid_parameter_with_remark;
pub use error_response::invalid_parameter_with_remark_and_opaque;
pub use error_response::no_permission_with_remark;
pub use error_response::no_permission_with_remark_and_opaque;
pub use error_response::query_not_found_with_remark;
pub use error_response::query_not_found_with_remark_and_opaque;
pub use error_response::request_code_not_supported;
pub use error_response::request_code_not_supported_with_opaque;
pub use error_response::request_code_not_supported_with_remark;
pub use error_response::request_code_not_supported_with_remark_and_opaque;
