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

//! Provider-neutral model gateway for RocketMQ Rust AI SRE.
//!
//! The crate owns canonical chat/embedding/rerank contracts, protocol
//! translation, capability-aware routing, bounded streaming, secret references,
//! and a versioned process-external provider SPI. Production cloud networking
//! uses the bounded asynchronous [`HttpModelTransport`]; the synchronous
//! [`ModelTransport`] remains an injected compatibility boundary for fixtures.
//! No cloud SDK or RocketMQ/MCP/executor credential is part of this dependency
//! graph.

mod adapters;
mod async_client;
mod aws_sigv4;
mod critic;
mod error;
mod fixtures;
mod grpc_spi;
mod http_transport;
mod ir;
mod profile;
mod provider;
mod router;
mod secret;
mod spi;
mod stream;
mod transport;
mod vault_agent;

pub use adapters::AnthropicMessagesAdapter;
pub use adapters::BedrockConverseAdapter;
pub use adapters::GeminiNativeAdapter;
pub use adapters::OpenAiCompatibleAdapter;
pub use adapters::adapter_for_profile;
pub use async_client::AsyncBuiltinProviderClient;
pub use critic::heterogeneous_critic_profiles;
pub use critic::normalize_model_family;
pub use error::ProviderError;
pub use error::ProviderErrorCode;
pub use error::map_provider_status;
pub use fixtures::phase00_provider_descriptors;
pub use grpc_spi::GrpcProviderSpiClient;
pub use grpc_spi::GrpcProviderSpiStream;
pub use grpc_spi::GrpcSpiClientTlsConfig;
pub use grpc_spi::bounded_provider_adapter_service;
pub use grpc_spi::wire as provider_spi_wire;
pub use http_transport::HttpModelTransport;
pub use http_transport::HttpTlsConfig;
pub use http_transport::HttpTransportConfig;
pub use http_transport::TlsClientIdentity;
pub use ir::CanonicalEmbeddingRequest;
pub use ir::CanonicalEmbeddingResponse;
pub use ir::CanonicalModelRequest;
pub use ir::CanonicalModelResponse;
pub use ir::CanonicalRerankRequest;
pub use ir::CanonicalRerankResponse;
pub use ir::FinishReason;
pub use ir::ModelContentPart;
pub use ir::ModelImageSource;
pub use ir::ModelMessage;
pub use ir::ModelRequestExtensions;
pub use ir::ModelRole;
pub use ir::ModelStreamEvent;
pub use ir::ModelTool;
pub use ir::ModelToolCall;
pub use ir::ModelUsage;
pub use ir::RerankDocument;
pub use ir::RerankResult;
pub use ir::ResponseFormat;
pub use ir::ToolChoice;
pub use profile::DataClass;
pub use profile::ProviderCapabilities;
pub use profile::ProviderCapability;
pub use profile::ProviderDialect;
pub use profile::ProviderFamily;
pub use profile::ProviderHealth;
pub use profile::ProviderProfile;
pub use profile::ProviderProfileManifest;
pub use profile::ProviderProfileManifestEntry;
pub use profile::builtin_provider_profiles;
pub use provider::ChatModelProvider;
pub use provider::EmbeddingProvider;
pub use provider::InvocationContext;
pub use provider::RerankProvider;
pub use router::DiagnosisModelSelection;
pub use router::FallbackAttempt;
pub use router::InvocationMetadata;
pub use router::InvocationPurpose;
pub use router::ModelInvocationId;
pub use router::ModelInvocationOutcome;
pub use router::ModelInvocationRecord;
pub use router::ModelInvocationResult;
pub use router::ProviderRegistry;
pub use router::ProviderRouter;
pub use router::RoutingPolicy;
pub use router::RoutingRequirements;
pub use router::RulesOnlyResult;
pub use secret::DevSecretProvider;
pub use secret::ExternalSecretClient;
pub use secret::ExternalSecretManagerProvider;
pub use secret::ExternalSecretValue;
pub use secret::SecretMaterial;
pub use secret::SecretProvider;
pub use secret::SecretReference;
pub use secret::SecretReferenceKind;
pub use secret::current_unix_ms;
pub use spi::CredentialOwner;
pub use spi::PROVIDER_SPI_WIRE_VERSION;
pub use spi::ProviderSpi;
pub use spi::ProviderSpiChatAdapter;
pub use spi::ProviderSpiClient;
pub use spi::SpiCancelRequest;
pub use spi::SpiClientConfig;
pub use spi::SpiHandshakeRequest;
pub use spi::SpiHandshakeResponse;
pub use spi::SpiHealth;
pub use spi::SpiInvokeRequest;
pub use spi::SpiStreamRequest;
pub use stream::BoundedModelStream;
pub use stream::CancellationToken;
pub use stream::StreamBounds;
pub use stream::StreamSink;
pub use transport::AsyncModelTransport;
pub use transport::ModelTransport;
pub use transport::TransportFuture;
pub use transport::TransportRequest;
pub use transport::TransportResponse;
pub use vault_agent::VaultAgentFileSecretClient;
pub use vault_agent::VaultAgentVersionSource;
