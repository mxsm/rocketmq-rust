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

//! Minimal process-external Provider SPI contract example.
//!
//! A real adapter maps this implementation to the versioned gRPC service,
//! terminates mTLS with its own SPIFFE workload identity, and resolves its own
//! `adapter://` credential reference. The gateway never sends credential
//! material to it.

use std::sync::Arc;

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_model_gateway::BoundedModelStream;
use rocketmq_sre_model_gateway::CancellationToken;
use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::CanonicalModelResponse;
use rocketmq_sre_model_gateway::CredentialOwner;
use rocketmq_sre_model_gateway::FinishReason;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ModelStreamEvent;
use rocketmq_sre_model_gateway::ProviderCapabilities;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::ProviderHealth;
use rocketmq_sre_model_gateway::ProviderSpi;
use rocketmq_sre_model_gateway::ProviderSpiClient;
use rocketmq_sre_model_gateway::SpiCancelRequest;
use rocketmq_sre_model_gateway::SpiClientConfig;
use rocketmq_sre_model_gateway::SpiHandshakeRequest;
use rocketmq_sre_model_gateway::SpiHandshakeResponse;
use rocketmq_sre_model_gateway::SpiHealth;
use rocketmq_sre_model_gateway::SpiInvokeRequest;
use rocketmq_sre_model_gateway::SpiStreamRequest;

struct ExampleAdapter;

impl ProviderSpi for ExampleAdapter {
    fn handshake(&self, request: &SpiHandshakeRequest) -> Result<SpiHandshakeResponse, ProviderError> {
        Ok(SpiHandshakeResponse {
            wire_version: request.wire_version.clone(),
            adapter_identity: "spiffe://sre/provider/example".to_owned(),
            credential_owner: CredentialOwner::Adapter,
            capabilities: ProviderCapabilities::chat_default(),
            credential_version_fingerprint: Some("version:example-v1".to_owned()),
        })
    }

    fn invoke(&self, _request: &SpiInvokeRequest) -> Result<CanonicalModelResponse, ProviderError> {
        Ok(CanonicalModelResponse::text(
            "example-spi",
            "example-model",
            "adapter response",
            FinishReason::Stop,
        ))
    }

    fn invoke_stream(&self, request: &SpiStreamRequest) -> Result<BoundedModelStream, ProviderError> {
        let (sink, stream) = BoundedModelStream::channel(request.bounds, CancellationToken::default())?;
        sink.try_send(ModelStreamEvent::Finish {
            reason: FinishReason::Stop,
        })?;
        Ok(stream)
    }

    fn cancel(&self, _request: &SpiCancelRequest) -> Result<(), ProviderError> {
        Ok(())
    }

    fn health(&self) -> Result<SpiHealth, ProviderError> {
        Ok(SpiHealth {
            status: ProviderHealth::Healthy,
            credential_version_fingerprint: Some("version:example-v1".to_owned()),
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ProviderSpiClient::connect(
        Arc::new(ExampleAdapter),
        SpiClientConfig::mutual_tls("spiffe://sre/gateway", "spiffe://sre/provider/example"),
    )?;
    let request = CanonicalModelRequest::new(
        CorrelationId::new(),
        "example-model",
        vec![ModelMessage::text(ModelRole::User, "health summary")],
    );
    let response = client.invoke(&InvocationContext::new(request.correlation_id), &request)?;
    println!("{}", response.content);
    Ok(())
}
