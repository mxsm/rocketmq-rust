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

use std::sync::Arc;
use std::time::Duration;

use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_model_gateway::AsyncBuiltinProviderClient;
use rocketmq_sre_model_gateway::CanonicalModelRequest;
use rocketmq_sre_model_gateway::DevSecretProvider;
use rocketmq_sre_model_gateway::HttpModelTransport;
use rocketmq_sre_model_gateway::HttpTransportConfig;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ProviderProfile;
use rocketmq_sre_model_gateway::SecretProvider;
use rocketmq_sre_model_gateway::ToolChoice;
use rocketmq_sre_model_gateway::current_unix_ms;

const PROFILE_ENV: &str = "ROCKETMQ_SRE_LIVE_PROVIDER_PROFILE_JSON";
const SECRET_PREFIX_ENV: &str = "ROCKETMQ_SRE_LIVE_PROVIDER_SECRET_PREFIX";

/// Credential-gated live smoke. With no profile it reports an explicit skip
/// and performs no network call. It never prints the prompt, response, endpoint,
/// profile JSON, or credential.
#[tokio::test]
#[ignore = "requires an explicitly configured live provider profile and credential reference"]
async fn live_builtin_provider_smoke_when_explicitly_configured() {
    let Ok(profile_json) = std::env::var(PROFILE_ENV) else {
        eprintln!("SKIPPED: {PROFILE_ENV} is not configured; no live provider call was attempted");
        return;
    };
    let profile: ProviderProfile =
        serde_json::from_str(&profile_json).expect("live provider profile JSON must be valid");
    profile.validate().expect("live provider profile must be valid");
    let secret_prefix = std::env::var(SECRET_PREFIX_ENV).unwrap_or_else(|_| "ROCKETMQ_SRE_LIVE_".to_owned());
    let secret_provider = DevSecretProvider::new(true, secret_prefix, None);
    let credential = profile.credential_ref.as_ref().map(|reference| {
        secret_provider
            .resolve(reference)
            .expect("referenced live provider credential must exist")
    });
    let transport = Arc::new(
        HttpModelTransport::new(
            HttpTransportConfig::default()
                .with_timeouts(Duration::from_secs(5), Duration::from_secs(20))
                .with_body_limits(32 * 1024, 64 * 1024),
        )
        .expect("live provider transport"),
    );
    let client = AsyncBuiltinProviderClient::new(profile.clone(), transport).expect("live provider client");
    let correlation_id = CorrelationId::new();
    let mut request = CanonicalModelRequest::new(
        correlation_id,
        profile.model,
        vec![ModelMessage::text(
            ModelRole::User,
            "Return the single word OK. Do not include analysis or sensitive data.",
        )],
    );
    request.tool_choice = ToolChoice::None;
    request.max_output_tokens = Some(16);
    request.temperature_milli = Some(0);
    let mut context = InvocationContext::new(correlation_id);
    context.deadline_unix_ms = Some(current_unix_ms().saturating_add(20_000));
    context.max_response_bytes = 64 * 1024;

    let response = client
        .invoke(&context, &request, credential)
        .await
        .expect("explicitly configured live provider call");
    assert!(
        !response.content.trim().is_empty(),
        "live provider returned empty content"
    );
    eprintln!("PASSED: live provider returned a bounded non-empty response; content was not printed");
}
