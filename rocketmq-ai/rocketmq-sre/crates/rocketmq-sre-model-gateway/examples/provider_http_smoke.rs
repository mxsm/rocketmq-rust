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
use rocketmq_sre_model_gateway::HttpModelTransport;
use rocketmq_sre_model_gateway::HttpTransportConfig;
use rocketmq_sre_model_gateway::InvocationContext;
use rocketmq_sre_model_gateway::ModelMessage;
use rocketmq_sre_model_gateway::ModelRole;
use rocketmq_sre_model_gateway::ProviderError;
use rocketmq_sre_model_gateway::ProviderErrorCode;
use rocketmq_sre_model_gateway::SecretMaterial;
use rocketmq_sre_model_gateway::builtin_provider_profiles;
use rocketmq_sre_model_gateway::current_unix_ms;

const PROFILE_ENV: &str = "ROCKETMQ_SRE_MODEL_SMOKE_PROFILE";
const MODEL_ENV: &str = "ROCKETMQ_SRE_MODEL_SMOKE_MODEL";
const ENDPOINT_ENV: &str = "ROCKETMQ_SRE_MODEL_SMOKE_ENDPOINT";
const CREDENTIAL_ENV: &str = "ROCKETMQ_SRE_MODEL_SMOKE_CREDENTIAL";

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match run().await {
        Ok(SmokeOutcome::Skipped(reason)) => println!("provider HTTP smoke skipped: {reason}"),
        Ok(SmokeOutcome::Invoked {
            profile,
            model,
            finish_reason,
            input_tokens,
            output_tokens,
        }) => println!(
            "provider HTTP smoke passed: profile={profile}, model={model}, finish_reason={finish_reason:?}, \
             input_tokens={input_tokens:?}, output_tokens={output_tokens:?}"
        ),
        Err(error) => {
            eprintln!("provider HTTP smoke failed: {error}");
            std::process::exit(1);
        }
    }
}

enum SmokeOutcome {
    Skipped(&'static str),
    Invoked {
        profile: String,
        model: String,
        finish_reason: rocketmq_sre_model_gateway::FinishReason,
        input_tokens: Option<u32>,
        output_tokens: Option<u32>,
    },
}

async fn run() -> Result<SmokeOutcome, ProviderError> {
    let Some(raw_credential) = non_empty_env(CREDENTIAL_ENV) else {
        return Ok(SmokeOutcome::Skipped(
            "ROCKETMQ_SRE_MODEL_SMOKE_CREDENTIAL is not set; no network request was made",
        ));
    };
    let Some(model) = non_empty_env(MODEL_ENV) else {
        return Ok(SmokeOutcome::Skipped(
            "ROCKETMQ_SRE_MODEL_SMOKE_MODEL is not set; no network request was made",
        ));
    };
    let profile_id = non_empty_env(PROFILE_ENV).unwrap_or_else(|| "openai".to_owned());
    let mut profile = builtin_provider_profiles()
        .into_iter()
        .find(|candidate| candidate.id == profile_id)
        .ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::ProfileInvalid,
                "requested smoke provider profile does not exist",
            )
        })?;
    if profile.credential_ref.is_none() {
        return Ok(SmokeOutcome::Skipped(
            "the selected profile is unauthenticated; this smoke only invokes explicitly credentialed providers",
        ));
    }
    if let Some(endpoint) = non_empty_env(ENDPOINT_ENV) {
        profile.endpoint = endpoint;
    }
    profile.model.clone_from(&model);

    let transport = Arc::new(HttpModelTransport::new(
        HttpTransportConfig::default()
            .with_timeouts(Duration::from_secs(5), Duration::from_secs(30))
            .with_body_limits(256 * 1024, 512 * 1024),
    )?);
    let client = AsyncBuiltinProviderClient::new(profile, transport)?;
    let correlation_id = CorrelationId::new();
    let mut context = InvocationContext::new(correlation_id);
    context.deadline_unix_ms = Some(current_unix_ms().saturating_add(30_000));
    context.max_response_bytes = 512 * 1024;
    let mut request = CanonicalModelRequest::new(
        correlation_id,
        model,
        vec![ModelMessage::text(
            ModelRole::User,
            "Reply with exactly: rocketmq-sre-smoke-ok",
        )],
    );
    request.max_output_tokens = Some(32);

    let response = client
        .invoke(
            &context,
            &request,
            Some(SecretMaterial::new(raw_credential, "manual-smoke", None)),
        )
        .await?;
    Ok(SmokeOutcome::Invoked {
        profile: client.profile_id().to_owned(),
        model: response.model,
        finish_reason: response.finish_reason,
        input_tokens: response.usage.input_tokens,
        output_tokens: response.usage.output_tokens,
    })
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
