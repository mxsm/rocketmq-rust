// Copyright 2026 The RocketMQ Rust Authors
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

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::Hmac;
use hmac::KeyInit;
use hmac::Mac;
use rocketmq_sre_contracts::GovernanceSignature;
use rocketmq_sre_contracts::GovernanceSignaturePayload;
use sha2::Digest;
use sha2::Sha256;

use crate::ControlPlaneError;

const ALGORITHM: &str = "hmac-sha256";

#[derive(Clone)]
pub(super) struct GovernanceSigner {
    key: Arc<[u8]>,
    key_id: Arc<str>,
}

impl GovernanceSigner {
    pub(super) fn new(key: impl AsRef<[u8]>) -> Result<Self, ControlPlaneError> {
        let key = key.as_ref();
        if key.len() < 32 {
            return Err(ControlPlaneError::configuration(
                "governance signing key must contain at least 32 bytes",
            ));
        }
        let digest = Sha256::digest(key);
        let short_digest = digest[..8].iter().map(|byte| format!("{byte:02x}")).collect::<String>();
        Ok(Self {
            key: Arc::from(key),
            key_id: Arc::from(format!("governance-{short_digest}")),
        })
    }

    pub(super) fn sign(&self, payload: &GovernanceSignaturePayload) -> Result<GovernanceSignature, ControlPlaneError> {
        let encoded = serde_jcs::to_vec(payload)
            .map_err(|_| ControlPlaneError::configuration("governance signature payload cannot be encoded"))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| ControlPlaneError::configuration("governance signing key is invalid"))?;
        mac.update(&encoded);
        Ok(GovernanceSignature {
            algorithm: ALGORITHM.to_owned(),
            key_id: self.key_id.to_string(),
            value: URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()),
        })
    }

    pub(super) fn verify(
        &self,
        payload: &GovernanceSignaturePayload,
        signature: &GovernanceSignature,
    ) -> Result<(), ControlPlaneError> {
        if signature.algorithm != ALGORITHM || signature.key_id.as_str() != self.key_id.as_ref() {
            return Err(ControlPlaneError::forbidden(
                "governance_signature_invalid",
                "governance signature metadata is invalid",
            ));
        }
        let signature_bytes = URL_SAFE_NO_PAD.decode(signature.value.as_bytes()).map_err(|_| {
            ControlPlaneError::forbidden(
                "governance_signature_invalid",
                "governance signature encoding is invalid",
            )
        })?;
        let encoded = serde_jcs::to_vec(payload)
            .map_err(|_| ControlPlaneError::configuration("governance signature payload cannot be encoded"))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .map_err(|_| ControlPlaneError::configuration("governance signing key is invalid"))?;
        mac.update(&encoded);
        mac.verify_slice(&signature_bytes).map_err(|_| {
            ControlPlaneError::forbidden(
                "governance_signature_invalid",
                "governance signature verification failed",
            )
        })
    }
}
