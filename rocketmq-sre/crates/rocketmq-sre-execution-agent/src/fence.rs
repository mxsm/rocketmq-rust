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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::Hmac;
use hmac::Mac;
use rocketmq_sre_contracts::FenceAck;
use rocketmq_sre_contracts::ReconcileGrant;
use serde::Serialize;
use sha2::Sha256;

use crate::ExecutionAgentError;

const SIGNATURE_PREFIX: &str = "hmac-sha256:";

#[derive(Serialize)]
struct UnsignedFenceAck<'a> {
    cluster_id: rocketmq_sre_contracts::ClusterId,
    epoch: rocketmq_sre_contracts::LeaseEpoch,
    pending_nonce: &'a str,
    agent_subject: &'a str,
    acknowledged_at: chrono::DateTime<chrono::Utc>,
}

/// Process-local Agent signer. The key is never returned to Executor.
#[derive(Clone)]
pub struct FenceAckSigner {
    key: Arc<[u8]>,
    agent_subject: Arc<str>,
}

impl FenceAckSigner {
    /// Creates a signer from the Agent-only projected secret.
    ///
    /// # Errors
    ///
    /// Rejects keys shorter than 32 bytes or an empty workload subject.
    pub fn new(key: impl AsRef<[u8]>, agent_subject: impl Into<Arc<str>>) -> Result<Self, ExecutionAgentError> {
        let key = key.as_ref();
        let agent_subject = agent_subject.into();
        if key.len() < 32 || agent_subject.trim().is_empty() {
            return Err(ExecutionAgentError::Configuration);
        }
        Ok(Self {
            key: Arc::from(key),
            agent_subject,
        })
    }

    /// Signs an acknowledgement bound to the pending lease nonce and epoch.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if canonicalization or HMAC setup fails.
    pub fn sign(
        &self,
        grant: &ReconcileGrant,
        acknowledged_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<FenceAck, ExecutionAgentError> {
        let mut ack = FenceAck {
            cluster_id: grant.cluster_id,
            epoch: grant.pending_epoch,
            pending_nonce: grant.nonce.clone(),
            agent_subject: self.agent_subject.to_string(),
            acknowledged_at,
            signature: String::new(),
        };
        let payload = serde_jcs::to_vec(&UnsignedFenceAck {
            cluster_id: ack.cluster_id,
            epoch: ack.epoch,
            pending_nonce: &ack.pending_nonce,
            agent_subject: &ack.agent_subject,
            acknowledged_at: ack.acknowledged_at,
        })
        .map_err(|_| ExecutionAgentError::Configuration)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).map_err(|_| ExecutionAgentError::Configuration)?;
        mac.update(&payload);
        ack.signature = format!(
            "{SIGNATURE_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        );
        Ok(ack)
    }
}

impl Debug for FenceAckSigner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FenceAckSigner")
            .field("key", &"[REDACTED]")
            .field("agent_subject", &self.agent_subject)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use chrono::Utc;
    use rocketmq_sre_contracts::ClusterId;
    use rocketmq_sre_contracts::LeaseEpoch;
    use rocketmq_sre_contracts::LeaseId;

    use super::*;

    #[test]
    fn signature_is_bound_to_pending_nonce_without_exposing_key() {
        let signer = FenceAckSigner::new("agent-only-signing-key-at-least-32-bytes", "agent-a").expect("signer");
        let now = Utc::now();
        let grant = ReconcileGrant {
            lease_id: LeaseId::new(),
            owner: "executor-a".to_owned(),
            cluster_id: ClusterId::new(),
            pending_epoch: LeaseEpoch(2),
            audience: "rocketmq-sre-execution-agent-reconcile".to_owned(),
            issued_at: now,
            expires_at: now + TimeDelta::seconds(30),
            nonce: "pending-nonce".to_owned(),
            signature: "authority-signature".to_owned(),
        };
        let ack = signer.sign(&grant, now).expect("ack");
        assert!(ack.signature.starts_with(SIGNATURE_PREFIX));
        assert_eq!(ack.pending_nonce, grant.nonce);
        assert!(!format!("{signer:?}").contains("agent-only"));
    }
}
