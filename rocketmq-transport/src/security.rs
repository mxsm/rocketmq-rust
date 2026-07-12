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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use cheetah_string::CheetahString;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_security_api::evaluate_request;
use rocketmq_security_api::Action;
use rocketmq_security_api::Decision;
use rocketmq_security_api::OutboundSigner;
use rocketmq_security_api::PeerInfo;
use rocketmq_security_api::Principal;
use rocketmq_security_api::RequestContext;
use rocketmq_security_api::RequestPolicy;
use rocketmq_security_api::Resource;
use rocketmq_security_api::SecurityRequestView;
use rocketmq_security_api::SigningError;

fn empty_fields() -> &'static HashMap<CheetahString, CheetahString> {
    static EMPTY: OnceLock<HashMap<CheetahString, CheetahString>> = OnceLock::new();
    EMPTY.get_or_init(HashMap::new)
}

/// Borrows security-relevant fields without copying the protocol command or body.
pub fn request_view<'a>(command: &'a RemotingCommand, peer: Option<&'a PeerInfo>) -> SecurityRequestView<'a> {
    let fields: &'a HashMap<CheetahString, CheetahString> = match command.ext_fields() {
        Some(fields) => fields,
        None => empty_fields(),
    };
    SecurityRequestView::new(
        command.code(),
        command.version(),
        fields,
        command.body().map(bytes::Bytes::as_ref),
        peer,
    )
}

/// Injected transport ports; provider implementations remain in composition crates.
pub struct TransportSecurity {
    policy: Option<Arc<dyn RequestPolicy>>,
    signer: Option<Arc<dyn OutboundSigner>>,
}

impl TransportSecurity {
    pub fn new(policy: Option<Arc<dyn RequestPolicy>>, signer: Option<Arc<dyn OutboundSigner>>) -> Self {
        Self { policy, signer }
    }

    pub fn authorize(
        &self,
        command: &RemotingCommand,
        peer: Option<&PeerInfo>,
        principal: Option<&Principal>,
        resource: Resource,
        action: Action,
    ) -> Decision {
        let Some(policy) = &self.policy else {
            return Decision::Allow;
        };
        let context = RequestContext::new(request_view(command, peer), principal, resource, action);
        evaluate_request(policy.as_ref(), &context)
    }

    pub fn sign(&self, command: &mut RemotingCommand, peer: Option<&PeerInfo>) -> Result<(), SigningError> {
        let Some(signer) = &self.signer else {
            return Ok(());
        };
        let signature = signer.sign(request_view(command, peer))?;
        command.ensure_ext_fields_initialized();
        for (key, value) in signature.fields() {
            command.add_ext_field(key.clone(), value.expose_secret().clone());
        }
        Ok(())
    }
}
