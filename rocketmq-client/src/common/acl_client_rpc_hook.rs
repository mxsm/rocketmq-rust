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

use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_auth::authentication::acl_signer::cal_signature_segments;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::RPCHook;
use smallvec::SmallVec;

use crate::common::session_credentials::SessionCredentials;
use crate::common::session_credentials::ACCESS_KEY;
use crate::common::session_credentials::SECURITY_TOKEN;
use crate::common::session_credentials::SIGNATURE;

/// Client-side ACL RPC hook compatible with the Java RocketMQ client.
///
/// The hook signs the request exactly like Java's `AclClientRPCHook`: merge the
/// custom header into `extFields`, sort all extension fields by key, concatenate
/// every value except `Signature`, append the request body, and write the HMAC
/// signature back to `extFields`.
#[derive(Debug, Clone)]
pub struct AclClientRPCHook {
    session_credentials: SessionCredentials,
}

impl AclClientRPCHook {
    pub fn new(session_credentials: SessionCredentials) -> Self {
        Self { session_credentials }
    }

    pub fn session_credentials(&self) -> &SessionCredentials {
        &self.session_credentials
    }

    fn sorted_ext_fields_for_signing(request: &RemotingCommand) -> SmallVec<[(&CheetahString, &CheetahString); 16]> {
        let mut sorted_fields = SmallVec::<[(&CheetahString, &CheetahString); 16]>::new();
        if let Some(ext_fields) = request.ext_fields() {
            for (key, value) in ext_fields {
                sorted_fields.push((key, value));
            }
        }
        sorted_fields.sort_unstable_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
        sorted_fields
    }
}

impl RPCHook for AclClientRPCHook {
    fn do_before_request(
        &self,
        _remote_addr: SocketAddr,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        let access_key = self
            .session_credentials
            .access_key()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RocketMQError::illegal_argument("ACL AccessKey must not be blank"))?;
        let secret_key = self
            .session_credentials
            .secret_key()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| RocketMQError::illegal_argument("ACL SecretKey must not be blank"))?;

        request.ensure_ext_fields_initialized();
        request.add_ext_field(ACCESS_KEY, access_key.clone());
        if let Some(security_token) = self.session_credentials.security_token() {
            request.add_ext_field(SECURITY_TOKEN, security_token.clone());
        }

        request.make_custom_header_to_net();
        let signature = {
            let sorted_fields = Self::sorted_ext_fields_for_signing(request);
            let field_segments = sorted_fields
                .iter()
                .filter(|(key, _)| key.as_str() != SIGNATURE)
                .map(|(_, value)| value.as_bytes());
            let body_segments = request.body().into_iter().map(|body| body.as_ref());

            cal_signature_segments(field_segments.chain(body_segments), secret_key.as_str()).map_err(|error| {
                RocketMQError::illegal_argument(format!("Failed to calculate ACL signature: {error}"))
            })?
        };
        request.add_ext_field(SIGNATURE, signature);

        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

    use super::*;

    #[test]
    fn acl_hook_signs_sorted_ext_fields_and_body_like_java_client() {
        let hook = AclClientRPCHook::new(SessionCredentials::with_token("ak", "secret", "token"));
        let mut request = RemotingCommand::create_remoting_command(10).set_body(Bytes::from_static(b"body"));
        request.ensure_ext_fields_initialized();
        request.add_ext_field("z", "last");
        request.add_ext_field("a", "first");

        hook.do_before_request("127.0.0.1:9876".parse().unwrap(), &mut request)
            .unwrap();

        let ext_fields = request.ext_fields().unwrap();
        assert_eq!(
            ext_fields.get(&CheetahString::from_static_str(ACCESS_KEY)).unwrap(),
            "ak"
        );
        assert_eq!(
            ext_fields.get(&CheetahString::from_static_str(SECURITY_TOKEN)).unwrap(),
            "token"
        );
        assert_eq!(
            ext_fields.get(&CheetahString::from_static_str(SIGNATURE)).unwrap(),
            "wuJkFDNRMKY6aFXTDwT/vg0UC8M="
        );
    }

    #[test]
    fn acl_hook_excludes_existing_signature_when_resigning_like_java_client() {
        let hook = AclClientRPCHook::new(SessionCredentials::with_token("ak", "secret", "token"));
        let mut request = RemotingCommand::create_remoting_command(10).set_body(Bytes::from_static(b"body"));
        request.ensure_ext_fields_initialized();
        request.add_ext_field("z", "last");
        request.add_ext_field("a", "first");
        request.add_ext_field(SIGNATURE, "stale-signature");

        hook.do_before_request("127.0.0.1:9876".parse().unwrap(), &mut request)
            .unwrap();

        let ext_fields = request.ext_fields().unwrap();
        assert_eq!(
            ext_fields.get(&CheetahString::from_static_str(SIGNATURE)).unwrap(),
            "wuJkFDNRMKY6aFXTDwT/vg0UC8M="
        );
    }

    #[test]
    fn acl_hook_rejects_blank_credentials_before_signing() {
        let hook = AclClientRPCHook::new(SessionCredentials::default());
        let mut request = RemotingCommand::create_remoting_command(10);

        assert!(hook
            .do_before_request("127.0.0.1:9876".parse().unwrap(), &mut request)
            .is_err());
    }
}
