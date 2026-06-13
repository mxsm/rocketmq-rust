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

//! Java-compatible ACL client RPC hook.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;

use crate::authentication::builder::DefaultAuthenticationContextBuilder;
use crate::authentication::chain::acl_signer;
use crate::authentication::chain::acl_signer::SignatureAlgorithm;
use crate::config::AuthConfig;

const ACCESS_KEY: &str = "AccessKey";
const SECURITY_TOKEN: &str = "SecurityToken";
const SIGNATURE: &str = "Signature";

#[derive(Clone, PartialEq, Eq)]
pub struct AclClientRpcHook {
    access_key: CheetahString,
    secret_key: CheetahString,
    security_token: Option<CheetahString>,
    signature_algorithm: SignatureAlgorithm,
}

impl fmt::Debug for AclClientRpcHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AclClientRpcHook")
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("security_token", &self.security_token.as_ref().map(|_| "<redacted>"))
            .field("signature_algorithm", &self.signature_algorithm)
            .finish()
    }
}

impl AclClientRpcHook {
    pub fn new(
        access_key: impl Into<CheetahString>,
        secret_key: impl Into<CheetahString>,
        security_token: Option<impl Into<CheetahString>>,
        signature_algorithm: SignatureAlgorithm,
    ) -> Option<Self> {
        let access_key = access_key.into();
        let secret_key = secret_key.into();
        if access_key.trim().is_empty() || secret_key.trim().is_empty() {
            return None;
        }

        Some(Self {
            access_key: CheetahString::from(access_key.trim()),
            secret_key: CheetahString::from(secret_key.trim()),
            security_token: security_token.and_then(|token| non_blank(token.into())),
            signature_algorithm,
        })
    }

    pub fn from_auth_config(config: &AuthConfig) -> RocketMQResult<Option<Self>> {
        Self::from_credentials_json(
            config.inner_client_authentication_credentials.as_str(),
            config.signature_algorithm,
        )
    }

    pub fn from_credentials_json(
        credentials_json: &str,
        signature_algorithm: SignatureAlgorithm,
    ) -> RocketMQResult<Option<Self>> {
        if credentials_json.trim().is_empty() {
            return Ok(None);
        }

        let credentials = serde_json::from_str::<ClientCredentials>(credentials_json).map_err(|error| {
            RocketMQError::auth_config_invalid(
                "innerClientAuthenticationCredentials",
                format!("invalid JSON credentials: {error}"),
            )
        })?;
        Ok(Self::from_credentials(credentials, signature_algorithm))
    }

    pub fn from_tools_file(
        path: impl AsRef<Path>,
        signature_algorithm: SignatureAlgorithm,
    ) -> RocketMQResult<Option<Self>> {
        let path = path.as_ref();
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(RocketMQError::auth_config_invalid(
                    "toolsAclFile",
                    format!("failed to read {}: {error}", path.display()),
                ));
            }
        };
        let credentials = serde_yaml::from_str::<ClientCredentials>(&content).map_err(|error| {
            RocketMQError::auth_config_invalid(
                "toolsAclFile",
                format!("invalid YAML credentials in {}: {error}", path.display()),
            )
        })?;
        Ok(Self::from_credentials(credentials, signature_algorithm))
    }

    pub fn into_rpc_hook(self) -> Arc<dyn RPCHook> {
        Arc::new(self)
    }

    pub fn access_key(&self) -> &CheetahString {
        &self.access_key
    }

    pub fn secret_key(&self) -> &CheetahString {
        &self.secret_key
    }

    pub fn security_token(&self) -> Option<&CheetahString> {
        self.security_token.as_ref()
    }

    fn from_credentials(credentials: ClientCredentials, signature_algorithm: SignatureAlgorithm) -> Option<Self> {
        Self::new(
            credentials.access_key?,
            credentials.secret_key?,
            credentials.security_token,
            signature_algorithm,
        )
    }
}

impl RPCHook for AclClientRpcHook {
    fn do_before_request(&self, _remote_addr: SocketAddr, request: &mut RemotingCommand) -> RocketMQResult<()> {
        request.ensure_ext_fields_initialized();
        request.add_ext_field(ACCESS_KEY, self.access_key.clone());
        if let Some(security_token) = &self.security_token {
            request.add_ext_field(SECURITY_TOKEN, security_token.clone());
        }
        request.make_custom_header_to_net();

        let sorted_fields = request
            .ext_fields()
            .into_iter()
            .flat_map(|fields| fields.iter())
            .filter(|(key, _)| key.as_str() != SIGNATURE)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let content = DefaultAuthenticationContextBuilder::combine_request_content(request, &sorted_fields);
        let signature = acl_signer::cal_signature_with_algorithm(
            content.as_slice(),
            self.secret_key.as_str(),
            self.signature_algorithm,
        )?;
        request.add_ext_field(SIGNATURE, signature);
        Ok(())
    }

    fn do_after_response(
        &self,
        _remote_addr: SocketAddr,
        _request: &RemotingCommand,
        _response: &mut RemotingCommand,
    ) -> RocketMQResult<()> {
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientCredentials {
    #[serde(alias = "AccessKey", alias = "access_key")]
    access_key: Option<String>,
    #[serde(alias = "SecretKey", alias = "secret_key")]
    secret_key: Option<String>,
    #[serde(alias = "SecurityToken", alias = "security_token")]
    security_token: Option<String>,
}

fn non_blank(value: CheetahString) -> Option<CheetahString> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(CheetahString::from(value))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;

    use super::*;

    fn remote_addr() -> SocketAddr {
        "127.0.0.1:9876".parse().unwrap()
    }

    #[test]
    fn from_auth_config_skips_blank_or_incomplete_credentials() {
        assert!(AclClientRpcHook::from_auth_config(&AuthConfig::default())
            .unwrap()
            .is_none());
        assert!(
            AclClientRpcHook::from_credentials_json(r#"{"accessKey":"ak"}"#, SignatureAlgorithm::HmacSha1)
                .unwrap()
                .is_none()
        );
        assert!(AclClientRpcHook::from_credentials_json(
            r#"{"accessKey":" ","secretKey":"sk"}"#,
            SignatureAlgorithm::HmacSha1
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn from_auth_config_parses_inner_client_credentials() {
        let hook = AclClientRpcHook::from_credentials_json(
            r#"{"accessKey":" inner ","secretKey":" secret ","securityToken":" token "}"#,
            SignatureAlgorithm::HmacSha1,
        )
        .unwrap()
        .unwrap();

        assert_eq!(hook.access_key().as_str(), "inner");
        assert_eq!(hook.secret_key().as_str(), "secret");
        assert_eq!(hook.security_token().map(CheetahString::as_str), Some("token"));
    }

    #[test]
    fn from_tools_file_reads_java_tools_credentials() {
        let temp = tempfile::tempdir().unwrap();
        let tools_file = temp.path().join("tools.yml");
        fs::write(&tools_file, "accessKey: tools-ak\nsecretKey: tools-sk\n").unwrap();

        let hook = AclClientRpcHook::from_tools_file(&tools_file, SignatureAlgorithm::HmacSha1)
            .unwrap()
            .unwrap();

        assert_eq!(hook.access_key().as_str(), "tools-ak");
        assert_eq!(hook.secret_key().as_str(), "tools-sk");
    }

    #[test]
    fn do_before_request_adds_java_acl_fields_and_signature() {
        let hook = AclClientRpcHook::new("alice", "secret", Some("token"), SignatureAlgorithm::HmacSha1).unwrap();
        let mut request =
            RemotingCommand::create_request_command(10, EmptyHeader {}).set_ext_fields(HashMap::from([(
                CheetahString::from("topic"),
                CheetahString::from("topic-a"),
            )]));

        hook.do_before_request(remote_addr(), &mut request).unwrap();

        let fields = request.ext_fields().unwrap();
        let expected_signature = acl_signer::cal_signature(b"alicetokentopic-a", "secret").unwrap();
        assert_eq!(fields.get("AccessKey").map(CheetahString::as_str), Some("alice"));
        assert_eq!(fields.get("SecurityToken").map(CheetahString::as_str), Some("token"));
        assert_eq!(
            fields.get("Signature").map(CheetahString::as_str),
            Some(expected_signature.as_str())
        );
    }

    #[test]
    fn do_before_request_excludes_existing_signature_when_resigning_like_java() {
        let hook = AclClientRpcHook::new("alice", "secret", Some("token"), SignatureAlgorithm::HmacSha1).unwrap();
        let mut request = RemotingCommand::create_request_command(10, EmptyHeader {}).set_ext_fields(HashMap::from([
            (CheetahString::from("topic"), CheetahString::from("topic-a")),
            (CheetahString::from(SIGNATURE), CheetahString::from("stale-signature")),
        ]));

        hook.do_before_request(remote_addr(), &mut request).unwrap();

        let fields = request.ext_fields().unwrap();
        let expected_signature = acl_signer::cal_signature(b"alicetokentopic-a", "secret").unwrap();
        assert_eq!(
            fields.get(SIGNATURE).map(CheetahString::as_str),
            Some(expected_signature.as_str())
        );
    }
}
