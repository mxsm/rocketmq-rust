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
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use object_store::CredentialProvider;
use object_store::aws::AwsCredential;
use object_store::aws::AwsCredentialProvider;
use rocketmq_sre_model_gateway::ExternalSecretManagerProvider;
use rocketmq_sre_model_gateway::SecretProvider;
use rocketmq_sre_model_gateway::SecretReference;
use rocketmq_sre_model_gateway::SecretReferenceKind;
use rocketmq_sre_model_gateway::VaultAgentFileSecretClient;

use super::optional_env;
use super::required_env;
use crate::ControlPlaneError;

const VAULT_AGENT_ROOT_ENV: &str = "ROCKETMQ_SRE_OBJECT_STORE_VAULT_AGENT_ROOT";
const SECRET_NAMESPACE_ENV: &str = "ROCKETMQ_SRE_OBJECT_STORE_SECRET_NAMESPACE";
const ACCESS_KEY_REF_ENV: &str = "ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY_REF";
const SECRET_KEY_REF_ENV: &str = "ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY_REF";
const SESSION_TOKEN_REF_ENV: &str = "ROCKETMQ_SRE_OBJECT_STORE_SESSION_TOKEN_REF";

struct S3SecretCredentialProvider {
    secrets: Arc<dyn SecretProvider>,
    access_key: SecretReference,
    secret_key: SecretReference,
    session_token: Option<SecretReference>,
}

impl S3SecretCredentialProvider {
    fn new(
        secrets: Arc<dyn SecretProvider>,
        access_key: SecretReference,
        secret_key: SecretReference,
        session_token: Option<SecretReference>,
    ) -> Self {
        Self {
            secrets,
            access_key,
            secret_key,
            session_token,
        }
    }

    fn resolve(&self) -> Result<Arc<AwsCredential>, object_store::Error> {
        let key_id = self
            .secrets
            .resolve(&self.access_key)
            .map_err(|_| s3_credential_error())?;
        let secret_key = self
            .secrets
            .resolve(&self.secret_key)
            .map_err(|_| s3_credential_error())?;
        let token = self
            .session_token
            .as_ref()
            .map(|reference| self.secrets.resolve(reference))
            .transpose()
            .map_err(|_| s3_credential_error())?;
        Ok(Arc::new(AwsCredential {
            key_id: key_id.expose_to_transport().to_owned(),
            secret_key: secret_key.expose_to_transport().to_owned(),
            token: token.map(|value| value.expose_to_transport().to_owned()),
        }))
    }
}

impl Debug for S3SecretCredentialProvider {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3SecretCredentialProvider")
            .field("secrets", &"[SECRET PROVIDER]")
            .field("access_key", &"[REFERENCE REDACTED]")
            .field("secret_key", &"[REFERENCE REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REFERENCE REDACTED]"),
            )
            .finish()
    }
}

impl CredentialProvider for S3SecretCredentialProvider {
    type Credential = AwsCredential;

    fn get_credential<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<Self::Credential>, object_store::Error>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.resolve() })
    }
}

pub(super) fn s3_credentials(dev_mode: bool) -> Result<AwsCredentialProvider, ControlPlaneError> {
    if let Some(root) = optional_env(VAULT_AGENT_ROOT_ENV) {
        let namespace = required_env(SECRET_NAMESPACE_ENV)?;
        let access_key = secret_reference(ACCESS_KEY_REF_ENV)?;
        let secret_key = secret_reference(SECRET_KEY_REF_ENV)?;
        let session_token = optional_env(SESSION_TOKEN_REF_ENV)
            .map(|value| {
                SecretReference::parse(&value).map_err(|_| {
                    ControlPlaneError::configuration(format!("{SESSION_TOKEN_REF_ENV} is not a valid secret reference"))
                })
            })
            .transpose()?;
        let cache_seconds = optional_env("ROCKETMQ_SRE_OBJECT_STORE_SECRET_CACHE_SECONDS")
            .map(|value| {
                value.parse::<u64>().map_err(|error| {
                    ControlPlaneError::configuration(format!(
                        "ROCKETMQ_SRE_OBJECT_STORE_SECRET_CACHE_SECONDS is invalid: {error}"
                    ))
                })
            })
            .transpose()?
            .unwrap_or(30);
        if cache_seconds > 300 {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_OBJECT_STORE_SECRET_CACHE_SECONDS must not exceed 300",
            ));
        }
        let client = VaultAgentFileSecretClient::new(root)
            .map_err(|_| ControlPlaneError::configuration("Vault Agent object-store secret root is unavailable"))?;
        let secrets: Arc<dyn SecretProvider> = Arc::new(ExternalSecretManagerProvider::new(
            Arc::new(client),
            namespace,
            Duration::from_secs(cache_seconds),
        ));
        return Ok(Arc::new(S3SecretCredentialProvider::new(
            secrets,
            access_key,
            secret_key,
            session_token,
        )));
    }

    if !dev_mode {
        return Err(ControlPlaneError::configuration(format!(
            "production object storage requires {VAULT_AGENT_ROOT_ENV} and external secret references"
        )));
    }
    let access_key = required_env("ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY")?;
    let secret_key = required_env("ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY")?;
    Ok(Arc::new(object_store::StaticCredentialProvider::new(AwsCredential {
        key_id: access_key,
        secret_key,
        token: None,
    })))
}

fn secret_reference(name: &'static str) -> Result<SecretReference, ControlPlaneError> {
    let value = required_env(name)?;
    let reference = SecretReference::parse(&value)
        .map_err(|_| ControlPlaneError::configuration(format!("{name} is not a valid secret reference")))?;
    if reference.kind() != SecretReferenceKind::External {
        return Err(ControlPlaneError::configuration(format!(
            "{name} must use the external secret reference scheme"
        )));
    }
    Ok(reference)
}

fn s3_credential_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "S3",
        source: Box::new(io::Error::other("external object-store credential is unavailable")),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn vault_agent_credentials_rotate_without_rebuilding_the_object_store() {
        let directory = TestDirectory::new();
        let namespace = directory.path().join("object-store");
        std::fs::create_dir(&namespace).expect("create secret namespace");
        let access_key_path = namespace.join("access-key");
        let secret_key_path = namespace.join("secret-key");
        std::fs::write(&access_key_path, "access-v1\n").expect("write access key");
        std::fs::write(&secret_key_path, "secret-v1\n").expect("write secret key");
        let client = VaultAgentFileSecretClient::new(directory.path()).expect("Vault Agent client");
        let secrets: Arc<dyn SecretProvider> = Arc::new(ExternalSecretManagerProvider::new(
            Arc::new(client),
            "object-store",
            Duration::ZERO,
        ));
        let provider = S3SecretCredentialProvider::new(
            secrets,
            SecretReference::external("object-store/access-key").expect("access reference"),
            SecretReference::external("object-store/secret-key").expect("secret reference"),
            None,
        );

        let first = provider.get_credential().await.expect("first credentials");
        assert_eq!(first.key_id, "access-v1");
        assert_eq!(first.secret_key, "secret-v1");

        std::fs::write(&access_key_path, "access-v2\n").expect("rotate access key");
        std::fs::write(&secret_key_path, "secret-v2\n").expect("rotate secret key");
        let rotated = provider.get_credential().await.expect("rotated credentials");
        assert_eq!(rotated.key_id, "access-v2");
        assert_eq!(rotated.secret_key, "secret-v2");
        let debug = format!("{provider:?}");
        assert!(!debug.contains("access-v2"));
        assert!(!debug.contains("secret-v2"));
    }

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("rocketmq-sre-object-credentials-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("create isolated test directory");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).expect("remove isolated test directory");
        }
    }
}
