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

use std::path::Path as FileSystemPath;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
#[cfg(test)]
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;

use crate::ControlPlaneError;

const LOCAL_STORE_ENV: &str = "ROCKETMQ_SRE_OBJECT_STORE_LOCAL_PATH";
const LOCAL_URI_PREFIX: &str = "rocketmq-sre-local://evidence/";
#[cfg(test)]
const MEMORY_URI_PREFIX: &str = "rocketmq-sre-memory://evidence/";
const MAX_OBJECT_PATH_BYTES: usize = 1_024;

#[derive(Clone)]
pub(crate) struct EvidenceBlobStore {
    store: Arc<dyn ObjectStore>,
    uri_prefix: Arc<str>,
    max_inline_bytes: usize,
}

impl EvidenceBlobStore {
    pub(crate) fn from_env(dev_mode: bool) -> Result<Self, ControlPlaneError> {
        let max_inline_bytes = std::env::var("ROCKETMQ_SRE_EVIDENCE_INLINE_BYTES")
            .ok()
            .map(|value| {
                value.parse::<usize>().map_err(|error| {
                    ControlPlaneError::configuration(format!("ROCKETMQ_SRE_EVIDENCE_INLINE_BYTES is invalid: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(64 * 1024);
        if !(1_024..=1024 * 1024).contains(&max_inline_bytes) {
            return Err(ControlPlaneError::configuration(
                "ROCKETMQ_SRE_EVIDENCE_INLINE_BYTES must be between 1024 and 1048576",
            ));
        }

        let local_path = optional_env(LOCAL_STORE_ENV);
        let endpoint = optional_env("ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT");
        if local_path.is_some() && endpoint.is_some() {
            return Err(ControlPlaneError::configuration(format!(
                "{LOCAL_STORE_ENV} and ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT are mutually exclusive"
            )));
        }
        if let Some(local_path) = local_path {
            if !dev_mode {
                return Err(ControlPlaneError::configuration(format!(
                    "{LOCAL_STORE_ENV} is permitted only when ROCKETMQ_SRE_DEV_AUTH=true"
                )));
            }
            return Self::local(local_path, max_inline_bytes);
        }
        if endpoint.is_none() && dev_mode {
            return Err(ControlPlaneError::configuration(format!(
                "{LOCAL_STORE_ENV} must be configured for persistent development evidence storage"
            )));
        }
        let endpoint = endpoint
            .ok_or_else(|| ControlPlaneError::configuration("ROCKETMQ_SRE_OBJECT_STORE_ENDPOINT must be configured"))?;
        let bucket = required_env("ROCKETMQ_SRE_OBJECT_STORE_BUCKET")?;
        let access_key = required_env("ROCKETMQ_SRE_OBJECT_STORE_ACCESS_KEY")?;
        let secret_key = required_env("ROCKETMQ_SRE_OBJECT_STORE_SECRET_KEY")?;
        let region = optional_env("ROCKETMQ_SRE_OBJECT_STORE_REGION").unwrap_or_else(|| "us-east-1".to_owned());
        let endpoint_is_http = endpoint.starts_with("http://");
        let endpoint_is_https = endpoint.starts_with("https://");
        if !endpoint_is_https && (!dev_mode || !endpoint_is_http) {
            return Err(ControlPlaneError::configuration(
                "object storage endpoint must use HTTPS outside development and HTTP or HTTPS in development",
            ));
        }
        let allow_http = endpoint_is_http && dev_mode;
        let store = AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_region(region)
            .with_endpoint(endpoint)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(allow_http)
            .build()
            .map_err(|_| ControlPlaneError::configuration("S3-compatible evidence store cannot be configured"))?;
        Ok(Self {
            store: Arc::new(store),
            uri_prefix: Arc::from(format!("s3://{bucket}/")),
            max_inline_bytes,
        })
    }

    fn local(path: impl AsRef<FileSystemPath>, max_inline_bytes: usize) -> Result<Self, ControlPlaneError> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(ControlPlaneError::configuration(format!(
                "{LOCAL_STORE_ENV} must be an absolute directory"
            )));
        }
        std::fs::create_dir_all(path)
            .map_err(|_| ControlPlaneError::configuration("local evidence store directory cannot be initialized"))?;
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| ControlPlaneError::configuration("local evidence store directory cannot be inspected"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ControlPlaneError::configuration(
                "local evidence store root must be a directory and must not be a symbolic link",
            ));
        }
        let store = LocalFileSystem::new_with_prefix(path)
            .map_err(|_| ControlPlaneError::configuration("local evidence store cannot be configured"))?;
        Ok(Self {
            store: Arc::new(store),
            uri_prefix: Arc::from(LOCAL_URI_PREFIX),
            max_inline_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn in_memory(max_inline_bytes: usize) -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            uri_prefix: Arc::from(MEMORY_URI_PREFIX),
            max_inline_bytes,
        }
    }

    pub(crate) const fn max_inline_bytes(&self) -> usize {
        self.max_inline_bytes
    }

    pub(crate) async fn put(&self, path: &str, value: Vec<u8>) -> Result<String, ControlPlaneError> {
        let path = valid_path(path)?;
        self.store
            .put(&path, PutPayload::from(value))
            .await
            .map_err(|_| ControlPlaneError::ObjectStore)?;
        Ok(format!("{}{path}", self.uri_prefix))
    }

    pub(crate) async fn get(&self, uri: &str, max_bytes: usize) -> Result<Bytes, ControlPlaneError> {
        let path = uri
            .strip_prefix(self.uri_prefix.as_ref())
            .ok_or_else(|| ControlPlaneError::forbidden("unauthorized_scope", "evidence URI is outside the store"))?;
        let result = self
            .store
            .get(&valid_path(path)?)
            .await
            .map_err(|_| ControlPlaneError::ObjectStore)?;
        let max_bytes = u64::try_from(max_bytes).map_err(|_| {
            ControlPlaneError::validation("output_too_large", "evidence download limit exceeds the supported size")
        })?;
        if result.meta.size > max_bytes {
            return Err(ControlPlaneError::validation(
                "output_too_large",
                "evidence content exceeds the bounded download size",
            ));
        }
        result.bytes().await.map_err(|_| ControlPlaneError::ObjectStore)
    }
}

fn valid_path(value: &str) -> Result<ObjectPath, ControlPlaneError> {
    let is_safe = !value.is_empty()
        && value.len() <= MAX_OBJECT_PATH_BYTES
        && !value.starts_with('/')
        && value.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        });
    if !is_safe {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "evidence object path is invalid",
        ));
    }
    ObjectPath::parse(value)
        .map_err(|_| ControlPlaneError::validation("invalid_request", "evidence object path is invalid"))
}

fn optional_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn required_env(name: &'static str) -> Result<String, ControlPlaneError> {
    optional_env(name).ok_or_else(|| ControlPlaneError::configuration(format!("{name} must be configured")))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn memory_store_round_trips_bounded_content() {
        let store = EvidenceBlobStore::in_memory(1_024);
        let uri = store
            .put("evidence/tenant/cluster/item.json", b"payload".to_vec())
            .await
            .expect("put");
        let value = store.get(&uri, 128).await.expect("get");
        assert_eq!(value.as_ref(), b"payload");
    }

    #[tokio::test]
    async fn local_store_survives_store_reconstruction() {
        let directory = TestDirectory::new();
        let uri = {
            let store = EvidenceBlobStore::local(directory.path(), 1_024).expect("local store");
            store
                .put("evidence/tenant/cluster/item.json", b"durable-payload".to_vec())
                .await
                .expect("put")
        };

        assert!(uri.starts_with(LOCAL_URI_PREFIX));
        let local_path = directory.path().to_string_lossy();
        assert!(!uri.contains(local_path.as_ref()));
        let restarted = EvidenceBlobStore::local(directory.path(), 1_024).expect("restarted local store");
        let value = restarted.get(&uri, 128).await.expect("get after restart");
        assert_eq!(value.as_ref(), b"durable-payload");
    }

    #[test]
    fn object_paths_reject_traversal_and_absolute_forms() {
        for path in [
            "../outside.json",
            "evidence/../../outside.json",
            "evidence/%2E%2E/outside.json",
            "/absolute/outside.json",
            r"evidence\..\outside.json",
        ] {
            assert!(valid_path(path).is_err(), "{path} must be rejected");
        }
    }

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("rocketmq-sre-evidence-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("create isolated test directory");
            Self { path }
        }

        fn path(&self) -> &FileSystemPath {
            &self.path
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).expect("remove isolated test directory");
        }
    }
}
