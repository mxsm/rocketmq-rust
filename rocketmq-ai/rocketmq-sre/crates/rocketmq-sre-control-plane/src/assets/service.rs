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

use rocketmq_sre_contracts::ClusterId;
use serde::Serialize;
use url::Url;
use uuid::Uuid;

use super::AssetKey;
use super::AssetListQuery;
use super::AssetPage;
use super::IngestInventoryRequest;
use super::InventorySnapshot;
use super::TopologyDiff;
use super::enforce_scope;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const MAX_DASHBOARD_ORIGINS: usize = 16;

/// Safe link returned to the UI. The control plane never fetches this URL.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct DashboardDeepLink {
    pub url: String,
}

/// Explicit dashboard allowlist. Request data can only become encoded query
/// values on a preconfigured HTTP(S) origin.
#[derive(Clone, Debug, Default)]
pub(crate) struct DashboardDeepLinkPolicy {
    bases: Vec<Url>,
}

impl DashboardDeepLinkPolicy {
    pub(crate) fn disabled() -> Self {
        Self::default()
    }

    pub(crate) fn from_allowlist<I, S>(origins: I) -> Result<Self, ControlPlaneError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut bases = Vec::new();
        for origin in origins {
            if bases.len() == MAX_DASHBOARD_ORIGINS {
                return Err(ControlPlaneError::configuration(
                    "dashboard deep-link allowlist exceeds the supported bound",
                ));
            }
            let mut url = Url::parse(origin.as_ref())
                .map_err(|_| ControlPlaneError::configuration("dashboard deep-link origin is invalid"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(ControlPlaneError::configuration(
                    "dashboard deep-link origins must be credential-free HTTP(S) URLs without query or fragment",
                ));
            }
            if !url.path().ends_with('/') {
                let path = format!("{}/", url.path());
                url.set_path(&path);
            }
            if !bases.iter().any(|existing| existing == &url) {
                bases.push(url);
            }
        }
        Ok(Self { bases })
    }

    fn link(&self, cluster_id: ClusterId, key: &AssetKey) -> Result<Option<DashboardDeepLink>, ControlPlaneError> {
        let Some(base) = self.bases.first() else {
            return Ok(None);
        };
        let mut url = base.clone();
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                ControlPlaneError::configuration("dashboard deep-link origin cannot contain path segments")
            })?;
            segments.pop_if_empty();
            segments.push("sre");
            segments.push(key.kind.dashboard_segment());
        }
        url.query_pairs_mut()
            .clear()
            .append_pair("cluster_id", &cluster_id.to_string())
            .append_pair("external_key", &key.external_key);
        Ok(Some(DashboardDeepLink { url: url.to_string() }))
    }
}

/// Read-only Asset/Topology application service.
#[derive(Clone, Debug)]
pub(crate) struct AssetTopologyService {
    repository: PostgresRepository,
    deep_links: DashboardDeepLinkPolicy,
}

impl AssetTopologyService {
    pub(crate) fn new(repository: PostgresRepository, deep_links: DashboardDeepLinkPolicy) -> Self {
        Self { repository, deep_links }
    }

    pub(crate) async fn ingest(
        &self,
        auth: &AuthContext,
        request: &IngestInventoryRequest,
    ) -> Result<(InventorySnapshot, TopologyDiff), ControlPlaneError> {
        let (snapshot, diff) = self.repository.persist_inventory_snapshot(auth, request).await?;
        self.repository.link_topology_diff(auth, &diff).await?;
        Ok((snapshot, diff))
    }

    pub(crate) async fn snapshot(
        &self,
        auth: &AuthContext,
        snapshot_id: Uuid,
    ) -> Result<InventorySnapshot, ControlPlaneError> {
        self.repository.inventory_snapshot(auth, snapshot_id).await
    }

    pub(crate) async fn latest(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<Option<InventorySnapshot>, ControlPlaneError> {
        self.repository.latest_inventory_snapshot(auth, cluster_id).await
    }

    pub(crate) async fn assets(
        &self,
        auth: &AuthContext,
        query: &AssetListQuery,
    ) -> Result<AssetPage, ControlPlaneError> {
        self.repository.list_latest_assets(auth, query).await
    }

    pub(crate) async fn latest_diff(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<Option<TopologyDiff>, ControlPlaneError> {
        self.repository.latest_topology_diff(auth, cluster_id).await
    }

    pub(crate) fn dashboard_link(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        key: &AssetKey,
    ) -> Result<Option<DashboardDeepLink>, ControlPlaneError> {
        enforce_scope(auth, auth.tenant_id, cluster_id)?;
        self.deep_links.link(cluster_id, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::AssetKind;

    #[test]
    fn dashboard_link_stays_on_allowlisted_origin_and_encodes_request_data() {
        let policy = DashboardDeepLinkPolicy::from_allowlist(["https://dashboard.example.test/base"]).expect("policy");
        let cluster = ClusterId::new();
        let key = AssetKey::new(AssetKind::Topic, "../orders?redirect=https://evil.test").expect("key");
        let link = policy.link(cluster, &key).expect("link").expect("enabled");
        let url = Url::parse(&link.url).expect("valid URL");

        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("dashboard.example.test"));
        assert_eq!(url.path(), "/base/sre/topics");
        assert_eq!(
            url.query_pairs()
                .find(|(name, _)| name == "external_key")
                .map(|(_, value)| value.into_owned()),
            Some("../orders?redirect=https://evil.test".to_owned())
        );
    }

    #[test]
    fn dashboard_allowlist_rejects_credentials_and_non_http_schemes() {
        assert!(DashboardDeepLinkPolicy::from_allowlist(["javascript:alert(1)"]).is_err());
        assert!(DashboardDeepLinkPolicy::from_allowlist(["https://user:secret@example.test"]).is_err());
    }
}
