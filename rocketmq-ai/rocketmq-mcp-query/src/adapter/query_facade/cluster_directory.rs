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

//! The names this server is configured with: logical clusters and their Proxy and Controller aliases.

use super::QueryFacade;
use crate::config::ClusterConfig;
use crate::tools::cluster_tools::ClusterEntry;

impl<F> QueryFacade<F> {
    /// Returns every configured logical cluster, in configuration order.
    ///
    /// The entries are read from configuration and carry names only. Who may see which entry is
    /// the caller's decision; nothing here is filtered.
    pub(crate) fn configured_clusters(&self) -> Vec<ClusterEntry> {
        cluster_entries(&self.config.clusters)
    }
}

pub(crate) fn cluster_entries(clusters: &[ClusterConfig]) -> Vec<ClusterEntry> {
    let default = crate::config::default_cluster(clusters).map(|cluster| cluster.name.as_str());
    clusters
        .iter()
        .map(|cluster| ClusterEntry {
            name: cluster.name.clone(),
            default: default == Some(cluster.name.as_str()),
            proxy_names: cluster.proxies.iter().map(|proxy| proxy.name.clone()).collect(),
            controller_names: cluster
                .controllers
                .iter()
                .map(|controller| controller.name.clone())
                .collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ControllerAlias;
    use crate::config::ProxyAlias;

    fn cluster(name: &str, default: Option<bool>) -> ClusterConfig {
        ClusterConfig {
            name: name.to_string(),
            namesrv_addr: "10.20.30.40:9876".to_string(),
            default,
            rocketmq_cluster_name: Some(format!("physical-{name}")),
            tenant: None,
            credentials: None,
            proxies: vec![ProxyAlias {
                name: format!("{name}-proxy"),
                endpoint: "10.20.30.41:8081".to_string(),
            }],
            controllers: vec![
                ControllerAlias {
                    name: format!("{name}-controller-a"),
                    endpoint: "10.20.30.42:9878".to_string(),
                },
                ControllerAlias {
                    name: format!("{name}-controller-b"),
                    endpoint: "10.20.30.43:9878".to_string(),
                },
            ],
        }
    }

    #[test]
    fn entries_carry_names_and_no_configured_address_or_physical_name() {
        let entries = cluster_entries(&[cluster("alpha", None), cluster("beta", Some(true))]);
        assert_eq!(
            entries,
            [
                ClusterEntry {
                    name: "alpha".to_string(),
                    default: false,
                    proxy_names: vec!["alpha-proxy".to_string()],
                    controller_names: vec!["alpha-controller-a".to_string(), "alpha-controller-b".to_string()],
                },
                ClusterEntry {
                    name: "beta".to_string(),
                    default: true,
                    proxy_names: vec!["beta-proxy".to_string()],
                    controller_names: vec!["beta-controller-a".to_string(), "beta-controller-b".to_string()],
                },
            ]
        );
        let published = serde_json::to_string(&entries).unwrap();
        for hidden in ["10.20.30", "9876", "8081", "9878", "physical-"] {
            assert!(!published.contains(hidden), "cluster entries expose {hidden}");
        }
    }

    #[test]
    fn the_only_configured_cluster_is_the_default_and_several_need_a_mark() {
        assert!(cluster_entries(&[cluster("only", None)])[0].default);
        let unmarked = cluster_entries(&[cluster("alpha", None), cluster("beta", None)]);
        assert!(unmarked.iter().all(|entry| !entry.default));
    }
}
