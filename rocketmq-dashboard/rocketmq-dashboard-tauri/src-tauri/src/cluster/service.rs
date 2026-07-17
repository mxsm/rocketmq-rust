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

use crate::cluster::admin::ManagedClusterAdmin;
use crate::cluster::types::ClusterBrokerCardItem;
use crate::cluster::types::ClusterBrokerConfigView;
use crate::cluster::types::ClusterBrokerStatusView;
use crate::cluster::types::ClusterError;
use crate::cluster::types::ClusterHomePageResponse;
use crate::cluster::types::ClusterOverviewSummary;
use crate::cluster::types::ClusterResult;
use crate::nameserver::NameServerRuntimeState;
use rocketmq_admin_core::client_adapter::AdminSession;
use rocketmq_admin_core::core::dashboard::DashboardAdmin;
use rocketmq_admin_core::core::dashboard::DashboardBrokerInfo;
use rocketmq_admin_core::core::dashboard::DashboardBrokerTarget;
use rocketmq_dashboard_common::ClusterBrokerConfigRequest;
use rocketmq_dashboard_common::ClusterBrokerStatusRequest;
use rocketmq_dashboard_common::ClusterHomePageRequest;
use rocketmq_dashboard_common::NameServerConfigSnapshot;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct ClusterManager {
    runtime: Arc<NameServerRuntimeState>,
    admin_session: Arc<Mutex<Option<ManagedClusterAdmin>>>,
}

impl ClusterManager {
    pub(crate) fn new(runtime: Arc<NameServerRuntimeState>) -> Self {
        Self {
            runtime,
            admin_session: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn shutdown(&self) {
        let mut session = self.admin_session.lock().await;
        if let Some(mut admin) = session.take() {
            admin.shutdown().await;
        }
    }

    pub(crate) async fn get_cluster_home_page(
        &self,
        request: ClusterHomePageRequest,
    ) -> ClusterResult<ClusterHomePageResponse> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            if request.force_refresh {
                self.reset_admin_session(&mut session_guard, "force refreshing cluster snapshot")
                    .await;
            }
            self.ensure_admin_session(&mut session_guard).await?;

            let snapshot = session_guard
                .as_ref()
                .expect("cluster admin session should be initialized before use")
                .snapshot
                .clone();
            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("cluster admin session should be initialized before use");
                self.get_cluster_home_page_with_admin(&mut session.admin, &snapshot)
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_cluster_home_page failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_cluster_home_page` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get_cluster_broker_config(
        &self,
        request: ClusterBrokerConfigRequest,
    ) -> ClusterResult<ClusterBrokerConfigView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("cluster admin session should be initialized before use");
                self.get_cluster_broker_config_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_cluster_broker_config failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_cluster_broker_config` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) async fn get_cluster_broker_status(
        &self,
        request: ClusterBrokerStatusRequest,
    ) -> ClusterResult<ClusterBrokerStatusView> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut session_guard = self.admin_session.lock().await;
            self.ensure_admin_session(&mut session_guard).await?;

            let result = {
                let session = session_guard
                    .as_mut()
                    .expect("cluster admin session should be initialized before use");
                self.get_cluster_broker_status_with_admin(&mut session.admin, request.clone())
                    .await
            };

            let should_reset = Self::should_reset_session(&result);
            if should_reset {
                self.reset_admin_session(&mut session_guard, "get_cluster_broker_status failed")
                    .await;
            }
            drop(session_guard);

            match result {
                Ok(response) => return Ok(response),
                Err(error) if should_reset && attempt < 2 => {
                    log::warn!("Retrying `get_cluster_broker_status` after reconnect: {}", error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn ensure_admin_session(&self, session_slot: &mut Option<ManagedClusterAdmin>) -> ClusterResult<()> {
        let generation = self.runtime.generation();
        let needs_reconnect = session_slot
            .as_ref()
            .is_none_or(|session| !session.matches_generation(generation));

        if needs_reconnect {
            self.reset_admin_session(session_slot, "refreshing cluster admin session")
                .await;
            let session = ManagedClusterAdmin::connect(&self.runtime).await?;
            log::info!(
                "Connected cluster admin session for namesrv `{}` at generation {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                session.generation
            );
            *session_slot = Some(session);
        }

        Ok(())
    }

    async fn reset_admin_session(&self, session_slot: &mut Option<ManagedClusterAdmin>, reason: &str) {
        if let Some(mut session) = session_slot.take() {
            log::info!(
                "Shutting down cluster admin session for namesrv `{}`: {}",
                session.snapshot.current_namesrv.as_deref().unwrap_or_default(),
                reason
            );
            session.shutdown().await;
        }
    }

    fn should_reset_session<T>(result: &ClusterResult<T>) -> bool {
        matches!(
            result,
            Err(ClusterError::Admin(error)) if error.is_retryable()
                || matches!(error, rocketmq_admin_core::core::AdminError::SessionClosed)
        )
    }

    async fn get_cluster_home_page_with_admin(
        &self,
        admin: &mut AdminSession,
        snapshot: &NameServerConfigSnapshot,
    ) -> ClusterResult<ClusterHomePageResponse> {
        let broker_list = admin.dashboard_list_brokers().await.map_err(ClusterError::Admin)?;
        let clusters = broker_list.clusters;
        let items = build_cluster_items(broker_list.items);
        let summary = build_summary(clusters.len(), &items);

        let response = ClusterHomePageResponse {
            clusters,
            items,
            summary,
            current_namesrv: snapshot.current_namesrv.clone().unwrap_or_default(),
            use_vip_channel: snapshot.use_vip_channel,
            use_tls: snapshot.use_tls,
        };

        log::info!(
            "get_cluster_home_page_with_admin response: clusters={}, items={}, namesrv={}",
            response.summary.total_clusters,
            response.summary.total_brokers,
            response.current_namesrv
        );

        Ok(response)
    }

    async fn get_cluster_broker_config_with_admin(
        &self,
        admin: &mut AdminSession,
        request: ClusterBrokerConfigRequest,
    ) -> ClusterResult<ClusterBrokerConfigView> {
        let broker_addr = request.broker_addr.trim();
        if broker_addr.is_empty() {
            return Err(ClusterError::Validation("Broker address cannot be empty.".into()));
        }

        let target = DashboardBrokerTarget {
            broker_name: String::new(),
            broker_addr: Some(broker_addr.to_string()),
        };
        let config = admin
            .dashboard_broker_config(&target)
            .await
            .map_err(ClusterError::Admin)?;

        Ok(ClusterBrokerConfigView {
            broker_addr: broker_addr.to_string(),
            entries: config.entries,
        })
    }

    async fn get_cluster_broker_status_with_admin(
        &self,
        admin: &mut AdminSession,
        request: ClusterBrokerStatusRequest,
    ) -> ClusterResult<ClusterBrokerStatusView> {
        let broker_addr = request.broker_addr.trim();
        if broker_addr.is_empty() {
            return Err(ClusterError::Validation("Broker address cannot be empty.".into()));
        }

        let target = DashboardBrokerTarget {
            broker_name: String::new(),
            broker_addr: Some(broker_addr.to_string()),
        };
        let runtime = admin
            .dashboard_broker_runtime(&target)
            .await
            .map_err(ClusterError::Admin)?;

        Ok(ClusterBrokerStatusView {
            broker_addr: broker_addr.to_string(),
            entries: runtime.entries,
        })
    }
}

fn build_cluster_items(mut brokers: Vec<DashboardBrokerInfo>) -> Vec<ClusterBrokerCardItem> {
    brokers.sort_by(|left, right| {
        (&left.cluster_name, &left.broker_name, left.broker_id, &left.address).cmp(&(
            &right.cluster_name,
            &right.broker_name,
            right.broker_id,
            &right.address,
        ))
    });

    brokers
        .into_iter()
        .map(|broker| {
            build_cluster_item(
                &broker.cluster_name,
                &broker.broker_name,
                broker.broker_id,
                &broker.address,
                broker.runtime_entries,
                broker.runtime_error,
            )
        })
        .collect()
}

fn build_cluster_item(
    cluster_name: &str,
    broker_name: &str,
    broker_id: u64,
    address: &str,
    raw_status: BTreeMap<String, String>,
    status_load_error: Option<String>,
) -> ClusterBrokerCardItem {
    ClusterBrokerCardItem {
        cluster_name: cluster_name.to_string(),
        broker_name: broker_name.to_string(),
        broker_id,
        role: if broker_id == 0 { "MASTER" } else { "SLAVE" }.to_string(),
        address: address.to_string(),
        version: raw_status.get("brokerVersionDesc").cloned().unwrap_or_default(),
        produce_tps: parse_rate_value(raw_status.get("putTps")),
        consume_tps: parse_rate_value(select_consume_tps_value(&raw_status)),
        today_received_total: parse_counter_value(raw_status.get("msgGetTotalTodayNow")),
        yesterday_produce: diff_counter(
            raw_status.get("msgPutTotalYesterdayMorning"),
            raw_status.get("msgPutTotalTodayMorning"),
        ),
        yesterday_consume: diff_counter(
            raw_status.get("msgGetTotalYesterdayMorning"),
            raw_status.get("msgGetTotalTodayMorning"),
        ),
        today_produce: diff_counter(
            raw_status.get("msgPutTotalTodayMorning"),
            raw_status.get("msgPutTotalTodayNow"),
        ),
        today_consume: diff_counter(
            raw_status.get("msgGetTotalTodayMorning"),
            raw_status.get("msgGetTotalTodayNow"),
        ),
        is_active: raw_status
            .get("brokerActive")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        status_load_error,
        raw_status,
    }
}

fn select_consume_tps_value(raw_status: &BTreeMap<String, String>) -> Option<&String> {
    match raw_status.get("getTransferedTps") {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => raw_status.get("getTransferredTps"),
    }
}

fn build_summary(total_clusters: usize, items: &[ClusterBrokerCardItem]) -> ClusterOverviewSummary {
    let total_masters = items.iter().filter(|item| item.role == "MASTER").count();
    let total_slaves = items.iter().filter(|item| item.role == "SLAVE").count();
    let active_brokers = items
        .iter()
        .filter(|item| item.status_load_error.is_none() && item.is_active)
        .count();
    let brokers_with_status_errors = items.iter().filter(|item| item.status_load_error.is_some()).count();

    ClusterOverviewSummary {
        total_clusters,
        total_brokers: items.len(),
        total_masters,
        total_slaves,
        active_brokers,
        inactive_brokers: items.len().saturating_sub(active_brokers + brokers_with_status_errors),
        brokers_with_status_errors,
    }
}

fn parse_rate_value(value: Option<&String>) -> f64 {
    value
        .and_then(|raw| raw.split_whitespace().next())
        .and_then(|token| token.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn parse_counter_value(value: Option<&String>) -> i64 {
    value.and_then(|raw| raw.parse::<i64>().ok()).unwrap_or(0)
}

fn diff_counter(from: Option<&String>, to: Option<&String>) -> i64 {
    parse_counter_value(to).saturating_sub(parse_counter_value(from))
}

#[cfg(test)]
mod tests {
    use super::build_cluster_item;
    use super::build_summary;
    use super::diff_counter;
    use super::parse_rate_value;
    use super::select_consume_tps_value;
    use crate::cluster::types::ClusterBrokerCardItem;
    use std::collections::BTreeMap;

    fn broker(role: &str, is_active: bool, status_load_error: Option<&str>) -> ClusterBrokerCardItem {
        ClusterBrokerCardItem {
            cluster_name: "DefaultCluster".into(),
            broker_name: "broker-a".into(),
            broker_id: if role == "MASTER" { 0 } else { 1 },
            role: role.into(),
            address: "127.0.0.1:10911".into(),
            version: "V5_4_0".into(),
            produce_tps: 0.0,
            consume_tps: 0.0,
            today_received_total: 0,
            yesterday_produce: 0,
            yesterday_consume: 0,
            today_produce: 0,
            today_consume: 0,
            is_active,
            status_load_error: status_load_error.map(str::to_string),
            raw_status: BTreeMap::new(),
        }
    }

    #[test]
    fn parse_rate_value_uses_first_token() {
        assert_eq!(parse_rate_value(Some(&"12.50 1min".to_string())), 12.50);
        assert_eq!(parse_rate_value(Some(&"".to_string())), 0.0);
        assert_eq!(parse_rate_value(None), 0.0);
    }

    #[test]
    fn diff_counter_computes_java_style_message_delta() {
        assert_eq!(diff_counter(Some(&"100".to_string()), Some(&"180".to_string())), 80);
        assert_eq!(diff_counter(None, Some(&"25".to_string())), 25);
    }

    #[test]
    fn build_summary_tracks_active_inactive_and_error_brokers() {
        let items = vec![
            broker("MASTER", true, None),
            broker("SLAVE", false, None),
            broker("SLAVE", false, Some("timeout")),
        ];

        let summary = build_summary(1, &items);

        assert_eq!(summary.total_clusters, 1);
        assert_eq!(summary.total_brokers, 3);
        assert_eq!(summary.total_masters, 1);
        assert_eq!(summary.total_slaves, 2);
        assert_eq!(summary.active_brokers, 1);
        assert_eq!(summary.inactive_brokers, 1);
        assert_eq!(summary.brokers_with_status_errors, 1);
    }

    #[test]
    fn build_cluster_item_maps_runtime_stats_using_java_semantics() {
        let raw_status = BTreeMap::from([
            ("brokerVersionDesc".to_string(), "V5_4_0".to_string()),
            ("putTps".to_string(), "12.50 1min".to_string()),
            ("getTransferredTps".to_string(), "8.25 1min".to_string()),
            ("msgGetTotalTodayNow".to_string(), "350".to_string()),
            ("msgPutTotalYesterdayMorning".to_string(), "100".to_string()),
            ("msgPutTotalTodayMorning".to_string(), "180".to_string()),
            ("msgPutTotalTodayNow".to_string(), "260".to_string()),
            ("msgGetTotalYesterdayMorning".to_string(), "90".to_string()),
            ("msgGetTotalTodayMorning".to_string(), "150".to_string()),
            ("msgGetTotalTodayNow".to_string(), "350".to_string()),
            ("brokerActive".to_string(), "true".to_string()),
        ]);

        let item = build_cluster_item("DefaultCluster", "broker-a", 0, "127.0.0.1:10911", raw_status, None);

        assert_eq!(item.role, "MASTER");
        assert_eq!(item.version, "V5_4_0");
        assert_eq!(item.produce_tps, 12.50);
        assert_eq!(item.consume_tps, 8.25);
        assert_eq!(item.today_received_total, 350);
        assert_eq!(item.yesterday_produce, 80);
        assert_eq!(item.yesterday_consume, 60);
        assert_eq!(item.today_produce, 80);
        assert_eq!(item.today_consume, 200);
        assert!(item.is_active);
    }

    #[test]
    fn select_consume_tps_value_prefers_legacy_key_when_present() {
        let raw_status = BTreeMap::from([
            ("getTransferedTps".to_string(), "7.50 1min".to_string()),
            ("getTransferredTps".to_string(), "8.25 1min".to_string()),
        ]);

        let selected = select_consume_tps_value(&raw_status);

        assert_eq!(selected.map(String::as_str), Some("7.50 1min"));
    }

    #[test]
    fn select_consume_tps_value_falls_back_when_legacy_key_is_blank() {
        let raw_status = BTreeMap::from([
            ("getTransferedTps".to_string(), "   ".to_string()),
            ("getTransferredTps".to_string(), "8.25 1min".to_string()),
        ]);

        let selected = select_consume_tps_value(&raw_status);

        assert_eq!(selected.map(String::as_str), Some("8.25 1min"));
    }
}
