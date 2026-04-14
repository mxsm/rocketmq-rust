//! Stats admin service models and operations.

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_client_rust::admin::mq_admin_ext_async::MQAdminExt;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::DLQ_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::mix_all::RETRY_GROUP_TOPIC_PREFIX;
use rocketmq_common::common::stats::Stats;
use rocketmq_remoting::protocol::subscription::broker_stats_data::BrokerStatsData;
use rocketmq_remoting::runtime::RPCHook;
use serde::Deserialize;
use serde::Serialize;

use crate::admin::default_mq_admin_ext::DefaultMQAdminExt;
use crate::core::admin::AdminBuilder;
use crate::core::RocketMQResult;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsAllQueryRequest {
    active_topic: bool,
    topic: Option<CheetahString>,
    namesrv_addr: Option<String>,
}

impl StatsAllQueryRequest {
    pub fn new(active_topic: bool, topic: Option<String>) -> Self {
        Self {
            active_topic,
            topic: trim_optional_string(topic).map(CheetahString::from),
            namesrv_addr: None,
        }
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = trim_optional_string(namesrv_addr);
        self
    }

    pub fn active_topic(&self) -> bool {
        self.active_topic
    }

    pub fn topic(&self) -> Option<&CheetahString> {
        self.topic.as_ref()
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match self.namesrv_addr() {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatsAllRow {
    pub topic: CheetahString,
    pub consumer_group: Option<CheetahString>,
    pub accumulation: i64,
    pub in_tps: f64,
    pub out_tps: Option<f64>,
    pub in_msg_count_24h: u64,
    pub out_msg_count_24h: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsAllTopicFailure {
    pub topic: CheetahString,
    pub error: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StatsAllQueryResult {
    pub rows: Vec<StatsAllRow>,
    pub failures: Vec<StatsAllTopicFailure>,
}

pub struct StatsService;

impl StatsService {
    pub async fn query_stats_all_by_request_with_rpc_hook(
        request: StatsAllQueryRequest,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> RocketMQResult<StatsAllQueryResult> {
        let mut admin = admin_builder_with_rpc_hook(request.admin_builder(), rpc_hook)
            .build_and_start()
            .await?;
        let result = Self::query_stats_all_with_admin(&admin, &request).await;
        admin.shutdown().await;
        result
    }

    pub async fn query_stats_all_with_admin(
        admin: &DefaultMQAdminExt,
        request: &StatsAllQueryRequest,
    ) -> RocketMQResult<StatsAllQueryResult> {
        let topic_list = admin.fetch_all_topic_list().await?;
        let mut result = StatsAllQueryResult::default();

        for topic in &topic_list.topic_list {
            if should_skip_topic(topic.as_str(), request.topic()) {
                continue;
            }

            match collect_topic_detail(admin, topic, request.active_topic()).await {
                Ok(mut rows) => result.rows.append(&mut rows),
                Err(error) => result.failures.push(StatsAllTopicFailure {
                    topic: topic.clone(),
                    error: error.to_string(),
                }),
            }
        }

        Ok(result)
    }

    pub fn compute_24_hour_sum(bsd: &BrokerStatsData) -> u64 {
        if bsd.get_stats_day().get_sum() != 0 {
            return bsd.get_stats_day().get_sum();
        }
        if bsd.get_stats_hour().get_sum() != 0 {
            return bsd.get_stats_hour().get_sum();
        }
        if bsd.get_stats_minute().get_sum() != 0 {
            return bsd.get_stats_minute().get_sum();
        }
        0
    }
}

async fn collect_topic_detail(
    admin: &DefaultMQAdminExt,
    topic: &CheetahString,
    active_topic: bool,
) -> RocketMQResult<Vec<StatsAllRow>> {
    let topic_route_data = admin.examine_topic_route_info(topic.clone()).await?;
    let group_list = admin.query_topic_consume_by_who(topic.clone()).await?;

    let mut in_tps = 0.0;
    let mut in_msg_count_24h = 0;

    if let Some(route_data) = &topic_route_data {
        for broker_data in &route_data.broker_datas {
            if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                if let Ok(bsd) = admin
                    .view_broker_stats_data(
                        master_addr.clone(),
                        CheetahString::from(Stats::TOPIC_PUT_NUMS),
                        topic.clone(),
                    )
                    .await
                {
                    in_tps += bsd.get_stats_minute().get_tps();
                    in_msg_count_24h += StatsService::compute_24_hour_sum(&bsd);
                }
            }
        }
    }

    let mut rows = Vec::new();
    if !group_list.group_list.is_empty() {
        for group in &group_list.group_list {
            let mut out_tps = 0.0;
            let mut out_msg_count_24h = 0;

            if let Some(route_data) = &topic_route_data {
                for broker_data in &route_data.broker_datas {
                    if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                        let stats_key = format!("{}@{}", topic, group);
                        if let Ok(bsd) = admin
                            .view_broker_stats_data(
                                master_addr.clone(),
                                CheetahString::from(Stats::GROUP_GET_NUMS),
                                CheetahString::from(stats_key.as_str()),
                            )
                            .await
                        {
                            out_tps += bsd.get_stats_minute().get_tps();
                            out_msg_count_24h += StatsService::compute_24_hour_sum(&bsd);
                        }
                    }
                }
            }

            let mut accumulation = 0;
            if let Ok(consume_stats) = admin
                .examine_consume_stats(group.clone(), Some(topic.clone()), None, None, None)
                .await
            {
                accumulation = consume_stats.compute_total_diff();
                if accumulation < 0 {
                    accumulation = 0;
                }
            }

            if !active_topic || in_msg_count_24h > 0 || out_msg_count_24h > 0 {
                rows.push(StatsAllRow {
                    topic: topic.clone(),
                    consumer_group: Some(group.clone()),
                    accumulation,
                    in_tps,
                    out_tps: Some(out_tps),
                    in_msg_count_24h,
                    out_msg_count_24h: Some(out_msg_count_24h),
                });
            }
        }
    } else if !active_topic || in_msg_count_24h > 0 {
        rows.push(StatsAllRow {
            topic: topic.clone(),
            consumer_group: None,
            accumulation: 0,
            in_tps,
            out_tps: None,
            in_msg_count_24h,
            out_msg_count_24h: None,
        });
    }

    Ok(rows)
}

fn should_skip_topic(topic: &str, selected_topic: Option<&CheetahString>) -> bool {
    if topic.starts_with(RETRY_GROUP_TOPIC_PREFIX) || topic.starts_with(DLQ_GROUP_TOPIC_PREFIX) {
        return true;
    }

    selected_topic
        .filter(|selected| !selected.is_empty())
        .is_some_and(|selected| topic != selected.as_str())
}

fn trim_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn admin_builder_with_rpc_hook(builder: AdminBuilder, rpc_hook: Option<Arc<dyn RPCHook>>) -> AdminBuilder {
    match rpc_hook {
        Some(hook) => builder.rpc_hook(hook),
        None => builder,
    }
}
