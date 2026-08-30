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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_model::common::FAQUrl;
use rocketmq_model::version::RocketMqVersion;
use rocketmq_observability::metrics::namesrv::NameServerRouteCacheOutcome;
use rocketmq_observability::metrics::namesrv::NameServerRouteStage;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::common::time_utils;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestProcessor;
use tracing::debug;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeHandle;
use crate::processor::response_factory::NameServerResponseFactoryExt;
use crate::processor::NAMESPACE_ORDER_TOPIC_CONFIG;
use crate::route::response_cache::JsonEncoding;
use crate::route::response_cache::RouteCacheKey;
use crate::route::response_cache::RouteCacheOutcomeKind;
use crate::route::response_cache::RouteCachePolicy;
use crate::route::zone_filter::filter_route_by_zone;
use crate::route::zone_filter::ZoneRequest;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_ENABLED;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_MARKER;
use crate::route::zone_filter::TYPED_ZONE_ROUTE_SHADOW;

#[cfg(test)]
thread_local! {
    static ROUTE_ENCODE_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Client request processor for handling route info queries
pub struct ClientRequestProcessor {
    name_server_runtime_inner: NameServerRuntimeHandle,
    command_factory: RemotingCommandFactory,
    need_check_namesrv_ready: AtomicBool,
    startup_time_millis: u64,
}

impl RequestProcessor for ClientRequestProcessor {
    #[inline]
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let response = self.handle_request(request.command_mut()).await?;
        crate::processor::response_outcome(response)
    }
}

impl ClientRequestProcessor {
    pub(crate) async fn handle_request(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let _runtime_guard = self.name_server_runtime_inner.upgrade().ok_or_else(|| {
            rocketmq_error::RocketMQError::not_initialized("NameServer runtime is no longer available")
        })?;
        let request_code = RequestCode::from(request.code());
        debug!(
            "Name server ClientRequestProcessor Received request code: {:?}",
            request_code
        );

        self.get_route_info_by_topic(request)
    }

    pub(crate) fn new(name_server_runtime_inner: NameServerRuntimeHandle) -> Self {
        let command_factory = name_server_runtime_inner.remoting_command_factory();
        Self {
            need_check_namesrv_ready: AtomicBool::new(true),
            startup_time_millis: time_utils::current_millis(),
            name_server_runtime_inner,
            command_factory,
        }
    }

    /// Handles route info query for a specific topic
    #[inline]
    fn get_route_info_by_topic(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let route_span = rocketmq_observability::trace::namesrv::route_lookup_span();
        let _route_guard = route_span.enter();
        let request_header = match request.decode_command_custom_header::<GetRouteInfoRequestHeader>() {
            Ok(header) => header,
            Err(error) => {
                route_span.record("result", "invalid_request");
                return Err(error);
            }
        };
        let route_config = self.name_server_runtime_inner.name_server_config();

        if route_config.need_wait_for_service {
            let elapsed_millis = time_utils::current_millis().saturating_sub(self.startup_time_millis);
            let wait_seconds_millis = route_config.wait_seconds_for_service as u64 * 1000;
            let namesrv_ready =
                !self.need_check_namesrv_ready.load(Ordering::Relaxed) || elapsed_millis >= wait_seconds_millis;

            if !namesrv_ready {
                warn!("name server not ready. request code {}", request.code());
                let error = rocketmq_error::RocketMQError::not_initialized("name server not ready");
                route_span.record("result", "not_ready");
                return Ok(Some(
                    self.command_factory
                        .command_from_error_with_remark(&error, "name server not ready"),
                ));
            }
        }

        // Lookup topic route data
        let topic_route_view = match self
            .name_server_runtime_inner
            .route_info_manager()
            .load_topic_route_view(request_header.topic.as_ref())
        {
            Ok(data) => data,
            Err(
                rocketmq_error::RocketMQError::TopicNotExist { .. }
                | rocketmq_error::RocketMQError::RouteNotFound { .. },
            ) => {
                route_span.record("result", "not_found");
                return Ok(Some(
                    self.command_factory
                        .create_response_command_with_code(ResponseCode::TopicNotExist)
                        .set_remark(format!(
                            "No topic route info in name server for the topic: {}{}",
                            request_header.topic,
                            FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                        )),
                ));
            }
            Err(error) => {
                route_span.record("result", "internal");
                return Err(error);
            }
        };
        let metrics = self.name_server_runtime_inner.namesrv_metrics();
        if metrics.should_record_route_freshness(route_config.route_freshness_sample_interval) {
            metrics.record_route_freshness_sampled();
            if let Some(freshness_ms) = self
                .name_server_runtime_inner
                .route_info_manager()
                .route_freshness_millis(topic_route_view.route_data())
            {
                metrics.record_route_freshness(freshness_ms);
            }
        }
        if self.need_check_namesrv_ready.load(Ordering::Relaxed) {
            self.need_check_namesrv_ready.store(false, Ordering::Relaxed);
        }

        let mut route_with_order_config;
        let topic_route_data = if route_config.order_message_enable {
            route_with_order_config = topic_route_view.route_data().as_ref().clone();
            route_with_order_config.order_topic_conf = self.name_server_runtime_inner.kvconfig_manager().get_kvconfig(
                &CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                &request_header.topic,
            );
            &route_with_order_config
        } else {
            topic_route_view.route_data().as_ref()
        };
        let zone_request = ZoneRequest::from_command(request);
        let zone_filtered = zone_request.is_enabled();
        let force_java_zone_legacy_json = route_config.namesrv_typed_zone_route_enable && zone_filtered;
        let filtered_route;
        let topic_route_data = if zone_filtered {
            request.add_ext_field(TYPED_ZONE_ROUTE_MARKER, TYPED_ZONE_ROUTE_ENABLED);
            let filter_started = Instant::now();
            filtered_route = filter_route_by_zone(topic_route_data, &zone_request);
            metrics.record_route_stage(NameServerRouteStage::ZoneFilter, filter_started.elapsed());
            filtered_route.as_ref()
        } else {
            if route_config.namesrv_typed_zone_route_shadow {
                request.add_ext_field(TYPED_ZONE_ROUTE_MARKER, TYPED_ZONE_ROUTE_SHADOW);
            }
            topic_route_data
        };

        let cache_policy = RouteCachePolicy {
            enabled: route_config.namesrv_route_response_cache_enable,
            zone_requested: zone_request.is_enabled(),
            order_enabled: route_config.order_message_enable,
            external_route: false,
        };
        let encode = || {
            let encode_started = Instant::now();
            let result = encode_topic_route_response_for_zone(
                topic_route_data,
                request.version(),
                request_header.accept_standard_json_only,
                force_java_zone_legacy_json,
            );
            metrics.record_route_stage(NameServerRouteStage::Encode, encode_started.elapsed());
            result
        };
        let content = if cache_policy.is_eligible() {
            let encoding = if should_use_standard_json(request.version(), request_header.accept_standard_json_only) {
                JsonEncoding::Standard
            } else {
                JsonEncoding::Legacy
            };
            let key = RouteCacheKey::new(
                request_header.topic.clone(),
                topic_route_view.version(),
                topic_route_view.variant(),
                encoding,
            );
            let cache = self.name_server_runtime_inner.route_response_cache();
            cache.get_or_try_insert_with(key, encode).map(|outcome| {
                if metrics.is_enabled() {
                    let metric_outcome = match outcome.kind {
                        RouteCacheOutcomeKind::Hit => NameServerRouteCacheOutcome::Hit,
                        RouteCacheOutcomeKind::Miss => NameServerRouteCacheOutcome::Miss,
                        RouteCacheOutcomeKind::Oversize => NameServerRouteCacheOutcome::Oversize,
                    };
                    metrics.record_route_cache(metric_outcome, cache.stats().weighted_size);
                }
                outcome.body
            })
        } else {
            if metrics.is_enabled() {
                metrics.record_route_cache(
                    NameServerRouteCacheOutcome::Bypass,
                    self.name_server_runtime_inner
                        .route_response_cache()
                        .stats()
                        .weighted_size,
                );
            }
            encode().map(Bytes::from)
        };
        let content = match content {
            Ok(content) => content,
            Err(error) => {
                route_span.record("result", "internal");
                return Err(error);
            }
        };
        metrics.record_route_response_bytes(content.len());
        route_span.record("result", "success");
        Ok(Some(
            self.command_factory
                .create_response_command_with_code(ResponseCode::Success)
                .set_body(content),
        ))
    }
}

#[cfg(test)]
pub(crate) fn encode_topic_route_response(
    topic_route_data: &TopicRouteData,
    request_version: i32,
    accept_standard_json_only: Option<bool>,
) -> rocketmq_error::RocketMQResult<Vec<u8>> {
    encode_topic_route_response_for_zone(topic_route_data, request_version, accept_standard_json_only, false)
}

pub(crate) fn encode_topic_route_response_for_zone(
    topic_route_data: &TopicRouteData,
    request_version: i32,
    accept_standard_json_only: Option<bool>,
    force_java_zone_legacy_json: bool,
) -> rocketmq_error::RocketMQResult<Vec<u8>> {
    #[cfg(test)]
    ROUTE_ENCODE_COUNT.with(|count| count.set(count.get() + 1));
    if force_java_zone_legacy_json {
        topic_route_data.encode()
    } else if should_use_standard_json(request_version, accept_standard_json_only) {
        topic_route_data.encode_standard_json()
    } else {
        topic_route_data.encode()
    }
}

#[cfg(test)]
pub(crate) fn reset_route_encode_count() {
    ROUTE_ENCODE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn route_encode_count() -> u64 {
    ROUTE_ENCODE_COUNT.with(std::cell::Cell::get)
}

pub(crate) fn should_use_standard_json(request_version: i32, accept_standard_json_only: Option<bool>) -> bool {
    request_version >= RocketMqVersion::V4_9_4 as i32 || accept_standard_json_only.unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
    use rocketmq_protocol::protocol::route::route_data_view::QueueData;

    use super::*;

    fn sample_topic_route_data() -> TopicRouteData {
        let mut broker_addrs = HashMap::new();
        broker_addrs.insert(2, CheetahString::from("10.0.0.2:10911"));
        broker_addrs.insert(10, CheetahString::from("10.0.0.10:10911"));

        TopicRouteData {
            order_topic_conf: Some(CheetahString::from("order-conf")),
            queue_datas: vec![QueueData::new(CheetahString::from("broker-a"), 4, 4, 6, 0)],
            broker_datas: vec![BrokerData::new(
                CheetahString::from("cluster-a"),
                CheetahString::from("broker-a"),
                broker_addrs,
                Some(CheetahString::from("zone-a")),
            )],
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        }
    }

    #[test]
    fn uses_standard_json_for_modern_versions() {
        assert!(should_use_standard_json(RocketMqVersion::V4_9_4 as i32, Some(false)));
        assert!(should_use_standard_json(RocketMqVersion::V5_0_0 as i32, None));
    }

    #[test]
    fn uses_standard_json_for_legacy_versions_when_flag_is_enabled() {
        assert!(should_use_standard_json(RocketMqVersion::V4_9_3 as i32, Some(true)));
        assert!(!should_use_standard_json(RocketMqVersion::V4_9_3 as i32, Some(false)));
        assert!(!should_use_standard_json(RocketMqVersion::V4_9_3 as i32, None));
    }

    #[test]
    fn standard_json_branch_uses_standard_encoder() {
        let topic_route_data = sample_topic_route_data();

        let encoded =
            encode_topic_route_response(&topic_route_data, RocketMqVersion::V4_9_4 as i32, Some(false)).unwrap();

        assert_eq!(encoded, topic_route_data.encode_standard_json().unwrap());
    }

    #[test]
    fn legacy_json_branch_uses_legacy_encoder() {
        let topic_route_data = sample_topic_route_data();

        let encoded =
            encode_topic_route_response(&topic_route_data, RocketMqVersion::V4_9_3 as i32, Some(false)).unwrap();

        assert_eq!(encoded, topic_route_data.encode().unwrap());
    }

    #[test]
    fn java_zone_mode_forces_legacy_encoder_for_modern_requests() {
        let topic_route_data = sample_topic_route_data();

        let encoded =
            encode_topic_route_response_for_zone(&topic_route_data, RocketMqVersion::V4_9_4 as i32, Some(true), true)
                .unwrap();

        assert_eq!(encoded, topic_route_data.encode().unwrap());
    }
}
