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

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::constant::file_readahead_mode::READ_AHEAD_MODE;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigToJsonRequestHeader;
use rocketmq_remoting::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigType;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::utils::ffi::MADV_NORMAL;
use rocketmq_store::utils::ffi::MADV_RANDOM;
use sysinfo::Disks;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct BrokerConfigRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> BrokerConfigRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        BrokerConfigRequestHandler { broker_runtime_inner }
    }
}
impl<MS: MessageStore> BrokerConfigRequestHandler<MS> {
    pub async fn update_broker_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command().set_opaque(request.opaque());
        let Some(body) = request.body() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("broker config body is empty"),
            ));
        };

        let body = String::from_utf8_lossy(body.as_ref());
        let Some(properties) = mix_all::string_to_properties(&body) else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("broker config body format is invalid"),
            ));
        };
        if properties.is_empty() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("broker config body is empty"),
            ));
        }

        let mut updated_config = self.broker_runtime_inner.broker_config().clone();
        let unsupported_keys = match Self::apply_supported_broker_config_properties(&mut updated_config, &properties) {
            Ok(unsupported_keys) => unsupported_keys,
            Err(remark) => {
                return Ok(Some(
                    response.set_code(ResponseCode::InvalidParameter).set_remark(remark),
                ));
            }
        };
        if !unsupported_keys.is_empty() {
            return Ok(Some(response.set_code(ResponseCode::InvalidParameter).set_remark(
                format!("unsupported broker config keys: {}", unsupported_keys.join(",")),
            )));
        }

        let inner = self.broker_runtime_inner.mut_from_ref();
        inner.set_broker_config(updated_config);
        let broker_config_arc = inner.broker_config_arc();
        inner.producer_manager_mut().set_broker_config(broker_config_arc);

        Ok(Some(
            response
                .set_code(ResponseCode::Success)
                .set_remark("update broker config success"),
        ))
    }

    pub async fn get_broker_config(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        // broker config => broker config
        // default message store config => message store config
        let broker_config = self.broker_runtime_inner.broker_config().clone();
        let message_store_config = self
            .broker_runtime_inner
            .message_store()
            .unwrap()
            .get_message_store_config()
            .clone();
        let broker_config_properties = broker_config.get_properties();
        let message_store_config_properties = message_store_config.get_properties();
        let combine_map = broker_config_properties
            .iter()
            .chain(message_store_config_properties.iter())
            .collect::<HashMap<_, _>>();
        let mut body = String::new();
        for (key, value) in combine_map {
            body.push_str(&format!("{key}:{value}\n"));
        }
        if !body.is_empty() {
            response.set_body_mut_ref(body);
        }
        Ok(Some(response))
    }

    pub async fn get_broker_runtime_info(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_response_command();
        let runtime_info = self.prepare_runtime_info();
        let key_value_table = KVTable { table: runtime_info };
        response.set_body_mut_ref(serde_json::to_string(&key_value_table).unwrap());
        Ok(Some(response))
    }

    pub async fn set_commitlog_read_mode(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();
        let Some(ext_fields) = request.get_ext_fields() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("set commitlog readahead mode param error"),
            ));
        };

        let Some(mode_text) = ext_fields.get(READ_AHEAD_MODE) else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("set commitlog readahead mode param error"),
            ));
        };

        let Ok(mode) = mode_text.parse::<i32>() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("set commitlog readahead mode param value error"),
            ));
        };

        if mode != MADV_NORMAL && mode != MADV_RANDOM {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("set commitlog readahead mode param value error"),
            ));
        }

        match self
            .broker_runtime_inner
            .mut_from_ref()
            .message_store_mut()
            .as_mut()
            .expect("message store should exist")
            .set_commitlog_read_mode(mode)
        {
            Ok(()) => {
                Ok(Some(response.set_code(ResponseCode::Success).set_remark(format!(
                    "set commitlog readahead mode success, mode: {mode}"
                ))))
            }
            Err(error) => Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(format!("set commitlog readahead mode failed: {error}")),
            )),
        }
    }

    pub async fn export_rocksdb_config_to_json(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();
        let request_header = request.decode_command_custom_header::<ExportRocksdbConfigToJsonRequestHeader>()?;
        let config_types = match request_header.fetch_config_type() {
            Ok(config_types) => config_types,
            Err(error) => {
                return Ok(Some(
                    response.set_code(ResponseCode::InvalidParameter).set_remark(error),
                ));
            }
        };

        for config_type in config_types {
            match config_type {
                ExportRocksdbConfigType::Topics => {
                    self.broker_runtime_inner.topic_config_manager().persist();
                }
                ExportRocksdbConfigType::SubscriptionGroups => {
                    self.broker_runtime_inner.subscription_group_manager().persist();
                }
                ExportRocksdbConfigType::ConsumerOffsets => {
                    self.broker_runtime_inner.consumer_offset_manager().persist();
                }
            }
        }

        Ok(Some(
            response.set_code(ResponseCode::Success).set_remark("export done."),
        ))
    }

    pub async fn get_timer_metrics(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(self.build_timer_metrics_response()))
    }

    pub async fn get_timer_check_point(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(self.build_timer_checkpoint_response()))
    }

    pub async fn switch_timer_engine(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_response_command();

        if !self.broker_runtime_inner.message_store_config().is_timer_wheel_enable() {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("broker timerWheelEnable is false"),
            ));
        }

        let Some(ext_fields) = request.get_ext_fields() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("param error, extFields is null"),
            ));
        };

        let Some(engine_type) = ext_fields.get(MessageConst::TIMER_ENGINE_TYPE) else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("param error"),
            ));
        };

        if engine_type.as_str() == MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("timerRocksDBEnable must be configured true when broker start"),
            ));
        }

        if engine_type.as_str() != MessageConst::TIMER_ENGINE_FILE_TIME_WHEEL {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("param error"),
            ));
        }

        let Some(timer_message_store) = self.broker_runtime_inner.timer_message_store().cloned() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("switch timer engine error"),
            ));
        };

        timer_message_store.set_should_running_dequeue(true);
        timer_message_store.start();

        Ok(Some(
            response
                .set_code(ResponseCode::Success)
                .set_remark("switch timer engine success"),
        ))
    }

    fn apply_supported_broker_config_properties(
        broker_config: &mut BrokerConfig,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> Result<Vec<String>, CheetahString> {
        let mut unsupported_keys = Vec::new();

        for (key, value) in properties {
            match key.as_str() {
                "enableLiteEventMode" => {
                    broker_config.enable_lite_event_mode = Self::parse_bool_property(key, value)?;
                }
                "liteEventCheckInterval" => {
                    broker_config.lite_event_check_interval = Self::parse_u64_property(key, value)?;
                }
                "liteTtlCheckInterval" => {
                    broker_config.lite_ttl_check_interval = Self::parse_u64_property(key, value)?;
                }
                "liteSubscriptionCheckInterval" => {
                    broker_config.lite_subscription_check_interval = Self::parse_u64_property(key, value)?;
                }
                "liteSubscriptionCheckTimeoutMills" => {
                    broker_config.lite_subscription_check_timeout_mills = Self::parse_u64_property(key, value)?;
                }
                "maxLiteSubscriptionCount" => {
                    broker_config.max_lite_subscription_count = Self::parse_positive_u64_property(key, value)?;
                }
                "enableLitePopLog" => {
                    broker_config.enable_lite_pop_log = Self::parse_bool_property(key, value)?;
                }
                "maxClientEventCount" => {
                    broker_config.max_client_event_count = Self::parse_positive_i32_property(key, value)?;
                }
                "liteEventFullDispatchDelayTime" => {
                    broker_config.lite_event_full_dispatch_delay_time = Self::parse_u64_property(key, value)?;
                }
                "liteLagLatencyCollectEnable" => {
                    broker_config.lite_lag_latency_collect_enable = Self::parse_bool_property(key, value)?;
                }
                "liteLagLatencyMetricsEnable" => {
                    broker_config.lite_lag_latency_metrics_enable = Self::parse_bool_property(key, value)?;
                }
                "liteLagCountMetricsEnable" => {
                    broker_config.lite_lag_count_metrics_enable = Self::parse_bool_property(key, value)?;
                }
                "liteLagLatencyTopK" => {
                    broker_config.lite_lag_latency_top_k = Self::parse_positive_i32_property(key, value)?;
                }
                "validateSystemTopicWhenUpdateTopic" => {
                    broker_config.validate_system_topic_when_update_topic = Self::parse_bool_property(key, value)?;
                }
                "enableMixedMessageType" => {
                    broker_config.enable_mixed_message_type = Self::parse_bool_property(key, value)?;
                }
                _ => unsupported_keys.push(key.to_string()),
            }
        }

        unsupported_keys.sort();
        Ok(unsupported_keys)
    }

    fn parse_bool_property(key: &CheetahString, value: &CheetahString) -> Result<bool, CheetahString> {
        value.parse::<bool>().map_err(|_| {
            CheetahString::from_string(format!("broker config [{}] expects boolean, got [{}]", key, value))
        })
    }

    fn parse_u64_property(key: &CheetahString, value: &CheetahString) -> Result<u64, CheetahString> {
        value.parse::<u64>().map_err(|_| {
            CheetahString::from_string(format!(
                "broker config [{}] expects unsigned integer, got [{}]",
                key, value
            ))
        })
    }

    fn parse_positive_u64_property(key: &CheetahString, value: &CheetahString) -> Result<u64, CheetahString> {
        let parsed = Self::parse_u64_property(key, value)?;
        if parsed == 0 {
            return Err(CheetahString::from_string(format!(
                "broker config [{}] expects positive integer, got [{}]",
                key, value
            )));
        }
        Ok(parsed)
    }

    fn parse_positive_i32_property(key: &CheetahString, value: &CheetahString) -> Result<i32, CheetahString> {
        let parsed = value.parse::<i32>().map_err(|_| {
            CheetahString::from_string(format!("broker config [{}] expects integer, got [{}]", key, value))
        })?;
        if parsed <= 0 {
            return Err(CheetahString::from_string(format!(
                "broker config [{}] expects positive integer, got [{}]",
                key, value
            )));
        }
        Ok(parsed)
    }

    fn build_timer_metrics_response(&self) -> RemotingCommand {
        let mut response =
            RemotingCommand::create_response_command_with_code_remark(ResponseCode::SystemError, "Unknown");
        let Some(timer_message_store) = self.broker_runtime_inner.timer_message_store() else {
            response.set_remark_mut(CheetahString::from_static_str("The timer message store is null"));
            return response;
        };

        response.set_body_mut_ref(timer_message_store.timer_metrics_payload());
        response.set_code_mut(ResponseCode::Success);
        response.set_remark_option_mut(None::<CheetahString>);
        response
    }

    fn build_timer_checkpoint_response(&self) -> RemotingCommand {
        let mut response =
            RemotingCommand::create_response_command_with_code_remark(ResponseCode::SystemError, "Unknown");
        let Some(timer_message_store) = self.broker_runtime_inner.timer_message_store() else {
            response.set_remark_mut(CheetahString::from_static_str("The timer message store is null"));
            return response;
        };
        let Some(checkpoint_body) = timer_message_store.timer_checkpoint_payload() else {
            response.set_remark_mut(CheetahString::from_static_str("The checkpoint is null"));
            return response;
        };

        response.set_body_mut_ref(checkpoint_body);
        response.set_code_mut(ResponseCode::Success);
        response.set_remark_option_mut(None::<CheetahString>);
        response
    }

    fn prepare_runtime_info(&self) -> HashMap<CheetahString, CheetahString> {
        let mut runtime_info = self.broker_runtime_inner.message_store().unwrap().get_runtime_info();
        self.broker_runtime_inner
            .schedule_message_service()
            .build_running_stats(&mut runtime_info);
        runtime_info.insert(
            "brokerActive".to_string(),
            self.is_special_service_running().to_string(),
        );
        let version = CURRENT_VERSION;
        runtime_info.insert("brokerVersionDesc".to_string(), version.name().to_string());
        runtime_info.insert("brokerVersion".to_string(), version.name().to_string());
        let msg_put_total_yesterday_morning = match &self.broker_runtime_inner.broker_stats() {
            Some(broker_stats) => broker_stats.get_msg_put_total_yesterday_morning().to_string(),
            None => String::from("No broker stats available msgPutTotalYesterdayMorning"),
        };
        runtime_info.insert(
            "msgPutTotalYesterdayMorning".to_string(),
            msg_put_total_yesterday_morning,
        );

        let msg_put_total_today_morning = match &self.broker_runtime_inner.broker_stats() {
            Some(broker_stats) => broker_stats.get_msg_put_total_today_morning().to_string(),
            None => String::from("No broker stats available msgPutTotalTodayMorning"),
        };
        runtime_info.insert("msgPutTotalTodayMorning".to_string(), msg_put_total_today_morning);

        let msg_put_total_today_now = match &self.broker_runtime_inner.broker_stats() {
            Some(broker_stats) => broker_stats.get_msg_put_total_today_now().to_string(),
            None => String::from("No broker stats available msgPutTotalTodayNow"),
        };
        runtime_info.insert("msgPutTotalTodayNow".to_string(), msg_put_total_today_now);

        let msg_get_total_yesterday_morning = match &self.broker_runtime_inner.broker_stats() {
            Some(broker_stats) => broker_stats.get_msg_get_total_yesterday_morning().to_string(),
            None => String::from("No broker stats available msgGetTotalYesterdayMorning"),
        };
        runtime_info.insert(
            "msgGetTotalYesterdayMorning".to_string(),
            msg_get_total_yesterday_morning,
        );

        let msg_get_total_today_morning = match &self.broker_runtime_inner.broker_stats() {
            Some(broker_stats) => broker_stats.get_msg_get_total_today_morning().to_string(),
            None => String::from("No broker stats available msgGetTotalTodayMorning"),
        };
        runtime_info.insert("msgGetTotalTodayMorning".to_string(), msg_get_total_today_morning);

        let msg_get_total_today_now = match &self.broker_runtime_inner.broker_stats() {
            Some(broker_stats) => broker_stats.get_msg_get_total_today_now().to_string(),
            None => String::from("No broker stats available msgGetTotalTodayNow"),
        };
        runtime_info.insert("msgGetTotalTodayNow".to_string(), msg_get_total_today_now);
        runtime_info.insert(
            "dispatchBehindBytes".to_string(),
            self.broker_runtime_inner
                .message_store()
                .unwrap()
                .dispatch_behind_bytes()
                .to_string(),
        );
        runtime_info.insert(
            "pageCacheLockTimeMills".to_string(),
            self.broker_runtime_inner
                .message_store()
                .unwrap()
                .lock_time_millis()
                .to_string(),
        );
        runtime_info.insert(
            "earliestMessageTimeStamp".to_string(),
            self.broker_runtime_inner
                .message_store()
                .unwrap()
                .get_earliest_message_time_store()
                .to_string(),
        );
        runtime_info.insert(
            "startAcceptSendRequestTimeStamp".to_string(),
            self.broker_runtime_inner
                .broker_config()
                .get_start_accept_send_request_time_stamp()
                .to_string(),
        );
        let is_timer_wheel_enable = self.broker_runtime_inner.message_store_config().is_timer_wheel_enable();
        if is_timer_wheel_enable {
            runtime_info.insert(
                "timerReadBehind".to_string(),
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_timer_message_store()
                    .unwrap()
                    .get_dequeue_behind()
                    .to_string(),
            );
            runtime_info.insert(
                "timerOffsetBehind".to_string(),
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_timer_message_store()
                    .unwrap()
                    .get_enqueue_behind_messages()
                    .to_string(),
            );
            runtime_info.insert(
                "timerCongestNum".to_string(),
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_timer_message_store()
                    .unwrap()
                    .get_all_congest_num()
                    .to_string(),
            );
            runtime_info.insert(
                "timerEnqueueTps".to_string(),
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_timer_message_store()
                    .unwrap()
                    .get_enqueue_tps()
                    .to_string(),
            );
            runtime_info.insert(
                "timerDequeueTps".to_string(),
                self.broker_runtime_inner
                    .message_store()
                    .unwrap()
                    .get_timer_message_store()
                    .unwrap()
                    .get_dequeue_tps()
                    .to_string(),
            );
        } else {
            runtime_info.insert("timerReadBehind".to_string(), "0".to_string());
            runtime_info.insert("timerOffsetBehind".to_string(), "0".to_string());
            runtime_info.insert("timerCongestNum".to_string(), "0".to_string());
            runtime_info.insert("timerEnqueueTps".to_string(), "0.0".to_string());
            runtime_info.insert("timerDequeueTps".to_string(), "0.0".to_string());
        }
        let default_message_store = self.broker_runtime_inner.message_store().unwrap();
        runtime_info.insert(
            "remainTransientStoreBufferNumbs".to_string(),
            default_message_store.remain_transient_store_buffer_numbs().to_string(),
        );
        if default_message_store
            .get_message_store_config()
            .transient_store_pool_enable
        {
            runtime_info.insert(
                "remainHowManyDataToCommit".to_string(),
                mix_all::human_readable_byte_count(default_message_store.remain_how_many_data_to_commit(), false),
            );
        }
        runtime_info.insert(
            "remainHowManyDataToFlush".to_string(),
            mix_all::human_readable_byte_count(default_message_store.remain_how_many_data_to_flush(), false),
        );
        let store_path_root_dir = &self.broker_runtime_inner.message_store_config().store_path_root_dir;
        let commit_log_dir = std::path::Path::new(store_path_root_dir.as_str());
        if commit_log_dir.exists() {
            let disks = Disks::new_with_refreshed_list();
            let path_str = commit_log_dir.to_str().unwrap();
            for disk in &disks {
                if disk.mount_point().to_str() == Some(path_str) {
                    runtime_info.insert(
                        "commitLogDirCapacity".to_string(),
                        format!(
                            "Total : {}, Free : {}.",
                            mix_all::human_readable_byte_count(disk.total_space() as i64, false),
                            mix_all::human_readable_byte_count(disk.available_space() as i64, false,)
                        ),
                    );
                }
            }
        }
        runtime_info
            .into_iter()
            .map(|(k, v)| (CheetahString::from_string(k), CheetahString::from_string(v)))
            .collect()
    }
    fn is_special_service_running(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::sync::Arc;
    use std::time::SystemTime;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_common::common::config_manager::ConfigManager;
    use rocketmq_common::common::constant::file_readahead_mode::READ_AHEAD_MODE;
    use rocketmq_common::common::message::MessageConst;
    use rocketmq_common::TimeUtils::current_millis;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::Channel;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigToJsonRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_rust::ArcMut;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::timer::timer_checkpoint::TimerCheckpointSnapshot;
    use rocketmq_store::timer::timer_message_store::TimerMessageStore;
    use rocketmq_store::timer::timer_metrics::TimerMetricsSerializeWrapper;
    use rocketmq_store::utils::ffi::MADV_NORMAL;
    use rocketmq_store::utils::ffi::MADV_RANDOM;

    use crate::broker_runtime::BrokerRuntime;

    use super::BrokerConfigRequestHandler;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-broker-config-{label}-{millis}"))
    }

    async fn new_test_runtime(label: &str, timer_wheel_enable: bool) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            timer_wheel_enable,
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await);
        runtime
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn build_timer_metrics_response_returns_encoded_timer_metrics() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);

        let timer_message_store = TimerMessageStore::new_empty();
        timer_message_store
            .timer_metrics
            .add_timing_count(&CheetahString::from_static_str("TimerTopicA"), 2);
        runtime.inner_for_test().set_timer_message_store(timer_message_store);

        let handler = BrokerConfigRequestHandler::new(runtime.inner_for_test().clone());
        let response = handler.build_timer_metrics_response();

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("timer metrics response body should exist");
        let wrapper: TimerMetricsSerializeWrapper = serde_json::from_slice(body.as_ref()).unwrap();
        assert_eq!(wrapper.timing_count_snapshot().get("TimerTopicA"), Some(&2));
    }

    #[tokio::test]
    async fn build_timer_checkpoint_response_returns_encoded_checkpoint_snapshot() {
        let temp_dir = env::temp_dir().join(format!("rmq-rust-timer-checkpoint-{}", current_millis()));
        let _ = fs::remove_dir_all(&temp_dir);
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig {
            timer_wheel_enable: true,
            store_path_root_dir: temp_dir.to_string_lossy().to_string().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config.clone());

        let timer_message_store = TimerMessageStore::new_with_config(None, message_store_config);
        assert!(timer_message_store.load());
        runtime.inner_for_test().set_timer_message_store(timer_message_store);

        let handler = BrokerConfigRequestHandler::new(runtime.inner_for_test().clone());
        let response = handler.build_timer_checkpoint_response();

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("timer checkpoint response body should exist");
        let snapshot = TimerCheckpointSnapshot::decode(body.as_ref()).unwrap();
        assert!(snapshot.last_read_time_ms() > 0);
        assert_eq!(snapshot.master_timer_queue_offset(), 0);
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn set_commitlog_read_mode_updates_store_config() {
        let mut runtime = new_test_runtime("commitlog-read-mode", false).await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = BrokerConfigRequestHandler::new(inner.clone());

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::SetCommitlogReadMode).set_ext_fields(HashMap::new());
        request.add_ext_field(CheetahString::from_static_str(READ_AHEAD_MODE), MADV_NORMAL.to_string());

        let response = handler
            .set_commitlog_read_mode(channel, ctx, RequestCode::SetCommitlogReadMode, &mut request)
            .await
            .expect("set commitlog read mode should succeed")
            .expect("set commitlog read mode should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(
            inner
                .message_store()
                .expect("message store should exist")
                .get_message_store_config()
                .data_read_ahead_enable
        );

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::SetCommitlogReadMode).set_ext_fields(HashMap::new());
        request.add_ext_field(CheetahString::from_static_str(READ_AHEAD_MODE), MADV_RANDOM.to_string());

        let response = handler
            .set_commitlog_read_mode(channel, ctx, RequestCode::SetCommitlogReadMode, &mut request)
            .await
            .expect("set commitlog read mode should succeed")
            .expect("set commitlog read mode should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(
            !inner
                .message_store()
                .expect("message store should exist")
                .get_message_store_config()
                .data_read_ahead_enable
        );

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_broker_config_applies_supported_runtime_properties() {
        let mut runtime = new_test_runtime("update-broker-config", false).await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = BrokerConfigRequestHandler::new(inner.clone());

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig).set_body(concat!(
            "enableLiteEventMode=false\n",
            "maxLiteSubscriptionCount=5\n",
            "maxClientEventCount=7\n",
            "liteEventFullDispatchDelayTime=1234",
        ));

        let response = handler
            .update_broker_config(channel, ctx, RequestCode::UpdateBrokerConfig, &mut request)
            .await
            .expect("update broker config should return broker response")
            .expect("update broker config should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(!inner.broker_config().enable_lite_event_mode);
        assert_eq!(inner.broker_config().max_lite_subscription_count, 5);
        assert_eq!(inner.broker_config().max_client_event_count, 7);
        assert_eq!(inner.broker_config().lite_event_full_dispatch_delay_time, 1234);

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_broker_config_rejects_unsupported_or_invalid_keys() {
        let mut runtime = new_test_runtime("update-broker-config-invalid", false).await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = BrokerConfigRequestHandler::new(inner.clone());

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig)
            .set_body("unknownKey=true\nmaxClientEventCount=0");

        let response = handler
            .update_broker_config(channel, ctx, RequestCode::UpdateBrokerConfig, &mut request)
            .await
            .expect("update broker config should return broker response")
            .expect("update broker config should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("maxClientEventCount")));
        assert_eq!(inner.broker_config().max_client_event_count, 100);

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig).set_body("unknownKey=true");

        let response = handler
            .update_broker_config(channel, ctx, RequestCode::UpdateBrokerConfig, &mut request)
            .await
            .expect("update broker config should return broker response")
            .expect("update broker config should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("unsupported broker config keys: unknownKey")));

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn export_rocksdb_config_to_json_persists_requested_managers() {
        let mut runtime = new_test_runtime("export-config", false).await;
        let mut inner = runtime.inner_for_test().clone();
        inner
            .topic_config_manager_mut()
            .update_topic_config(ArcMut::new(TopicConfig::with_queues("export-topic", 1, 1)));

        let mut subscription_group_config = SubscriptionGroupConfig::default();
        subscription_group_config.set_group_name(CheetahString::from_static_str("export-group"));
        inner
            .subscription_group_manager_mut()
            .update_subscription_group_config(&mut subscription_group_config);
        inner.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("client"),
            &CheetahString::from_static_str("export-group"),
            &CheetahString::from_static_str("export-topic"),
            0,
            7,
        );

        let mut handler = BrokerConfigRequestHandler::new(inner.clone());
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ExportRocksdbConfigToJson,
            ExportRocksdbConfigToJsonRequestHeader {
                config_type: CheetahString::from_static_str("topics;subscriptionGroups;consumerOffsets;"),
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .export_rocksdb_config_to_json(channel, ctx, RequestCode::ExportRocksdbConfigToJson, &mut request)
            .await
            .expect("export config should succeed")
            .expect("export config should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let topic_json =
            fs::read_to_string(inner.topic_config_manager().config_file_path()).expect("topic config file");
        let group_json =
            fs::read_to_string(inner.subscription_group_manager().config_file_path()).expect("group config file");
        let offset_json =
            fs::read_to_string(inner.consumer_offset_manager().config_file_path()).expect("offset config file");
        assert!(topic_json.contains("export-topic"));
        assert!(group_json.contains("export-group"));
        assert!(offset_json.contains("export-topic@export-group"));

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn switch_timer_engine_accepts_file_time_wheel_and_rejects_rocksdb() {
        let mut runtime = new_test_runtime("switch-timer-engine", true).await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = BrokerConfigRequestHandler::new(inner.clone());

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut file_request =
            RemotingCommand::create_remoting_command(RequestCode::SwitchTimerEngine).set_ext_fields(HashMap::new());
        file_request.add_ext_field(
            CheetahString::from_static_str(MessageConst::TIMER_ENGINE_TYPE),
            MessageConst::TIMER_ENGINE_FILE_TIME_WHEEL,
        );

        let response = handler
            .switch_timer_engine(channel, ctx, RequestCode::SwitchTimerEngine, &mut file_request)
            .await
            .expect("switch timer engine should succeed")
            .expect("switch timer engine should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(inner
            .timer_message_store()
            .expect("timer message store should exist")
            .is_should_running_dequeue());

        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut rocksdb_request =
            RemotingCommand::create_remoting_command(RequestCode::SwitchTimerEngine).set_ext_fields(HashMap::new());
        rocksdb_request.add_ext_field(
            CheetahString::from_static_str(MessageConst::TIMER_ENGINE_TYPE),
            MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE,
        );

        let response = handler
            .switch_timer_engine(channel, ctx, RequestCode::SwitchTimerEngine, &mut rocksdb_request)
            .await
            .expect("switch timer engine should succeed")
            .expect("switch timer engine should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
