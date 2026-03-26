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
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
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
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        todo!()
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
    use std::env;
    use std::fs;
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::TimeUtils::current_millis;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::timer::timer_checkpoint::TimerCheckpointSnapshot;
    use rocketmq_store::timer::timer_message_store::TimerMessageStore;
    use rocketmq_store::timer::timer_metrics::TimerMetricsSerializeWrapper;

    use crate::broker_runtime::BrokerRuntime;

    use super::BrokerConfigRequestHandler;

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
}
