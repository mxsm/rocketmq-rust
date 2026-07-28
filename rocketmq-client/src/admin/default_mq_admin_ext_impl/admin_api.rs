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

use super::broker::*;
use super::group::*;
use super::security::*;
use super::topic::*;
use super::*;

#[allow(unused_variables)]
#[allow(unused_mut)]
impl MQAdminExt for DefaultMQAdminExtImpl {
    async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                self.client_config.change_instance_name_to_pid();
                if "{}".eq(&self.client_config.socks_proxy_config) {
                    self.client_config.socks_proxy_config =
                        env::var(SOCKS_PROXY_JSON).unwrap_or_else(|_| "{}".to_string()).into();
                }
                let pooled = self
                    .client_pool
                    .get_or_create(self.client_config.clone(), self.rpc_hook.clone())?;
                let (client_instance, token) = pooled.into_parts();
                self.client_instance = Some(client_instance);
                self.client_pool_token = Some(token);

                let group = &self.admin_ext_group.clone();
                let register_ok = self
                    .client_instance
                    .as_mut()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
                    .register_admin_ext(group)
                    .await;
                if !register_ok {
                    if let Some(token) = self.client_pool_token.take() {
                        self.client_pool.release(token).await;
                    }
                    self.service_state = ServiceState::StartFailed;
                    return Err(rocketmq_error::RocketMQError::illegal_argument(format!(
                        "The adminExt group[{}] has created already, specified another name please.{}",
                        self.admin_ext_group,
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    )));
                }
                let arc_mut = self
                    .client_instance
                    .clone()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;
                if let Err(error) = self
                    .client_instance
                    .as_mut()
                    .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
                    .start()
                    .await
                {
                    if let Some(token) = self.client_pool_token.take() {
                        self.client_pool.release(token).await;
                    }
                    return Err(error);
                }
                self.service_state = ServiceState::Running;
                info!("the adminExt [{}] start OK", self.admin_ext_group);
                Ok(())
            }
            ServiceState::Running | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
                Err(rocketmq_error::RocketMQError::ClientAlreadyStarted)
            }
        }
    }

    async fn shutdown(&mut self) {
        match self.service_state {
            ServiceState::CreateJust | ServiceState::ShutdownAlready | ServiceState::StartFailed => {
                // do nothing
            }
            ServiceState::Running => {
                if let Some(instance) = self.client_instance.as_mut() {
                    instance.unregister_admin_ext(&self.admin_ext_group).await;
                }
                if let Some(token) = self.client_pool_token.take() {
                    self.client_pool.release(token).await;
                }
                self.service_state = ServiceState::ShutdownAlready;
            }
        }
    }

    async fn add_broker_to_container(
        &self,
        broker_container_addr: CheetahString,
        broker_config: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .add_broker(&broker_container_addr, broker_config, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn remove_broker_from_container(
        &self,
        broker_container_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .remove_broker(
                    &broker_container_addr,
                    cluster_name,
                    broker_name,
                    broker_id,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_broker_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let validator_input = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>();
        Validators::check_broker_config(&validator_input)?;

        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .update_broker_config(&broker_addr, properties, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_broker_config(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, CheetahString>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_config(&broker_addr, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_and_update_topic_config(
        &self,
        addr: CheetahString,
        config: TopicConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        let topic = config
            .topic_name
            .clone()
            .ok_or_else(|| rocketmq_error::RocketMQError::IllegalArgument("Topic name is required".into()))?;
        let attributes = encode_topic_attributes(&config.attributes);
        let request_header = CreateTopicRequestHeader {
            topic,
            default_topic: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
            read_queue_nums: config.read_queue_nums as i32,
            write_queue_nums: config.write_queue_nums as i32,
            perm: config.perm as i32,
            topic_filter_type: CheetahString::from_static_str(config.topic_filter_type.as_str()),
            topic_sys_flag: Some(config.topic_sys_flag as i32),
            order: config.order,
            attributes,
            force: Some(false),
            topic_request_header: None,
        };

        self.mq_client_api()?
            .update_or_create_topic(&addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_and_update_topic_config_list(
        &self,
        addr: CheetahString,
        topic_config_list: Vec<TopicConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        for config in topic_config_list {
            self.create_and_update_topic_config(addr.clone(), config).await?;
        }
        Ok(())
    }

    async fn create_and_update_plain_access_config(
        &self,
        addr: CheetahString,
        config: PlainAccessConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .create_and_update_plain_access_config(addr, &config, self.remoting_timeout_millis()?)
            .await
    }

    async fn delete_plain_access_config(
        &self,
        addr: CheetahString,
        access_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_plain_access_config(addr, access_key, self.remoting_timeout_millis()?)
            .await
    }

    async fn update_global_white_addr_config(
        &self,
        addr: CheetahString,
        global_white_addrs: CheetahString,
        acl_file_full_path: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mq_client_api = self.mq_client_api()?;

        validate_acl_file_path_for_global_white_addr_config(acl_file_full_path.as_ref())?;

        mq_client_api
            .update_global_white_addrs_config(addr, global_white_addrs, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_broker_cluster_acl_version_info(
        &self,
        addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        let version_info = self
            .mq_client_api()?
            .get_broker_cluster_acl_version_info(addr, self.remoting_timeout_millis()?)
            .await?;
        serde_json::to_string(&version_info)
            .map(CheetahString::from_string)
            .map_err(|error| mq_client_err!(format!("encode ClusterAclVersionInfo failed: {error}")))
    }

    async fn create_and_update_subscription_group_config(
        &self,
        addr: CheetahString,
        config: SubscriptionGroupConfig,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .create_subscription_group(&addr, &config, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_and_update_subscription_group_config_list(
        &self,
        broker_addr: CheetahString,
        configs: Vec<SubscriptionGroupConfig>,
    ) -> rocketmq_error::RocketMQResult<()> {
        for config in configs {
            self.create_and_update_subscription_group_config(broker_addr.clone(), config)
                .await?;
        }
        Ok(())
    }

    async fn examine_subscription_group_config(
        &self,
        addr: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupConfig> {
        self.mq_client_api()?
            .get_subscription_group_config(&addr, group, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_topic_stats(
        &self,
        topic: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        let timeout = self.remoting_timeout_millis()?;
        let request_header = GetTopicStatsInfoRequestHeader {
            topic: topic.clone(),
            topic_request_header: None,
        };
        if let Some(addr) = broker_addr {
            return self
                .mq_client_api()?
                .get_topic_stats_info(&addr, request_header, timeout)
                .await;
        }

        let topic_route = self.examine_topic_route_info(topic).await?;
        let mut result = TopicStatsTable::new();
        if let Some(route_data) = topic_route {
            for broker_data in &route_data.broker_datas {
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let stats = self
                        .mq_client_api()?
                        .get_topic_stats_info(master_addr, request_header.clone(), timeout)
                        .await?;
                    result.get_offset_table_mut().extend(stats.into_offset_table());
                }
            }
        }

        Ok(result)
    }

    async fn examine_topic_stats_concurrent(&self, topic: CheetahString) -> AdminToolResult<TopicStatsTable> {
        match self.examine_topic_stats(topic, None).await {
            Ok(stats) => AdminToolResult::success(stats),
            Err(error) => AdminToolResult::failure(
                crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum::RemotingError,
                error.to_string(),
            ),
        }
    }

    async fn fetch_all_topic_list(&self) -> rocketmq_error::RocketMQResult<TopicList> {
        self.mq_client_api()?
            .get_all_topic_list_from_name_server(self.remoting_timeout_millis()?)
            .await
    }

    async fn fetch_topics_by_cluster(&self, cluster_name: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        self.mq_client_api()?
            .get_topics_by_cluster(cluster_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn fetch_broker_runtime_stats(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.mq_client_api()?
            .get_broker_runtime_info(&broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_consume_stats(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
        broker_addr: Option<CheetahString>,
        timeout_millis: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats> {
        let timeout = timeout_millis.unwrap_or(self.remoting_timeout_millis()?);
        let topic_str = topic.clone().unwrap_or_default();

        if let Some(addr) = broker_addr {
            let request_header = GetConsumeStatsRequestHeader {
                consumer_group,
                topic: topic_str,
                topic_request_header: None,
            };
            return self
                .mq_client_api()?
                .get_consume_stats(&addr, request_header, timeout)
                .await;
        }

        let retry_topic: CheetahString = rocketmq_model::common::mix_all::get_retry_topic(&consumer_group).into();
        let topic_route = self
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&retry_topic, timeout)
            .await?;

        let mut result = ConsumeStats::new();

        if let Some(route_data) = topic_route {
            for bd in &route_data.broker_datas {
                if let Some(master_addr) = bd.broker_addrs().get(&rocketmq_model::common::mix_all::MASTER_ID) {
                    let request_header = GetConsumeStatsRequestHeader {
                        consumer_group: consumer_group.clone(),
                        topic: topic_str.clone(),
                        topic_request_header: None,
                    };
                    let cs = self
                        .mq_client_api()?
                        .get_consume_stats(master_addr, request_header, timeout)
                        .await?;

                    result.get_offset_table_mut().extend(cs.offset_table);
                    let new_tps = result.get_consume_tps() + cs.consume_tps;
                    result.set_consume_tps(new_tps);
                }
            }
        }

        Ok(result)
    }

    async fn check_rocksdb_cq_write_progress(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        check_store_time: i64,
    ) -> rocketmq_error::RocketMQResult<CheckRocksdbCqWriteResult> {
        self.mq_client_api()?
            .check_rocksdb_cq_write_progress(&broker_addr, topic, check_store_time, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_broker_cluster_info(&self) -> rocketmq_error::RocketMQResult<ClusterInfo> {
        self.mq_client_api()?
            .get_broker_cluster_info(self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_topic_route_info(
        &self,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_consumer_connection_info(
        &self,
        consumer_group: CheetahString,
        broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<ConsumerConnection> {
        let mut result = ConsumerConnection::new();
        let timeout = self.remoting_timeout_millis()?;

        let selected_addr = if let Some(broker_addr) = broker_addr {
            Some(broker_addr)
        } else {
            let topic = CheetahString::from_string(mix_all::get_retry_topic(consumer_group.as_str()));
            let topic_route_data = self
                .mq_client_api()?
                .get_topic_route_info_from_name_server(&topic, timeout)
                .await?;

            topic_route_data.and_then(|topic_route_data| {
                topic_route_data
                    .broker_datas
                    .choose(&mut rand::rng())
                    .and_then(|broker_data| broker_data.select_broker_addr())
            })
        };

        if let Some(broker_addr) = selected_addr {
            result = self
                .mq_client_api()?
                .get_consumer_connection_list(broker_addr.as_str(), consumer_group.clone(), timeout)
                .await?;
        }

        if result.get_connection_set().is_empty() {
            return Err(mq_client_err!(
                rocketmq_protocol::code::response_code::ResponseCode::ConsumerNotOnline,
                "Not found the consumer group connection"
            ));
        }

        Ok(result)
    }

    async fn examine_producer_connection_info(
        &self,
        producer_group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        let mut result = ProducerConnection::new();
        let timeout = self.remoting_timeout_millis()?;

        if let Some(topic_route_data) = self.examine_topic_route_info(topic).await? {
            let brokers = &topic_route_data.broker_datas;
            let selected_addr = brokers
                .choose(&mut rand::rng())
                .and_then(|broker_data| broker_data.select_broker_addr());
            if let Some(addr) = selected_addr {
                result = self
                    .mq_client_api()?
                    .get_producer_connection_list(addr.as_str(), producer_group.clone(), timeout)
                    .await?;
            }
        }

        if result.connection_set().is_empty() {
            return Err(mq_client_err!("Not found the producer group connection"));
        }

        Ok(result)
    }

    async fn get_all_producer_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo> {
        self.mq_client_api()?
            .get_all_producer_info(broker_addr.as_str(), self.remoting_timeout_millis()?)
            .await
    }

    async fn get_name_server_address_list(&self) -> Vec<CheetahString> {
        self.client_instance
            .as_ref()
            .map(|client_instance| {
                client_instance
                    .get_mq_client_api_impl()
                    .map(|mq_client_api| mq_client_api.get_name_server_address_list().to_vec())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

    async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.mq_client_api()?
            .wipe_write_perm_of_broker(namesrv_addr, broker_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<i32> {
        self.mq_client_api()?
            .add_write_perm_of_broker(namesrv_addr, broker_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn put_kv_config(&self, namespace: CheetahString, key: CheetahString, value: CheetahString) {
        if let Err(error) = self.create_and_update_kv_config(namespace, key, value).await {
            warn!("put_kv_config failed: {}", error);
        }
    }

    async fn get_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        Ok(self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .get_kvconfig_value(namespace, key, self.remoting_timeout_millis()?)
            .await?
            .unwrap_or_default())
    }

    async fn get_kv_list_by_namespace(&self, namespace: CheetahString) -> rocketmq_error::RocketMQResult<KVTable> {
        self.mq_client_api()?
            .get_kvlist_by_namespace(namespace, self.remoting_timeout_millis()?)
            .await
    }

    async fn delete_topic(
        &self,
        topic_name: CheetahString,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let cluster_info = self.examine_broker_cluster_info().await?;
        let mut broker_addrs = HashSet::new();
        if let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() {
            if let Some(broker_names) = cluster_addr_table.get(&cluster_name) {
                if let Some(broker_addr_table) = cluster_info.broker_addr_table.as_ref() {
                    for broker_name in broker_names {
                        if let Some(broker_data) = broker_addr_table.get(broker_name) {
                            broker_addrs.extend(broker_data.broker_addrs().values().cloned());
                        }
                    }
                }
            }
        }
        self.delete_topic_in_broker(broker_addrs, topic_name.clone()).await?;

        let namesrv_addrs: HashSet<CheetahString> = self.get_name_server_address_list().await.into_iter().collect();
        self.delete_topic_in_name_server(namesrv_addrs, Some(cluster_name), topic_name)
            .await
    }

    async fn delete_topic_in_broker(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = DeleteTopicRequestHeader {
            topic: topic.clone(),
            topic_request_header: None,
        };
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        for addr in addrs {
            api.delete_topic_in_broker(
                &addr,
                DeleteTopicRequestHeader {
                    topic: request_header.topic.clone(),
                    topic_request_header: None,
                },
                timeout,
            )
            .await?;
        }
        Ok(())
    }

    async fn delete_topic_in_name_server(
        &self,
        addrs: HashSet<CheetahString>,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = DeleteTopicFromNamesrvRequestHeader::new(topic, cluster_name);
        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        for addr in addrs {
            api.delete_topic_in_nameserver(&addr, request_header.clone(), timeout)
                .await?;
        }
        Ok(())
    }

    async fn delete_subscription_group(
        &self,
        addr: CheetahString,
        group_name: CheetahString,
        remove_offset: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_subscription_group(
                &addr,
                group_name,
                remove_offset.unwrap_or(false),
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn create_and_update_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .put_kvconfig_value(namespace, key, value, self.remoting_timeout_millis()?)
            .await
    }

    async fn delete_kv_config(
        &self,
        namespace: CheetahString,
        key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .delete_kvconfig_value(namespace, key, self.remoting_timeout_millis()?)
            .await
    }

    async fn reset_offset_by_timestamp(
        &self,
        cluster_name: Option<CheetahString>,
        topic: CheetahString,
        group: CheetahString,
        timestamp: u64,
        is_force: bool,
    ) -> rocketmq_error::RocketMQResult<HashMap<MessageQueue, u64>> {
        let timestamp = timestamp_to_java_long("resetOffsetByTimestamp", timestamp)?;
        let topic_route = self.examine_topic_route_info(topic.clone()).await?;
        let mut offset_table = HashMap::new();
        let timeout = self.remoting_timeout_millis()?;

        if let Some(route_data) = topic_route {
            for broker_data in &route_data.broker_datas {
                if let Some(expected_cluster) = cluster_name.as_ref() {
                    if broker_data.cluster() != expected_cluster {
                        continue;
                    }
                }
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let request_header = ResetOffsetRequestHeader {
                        topic: topic.clone(),
                        group: group.clone(),
                        queue_id: -1,
                        offset: Some(-1),
                        timestamp,
                        is_force,
                        topic_request_header: None,
                    };
                    let offsets = self
                        .mq_client_api()?
                        .invoke_broker_to_reset_offset(master_addr, request_header, timeout)
                        .await?;
                    for (mq, offset) in offsets {
                        offset_table.insert(mq, java_long_to_u64("resetOffsetByTimestamp", "offset", offset)?);
                    }
                }
            }
        }

        Ok(offset_table)
    }

    async fn reset_offset_new(
        &self,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        match self
            .reset_offset_by_timestamp(None, topic.clone(), consumer_group.clone(), timestamp, true)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_consumer_not_online_error(&error) => {
                self.reset_offset_by_timestamp_old(None, consumer_group, topic, timestamp, true)
                    .await?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn get_consume_status(
        &self,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<MessageQueue, u64>>> {
        let Some(route_data) = self.examine_topic_route_info(topic.clone()).await? else {
            return Ok(HashMap::new());
        };

        let api = self.mq_client_api()?;
        let timeout = self.remoting_timeout_millis()?;
        let mut merged = HashMap::new();
        let mut queried = false;
        let mut last_error = None;

        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            queried = true;
            match api
                .invoke_broker_to_get_consumer_status(
                    addr.as_str(),
                    topic.clone(),
                    group.clone(),
                    client_addr.clone(),
                    timeout,
                )
                .await
            {
                Ok(result) => merge_consume_status_result(&mut merged, result)?,
                Err(error) => {
                    warn!("get_consume_status failed on broker {}: {}", addr, error);
                    last_error = Some(error);
                }
            }
        }

        if !merged.is_empty() || last_error.is_none() || !queried {
            return Ok(merged);
        }

        if let Some(error) = last_error {
            Err(error)
        } else {
            Ok(merged)
        }
    }

    async fn create_or_update_order_conf(
        &self,
        key: CheetahString,
        value: CheetahString,
        is_cluster: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        if is_cluster {
            return self
                .mq_client_api()?
                .put_kvconfig_value(
                    CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                    key,
                    value,
                    self.remoting_timeout_millis()?,
                )
                .await;
        }

        let existing = self
            .mq_client_api()?
            .get_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                key.clone(),
                self.remoting_timeout_millis()?,
            )
            .await?
            .unwrap_or_default();

        let merged_order_conf = merge_order_conf_entries(existing.as_str(), value.as_str());

        self.mq_client_api()?
            .put_kvconfig_value(
                CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG),
                key,
                merged_order_conf.into(),
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn query_topic_consume_by_who(&self, topic: CheetahString) -> rocketmq_error::RocketMQResult<GroupList> {
        let topic_route = self
            .mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, self.remoting_timeout_millis()?)
            .await?;

        if let Some(route_data) = topic_route {
            for bd in &route_data.broker_datas {
                if let Some(master_addr) = bd.broker_addrs().get(&rocketmq_model::common::mix_all::MASTER_ID) {
                    let request_header = QueryTopicConsumeByWhoRequestHeader {
                        topic: topic.clone(),
                        topic_request_header: None,
                    };
                    return self
                        .mq_client_api()?
                        .query_topic_consume_by_who(master_addr, request_header, self.remoting_timeout_millis()?)
                        .await;
                }
            }
        }

        Ok(GroupList::default())
    }

    async fn query_topics_by_consumer(&self, group: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        self.query_topics_by_consumer_from_route(group).await
    }

    async fn query_topics_by_consumer_concurrent(&self, group: CheetahString) -> AdminToolResult<TopicList> {
        let timeout = match self.remoting_timeout_millis() {
            Ok(timeout) => timeout,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let retry_topic: CheetahString = mix_all::get_retry_topic(&group).into();
        let api = match self.mq_client_api() {
            Ok(api) => api,
            Err(error) => {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::MQClientError, error.to_string());
            }
        };
        let topic_route = match api.get_topic_route_info_from_name_server(&retry_topic, timeout).await {
            Ok(Some(route_data)) if !route_data.broker_datas.is_empty() => route_data,
            Ok(_) => {
                return AdminToolResult::failure(
                    AdminToolsResultCodeEnum::TopicRouteInfoNotExist,
                    "router info not found.".to_string(),
                )
            }
            Err(error) => {
                return AdminToolResult::failure(AdminToolsResultCodeEnum::MQClientError, error.to_string());
            }
        };

        let mut result = TopicList::default();
        for broker_data in &topic_route.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            match api
                .query_topics_by_consumer(&addr, QueryTopicsByConsumerRequestHeader::new(group.clone()), timeout)
                .await
            {
                Ok(topic_list) => {
                    for topic in topic_list.topic_list {
                        if !result.topic_list.contains(&topic) {
                            result.topic_list.push(topic);
                        }
                    }
                }
                Err(error) => {
                    warn!("query_topics_by_consumer error. group={}, error={}", group, error);
                }
            }
        }

        AdminToolResult::success(result)
    }

    async fn query_subscription(
        &self,
        group: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<SubscriptionData> {
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        let topic_route = api.get_topic_route_info_from_name_server(&topic, timeout).await?;
        let Some(route_data) = topic_route else {
            return Err(mq_client_err!(format!("Topic route not found for: {topic}")));
        };

        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };
            let request_header = QuerySubscriptionByConsumerRequestHeader {
                group: group.clone(),
                topic: topic.clone(),
                topic_request_header: None,
            };
            return api.query_subscription_by_consumer(&addr, request_header, timeout).await;
        }

        Err(mq_client_err!(format!("Broker address not found for topic: {topic}")))
    }

    async fn clean_expired_consumer_queue(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.execute_broker_cleanup_operation(cluster, addr, BrokerCleanupOperation::CleanExpiredConsumerQueue)
            .await
    }

    async fn clean_expired_consumer_queue_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.clean_expired_consumer_queue(None, Some(addr)).await
    }

    async fn delete_expired_commit_log(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.execute_broker_cleanup_operation(cluster, addr, BrokerCleanupOperation::DeleteExpiredCommitLog)
            .await
    }

    async fn delete_expired_commit_log_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.delete_expired_commit_log(None, Some(addr)).await
    }

    async fn clean_unused_topic(
        &self,
        cluster: Option<CheetahString>,
        addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.execute_broker_cleanup_operation(cluster, addr, BrokerCleanupOperation::CleanUnusedTopic)
            .await
    }

    async fn clean_unused_topic_by_addr(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<bool> {
        self.clean_unused_topic(None, Some(addr)).await
    }

    async fn get_consumer_running_info(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        jstack: bool,
        _metrics: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<ConsumerRunningInfo> {
        let broker_addr = self
            .examine_consumer_connection_info(consumer_group.clone(), None)
            .await?
            .get_connection_set()
            .iter()
            .find(|connection| connection.get_client_id() == client_id)
            .map(|connection| connection.get_client_addr().clone())
            .ok_or_else(|| {
                rocketmq_error::RocketMQError::IllegalArgument(format!(
                    "Client `{}` was not found in consumer group `{}`",
                    client_id, consumer_group
                ))
            })?;

        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .get_consumer_running_info(
                &broker_addr,
                consumer_group,
                client_id,
                jstack,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn consume_message_directly(
        &self,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        let consumer_connection = self
            .examine_consumer_connection_info(consumer_group.clone(), None)
            .await?;
        let (resolved_client_id, client_addr) =
            select_consumer_direct_connection(&consumer_group, &consumer_connection, Some(&client_id))?;
        let message = MQAdminExt::query_message(self, CheetahString::default(), topic.clone(), msg_id.clone()).await?;
        let request_header = ConsumeMessageDirectlyResultRequestHeader {
            consumer_group,
            client_id: Some(resolved_client_id),
            msg_id: Some(msg_id),
            broker_name: (!message.broker_name().is_empty()).then(|| message.broker_name.clone()),
            topic: Some(topic),
            topic_sys_flag: None,
            group_sys_flag: None,
            topic_request_header: None,
        };

        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .consume_message_directly(&client_addr, request_header, &message, self.remoting_timeout_millis()?)
            .await
    }

    async fn consume_message_directly_ext(
        &self,
        _cluster_name: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<ConsumeMessageDirectlyResult> {
        self.consume_message_directly(consumer_group, client_id, topic, msg_id)
            .await
    }

    async fn clone_group_offset(
        &self,
        src_group: CheetahString,
        dest_group: CheetahString,
        topic: CheetahString,
        is_offline: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let retry_topic: CheetahString = mix_all::get_retry_topic(src_group.as_str()).into();
        let topic_route_data = self
            .examine_topic_route_info(retry_topic.clone())
            .await?
            .ok_or_else(|| mq_client_err!(format!("Topic route not found for retry topic: {retry_topic}")))?;
        let timeout = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;

        for broker_data in &topic_route_data.broker_datas {
            if let Some(addr) = broker_data.select_broker_addr() {
                api.clone_group_offset(
                    &addr,
                    src_group.clone(),
                    dest_group.clone(),
                    topic.clone(),
                    is_offline,
                    timeout,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn get_cluster_list(&self, topic: String) -> rocketmq_error::RocketMQResult<HashSet<CheetahString>> {
        self.get_topic_cluster_list(topic).await
    }

    async fn get_topic_cluster_list(&self, topic: String) -> rocketmq_error::RocketMQResult<HashSet<CheetahString>> {
        let cluster_info = self.examine_broker_cluster_info().await?;
        let topic_route_data = self
            .examine_topic_route_info(topic.clone().into())
            .await?
            .ok_or_else(|| mq_client_err!(format!("Topic route not found for: {topic}")))?;
        let broker_data = topic_route_data
            .broker_datas
            .first()
            .ok_or_else(|| mq_client_err!("Broker datas is empty"))?;
        let mut cluster_set = HashSet::new();
        let broker_name = broker_data.broker_name();
        if let Some(cluster_addr_table) = cluster_info.cluster_addr_table.as_ref() {
            cluster_set.extend(
                cluster_addr_table
                    .iter()
                    .filter(|(cluster_name, broker_names)| broker_names.contains(broker_name))
                    .map(|(cluster_name, broker_names)| cluster_name.clone()),
            );
        }
        Ok(cluster_set)
    }

    async fn get_all_topic_config(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        self.mq_client_api()?
            .get_all_topic_config(&broker_addr, timeout_millis)
            .await
    }

    async fn get_user_topic_config(
        &self,
        broker_addr: CheetahString,
        special_topic: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<TopicConfigSerializeWrapper> {
        let mut topic_config_wrapper = self.get_all_topic_config(broker_addr.clone(), timeout_millis).await?;
        let system_topic_list = self.get_system_topic_list_from_broker(broker_addr).await?;

        if let Some(ref mut topic_table) = topic_config_wrapper.topic_config_table_mut() {
            retain_java_user_topic_config(topic_table, &system_topic_list.topic_list, special_topic);
        }

        Ok(topic_config_wrapper)
    }

    async fn update_consume_offset(
        &self,
        broker_addr: CheetahString,
        consume_group: CheetahString,
        mq: MessageQueue,
        offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = update_consume_offset_request_header(consume_group, &mq, offset)?;
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;

        client_instance
            .get_mq_client_api_impl()?
            .update_consumer_offset(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        name_servers: Option<Vec<CheetahString>>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .update_name_server_config(properties, name_servers, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_name_server_config(
        &self,
        name_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        Ok(self
            .mq_client_api()?
            .get_name_server_config(Some(name_servers), self.timeout_millis)
            .await?
            .unwrap_or_default())
    }

    async fn probe_name_server(&self, name_server: CheetahString) -> rocketmq_error::RocketMQResult<()> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_mq_client_api_impl()?
            .probe_name_server(&name_server, self.timeout_millis)
            .await
    }

    async fn resume_check_half_message(
        &self,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        let message = self
            .query_message(CheetahString::default(), topic.clone(), msg_id.clone())
            .await?;
        let broker_addr = CheetahString::from_string(message.store_host().to_string());
        let broker_msg_id = if message
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            ))
            .is_some()
        {
            message.msg_id().clone()
        } else {
            msg_id
        };

        self.mq_client_api()?
            .resume_check_half_message(&broker_addr, topic, broker_msg_id, self.remoting_timeout_millis()?)
            .await
    }

    async fn set_message_request_mode(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        mode: MessageRequestMode,
        pop_work_group_size: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mq_client_api = self.mq_client_api()?;
        match mq_client_api
            .set_message_request_mode(
                &broker_addr,
                &topic,
                &consumer_group,
                mode,
                pop_work_group_size,
                timeout_millis,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn reset_offset_by_queue_id(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        reset_offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let (update_header, reset_header) =
            reset_offset_by_queue_id_request_headers(consumer_group, topic_name, queue_id, reset_offset)?;
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;
        let timeout_millis = self.remoting_timeout_millis()?;

        client_instance
            .get_mq_client_api_impl()?
            .update_consumer_offset(&broker_addr, update_header, timeout_millis)
            .await?;

        let offsets = client_instance
            .get_mq_client_api_impl()?
            .invoke_broker_to_reset_offset(&broker_addr, reset_header, timeout_millis)
            .await?;

        for (mq, old_offset) in offsets {
            info!(
                "Reset single message queue {} offset from {} to {}",
                mq, old_offset, reset_offset
            );
        }

        Ok(())
    }

    async fn examine_topic_config(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_topic_config(&addr, topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_static_topic(
        &self,
        addr: CheetahString,
        default_topic: CheetahString,
        topic_config: TopicConfig,
        mapping_detail: TopicQueueMappingDetail,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .create_static_topic(
                &addr,
                default_topic,
                topic_config,
                mapping_detail,
                force,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn get_controller_meta_data(
        &self,
        controller_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetMetaDataResponseHeader> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_controller_metadata(controller_addr, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn reset_master_flush_offset(
        &self,
        broker_addr: CheetahString,
        master_flush_offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let master_flush_offset = master_flush_offset_to_java_long(master_flush_offset)?;
            mq_client_instance
                .get_mq_client_api_impl()?
                .reset_master_flush_offset(&broker_addr, master_flush_offset)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_controller_config(
        &self,
        controller_servers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mut result: HashMap<CheetahString, HashMap<CheetahString, CheetahString>> = HashMap::new();
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let controller_servers =
                controller_servers_or_namesrv(controller_servers, &mq_client_api.get_name_server_address_list());

            for controller_addr in controller_servers {
                let config = mq_client_api
                    .get_controller_config(controller_addr.clone(), timeout_millis)
                    .await?;
                result.insert(controller_addr, config);
            }

            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_controller_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        controllers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .update_controller_config(properties, controllers, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn clean_controller_broker_data(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_controller_ids_to_clean: Option<CheetahString>,
        is_clean_living_broker: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .clean_controller_broker_data(
                    controller_addr,
                    cluster_name,
                    broker_name,
                    broker_controller_ids_to_clean,
                    is_clean_living_broker,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        properties: HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .update_cold_data_flow_ctr_group_config(broker_addr, properties, self.remoting_timeout_millis()?)
            .await
    }

    async fn remove_cold_data_flow_ctr_group_config(
        &self,
        broker_addr: CheetahString,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .remove_cold_data_flow_ctr_group_config(broker_addr, consumer_group, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_cold_data_flow_ctr_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.mq_client_api()?
            .get_cold_data_flow_ctr_info(broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn set_commit_log_read_ahead_mode(
        &self,
        broker_addr: CheetahString,
        mode: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.mq_client_api()?
            .set_commit_log_read_ahead_mode(broker_addr, mode, self.remoting_timeout_millis()?)
            .await
    }

    async fn create_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let user_info = UserInfo {
            username: Some(username),
            user_type: Some(user_type),
            password: Some(password),
            user_status: None,
        };

        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            mq_client_api
                .create_user(broker_addr, &user_info, timeout_millis)
                .await?;
            Ok(())
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
        user_type: CheetahString,
        user_status: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut user_info = UserInfo {
            username: Some(username),
            user_type: Some(user_type),
            password: Some(password),
            user_status: Some(user_status),
        };

        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            mq_client_api
                .update_user(broker_addr, &user_info, timeout_millis)
                .await?;
            Ok(())
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn delete_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            mq_client_api.delete_user(broker_addr, username, timeout_millis).await?;
            Ok(())
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let acl_info = build_acl_info(subject, resources, actions, source_ips, decision);
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .create_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn update_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resources: Vec<CheetahString>,
        actions: Vec<CheetahString>,
        source_ips: Vec<CheetahString>,
        decision: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let acl_info = build_acl_info(subject, resources, actions, source_ips, decision);
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .update_acl(broker_addr, &acl_info, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn delete_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
        resource: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref client_instance) = self.client_instance {
            let mq_client_api = client_instance.get_mq_client_api_impl()?;
            mq_client_api
                .delete_acl(broker_addr, subject, resource, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        queue_num: i32,
        topic_sys_flag: i32,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let config = lite_pull_topic_config(
            topic,
            queue_num,
            topic_sys_flag,
            read_queue_nums,
            write_queue_nums,
            false,
        )?;
        self.create_and_update_topic_config(addr, config).await
    }

    async fn update_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        read_queue_nums: i32,
        write_queue_nums: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let config = lite_pull_topic_config(topic, 0, 0, read_queue_nums, write_queue_nums, true)?;
        self.create_and_update_topic_config(addr, config).await
    }

    async fn get_lite_pull_topic(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        let lite_info = self
            .mq_client_api()?
            .get_broker_lite_info(&addr, self.remoting_timeout_millis()?)
            .await?;
        if !lite_info.get_topic_meta().contains_key(&topic) {
            return Err(mq_client_err!(format!("Lite pull topic not found: {topic}")));
        }
        self.examine_topic_config(addr, topic).await
    }

    async fn delete_lite_pull_topic(
        &self,
        addr: CheetahString,
        cluster_name: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.delete_topic_in_broker(HashSet::from([addr]), topic.clone())
            .await?;
        if cluster_name.is_empty() {
            return Ok(());
        }
        let namesrv_addrs = self.get_name_server_address_list().await.into_iter().collect();
        self.delete_topic_in_name_server(namesrv_addrs, Some(cluster_name), topic)
            .await
    }

    async fn query_lite_pull_topic_list(&self, addr: CheetahString) -> rocketmq_error::RocketMQResult<TopicList> {
        let lite_info = self
            .mq_client_api()?
            .get_broker_lite_info(&addr, self.remoting_timeout_millis()?)
            .await?;
        Ok(lite_topic_list_from_broker_lite_info(Some(addr), &lite_info))
    }

    async fn query_lite_pull_topic_by_cluster(
        &self,
        cluster_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        let timeout_millis = self.remoting_timeout_millis()?;
        let api = self.mq_client_api()?;
        let cluster_info = api.get_broker_cluster_info(timeout_millis).await?;
        let mut topic_names = HashSet::new();

        for broker_addr in broker_addrs_for_cluster(&cluster_info, &cluster_name) {
            let lite_info = api.get_broker_lite_info(&broker_addr, timeout_millis).await?;
            topic_names.extend(lite_info.get_topic_meta().keys().cloned());
        }

        Ok(topic_list_from_lite_topic_names(None, topic_names))
    }

    async fn query_lite_pull_subscription_list(
        &self,
        addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GroupList> {
        let lite_info = self
            .mq_client_api()?
            .get_broker_lite_info(&addr, self.remoting_timeout_millis()?)
            .await?;
        Ok(lite_subscription_group_list_from_broker_lite_info(&topic, &lite_info))
    }

    async fn update_lite_pull_consumer_offset(
        &self,
        addr: CheetahString,
        topic: CheetahString,
        group: CheetahString,
        queue_id: i32,
        offset: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = lite_pull_update_consumer_offset_request_header(topic, group, queue_id, offset)?;
        self.mq_client_api()?
            .update_consumer_offset(&addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_consume_stats_with_queue(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        queue_id: Option<i32>,
    ) -> rocketmq_error::RocketMQResult<ConsumeStats> {
        let mut stats = self
            .examine_consume_stats(consumer_group, topic.clone(), None, None, None)
            .await?;
        filter_consume_stats(&mut stats, topic.as_ref(), queue_id);
        Ok(stats)
    }

    async fn examine_consume_stats_concurrent(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        match self
            .examine_consume_stats(consumer_group, topic, None, None, None)
            .await
        {
            Ok(stats) => AdminToolResult::success(stats),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }

    async fn examine_consume_stats_concurrent_with_cluster(
        &self,
        consumer_group: CheetahString,
        topic: Option<CheetahString>,
        cluster_name: Option<CheetahString>,
    ) -> AdminToolResult<ConsumeStats> {
        match self
            .examine_consume_stats(consumer_group, topic, cluster_name, None, None)
            .await
        {
            Ok(stats) => AdminToolResult::success(stats),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }

    async fn export_rocksdb_consumer_offset_to_json(
        &self,
        broker_addr: CheetahString,
        file_path: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if !file_path.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "exportRocksDB consumerOffsets filePath is local-mode only and cannot be sent over RPC",
            ));
        }

        self.mq_client_api()?
            .export_rocksdb_config_to_json(
                broker_addr,
                vec![CheetahString::from_static_str(ROCKSDB_CONFIG_TYPE_CONSUMER_OFFSETS)],
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn export_rocksdb_consumer_offset_from_memory(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        self.mq_client_api()?
            .get_all_consumer_offset_json(broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn sync_broker_member_group(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        if !controller_addr.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "syncBrokerMemberGroup uses NameServer; controllerAddr is not supported by this facade",
            ));
        }

        self.mq_client_api()?
            .sync_broker_member_group(&cluster_name, &broker_name, false)
            .await?;
        Ok(())
    }

    async fn get_topic_config_by_topic_name(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicConfig> {
        self.client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?
            .get_topic_config(&broker_addr, topic_name, self.remoting_timeout_millis()?)
            .await
    }

    async fn notify_min_broker_id_changed(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        min_broker_id: u64,
        min_broker_addr: CheetahString,
        offline_broker_addr: Option<CheetahString>,
        ha_broker_addr: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = notify_min_broker_id_change_request_header(
            min_broker_id,
            min_broker_addr,
            offline_broker_addr,
            ha_broker_addr,
        )?;
        let mq_client_api = self.mq_client_api()?;
        let broker_member_group = mq_client_api
            .sync_broker_member_group(&cluster_name, &broker_name, false)
            .await?
            .unwrap_or_else(|| BrokerMemberGroup::new(cluster_name.clone(), broker_name.clone()));
        let broker_addrs = choose_min_broker_notify_addrs(
            &broker_member_group.broker_addrs,
            min_broker_id,
            request_header.offline_broker_addr.as_ref(),
        );

        if broker_addrs.is_empty() {
            return Err(RocketMQError::illegal_argument(format!(
                "notifyMinBrokerIdChanged cannot resolve broker addresses for cluster `{}` broker `{}`",
                cluster_name, broker_name
            )));
        }

        for broker_addr in broker_addrs {
            mq_client_api
                .notify_min_broker_id_changed(&broker_addr, request_header.clone(), 300)
                .await?;
        }
        Ok(())
    }

    async fn get_topic_stats_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicStatsTable> {
        self.mq_client_api()?
            .get_topic_stats_info(
                &broker_addr,
                GetTopicStatsInfoRequestHeader {
                    topic,
                    topic_request_header: None,
                },
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn query_broker_has_topic(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<bool> {
        map_topic_config_lookup_result(self.get_topic_config_by_topic_name(broker_addr, topic).await)
    }

    async fn get_system_topic_list_from_broker(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<TopicList> {
        self.mq_client_api()?
            .get_system_topic_list_from_broker(&broker_addr, self.remoting_timeout_millis()?)
            .await
    }

    async fn examine_topic_route_info_with_timeout(
        &self,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.mq_client_api()?
            .get_topic_route_info_from_name_server(&topic, timeout_millis)
            .await
    }

    async fn export_pop_records(&self, broker_addr: CheetahString, timeout: u64) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?.export_pop_record(broker_addr, timeout).await
    }

    async fn switch_timer_engine(
        &self,
        broker_addr: CheetahString,
        des_timer_engine: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .switch_timer_engine(&broker_addr, des_timer_engine, self.remoting_timeout_millis()?)
            .await
    }

    async fn trigger_lite_dispatch(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.mq_client_api()?
            .trigger_lite_dispatch(&broker_addr, group, client_id, self.remoting_timeout_millis()?)
            .await
    }
    #[allow(deprecated)]
    async fn delete_topic_in_broker_concurrent(
        &self,
        addrs: HashSet<CheetahString>,
        topic: CheetahString,
    ) -> AdminToolResult<BrokerOperatorResult> {
        let api = match self.mq_client_api() {
            Ok(api) => api,
            Err(error) => {
                return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string());
            }
        };
        let timeout = match self.remoting_timeout_millis() {
            Ok(timeout) => timeout,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let mut success_list = Vec::new();
        let mut failure_list = Vec::new();

        for addr in addrs {
            let request_header = DeleteTopicRequestHeader {
                topic: topic.clone(),
                topic_request_header: None,
            };
            match api.delete_topic_in_broker(&addr, request_header, timeout).await {
                Ok(()) => success_list.push(addr),
                Err(error) => {
                    warn!("deleteTopicInBroker error. topic={}, broker={}, {}", topic, addr, error);
                    failure_list.push(addr);
                }
            }
        }

        AdminToolResult::success(broker_operator_result(success_list, failure_list))
    }

    async fn reset_offset_by_timestamp_old(
        &self,
        cluster_name: Option<CheetahString>,
        consumer_group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
        force: bool,
    ) -> rocketmq_error::RocketMQResult<Vec<RollbackStats>> {
        let timestamp = timestamp_to_java_long("resetOffsetByTimestampOld", timestamp)?;
        let mut route_topic = topic.clone();
        if !topic.is_empty()
            && (mix_all::is_lmq(Some(topic.as_str()))
                || topic.as_str() == format!("{}wheel_timer", TopicValidator::SYSTEM_TOPIC_PREFIX))
            && cluster_name.as_ref().is_some_and(|name| !name.is_empty())
        {
            if let Some(cluster_name) = cluster_name {
                route_topic = cluster_name;
            }
        }
        let topic_route_data = self.examine_topic_route_info(route_topic).await?;
        let mut rollback_stats_list = Vec::new();

        if let Some(route_data) = topic_route_data {
            let mut topic_route_map = HashMap::new();
            for queue_data in &route_data.queue_datas {
                topic_route_map.insert(queue_data.broker_name().to_string(), queue_data.clone());
            }

            for broker_data in &route_data.broker_datas {
                if let Some(addr) = broker_data.select_broker_addr() {
                    if let Some(queue_data) = topic_route_map.get(broker_data.broker_name().as_str()) {
                        let mut rollback_stats = self
                            .reset_offset_by_timestamp_old_on_broker(
                                addr,
                                queue_data,
                                consumer_group.clone(),
                                topic.clone(),
                                timestamp,
                                force,
                            )
                            .await?;
                        rollback_stats_list.append(&mut rollback_stats);
                    }
                }
            }
        }

        Ok(rollback_stats_list)
    }
    #[allow(deprecated)]
    async fn reset_offset_new_concurrent(
        &self,
        group: CheetahString,
        topic: CheetahString,
        timestamp: u64,
    ) -> AdminToolResult<BrokerOperatorResult> {
        let timestamp = match timestamp_to_java_long("resetOffsetNewConcurrent", timestamp) {
            Ok(timestamp) => timestamp,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let api = match self.mq_client_api() {
            Ok(api) => api,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let timeout = match self.remoting_timeout_millis() {
            Ok(timeout) => timeout,
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };
        let route_data = match api.get_topic_route_info_from_name_server(&topic, timeout).await {
            Ok(Some(route_data)) if !route_data.broker_datas.is_empty() => route_data,
            Ok(_) => {
                return AdminToolResult::failure(
                    AdminToolsResultCodeEnum::TopicRouteInfoNotExist,
                    "topic router info not found".to_string(),
                );
            }
            Err(error) => return AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        };

        let mut topic_route_map = HashMap::new();
        for queue_data in &route_data.queue_datas {
            topic_route_map.insert(queue_data.broker_name().clone(), queue_data.clone());
        }

        let mut success_list = Vec::new();
        let mut failure_list = Vec::new();
        for broker_data in &route_data.broker_datas {
            let Some(addr) = broker_data.select_broker_addr() else {
                continue;
            };

            let reset_header = ResetOffsetRequestHeader {
                topic: topic.clone(),
                group: group.clone(),
                queue_id: -1,
                offset: Some(-1),
                timestamp,
                is_force: true,
                topic_request_header: None,
            };

            match api.invoke_broker_to_reset_offset(&addr, reset_header, timeout).await {
                Ok(_) => success_list.push(addr),
                Err(error) if is_consumer_not_online_error(&error) => {
                    match topic_route_map.get(broker_data.broker_name()) {
                        Some(queue_data) => {
                            match self
                                .reset_offset_by_timestamp_old_on_broker(
                                    addr.clone(),
                                    queue_data,
                                    group.clone(),
                                    topic.clone(),
                                    timestamp,
                                    true,
                                )
                                .await
                            {
                                Ok(_) => success_list.push(addr),
                                Err(error) => {
                                    warn!(
                                        "resetOffsetByTimestampOld error. addr={}, topic={}, group={}, timestamp={}, \
                                         {}",
                                        addr, topic, group, timestamp, error
                                    );
                                    failure_list.push(addr);
                                }
                            }
                        }
                        None => {
                            warn!(
                                "resetOffsetByTimestampOld error. addr={}, topic={}, group={}, timestamp={}, missing \
                                 queue data for broker {}",
                                addr,
                                topic,
                                group,
                                timestamp,
                                broker_data.broker_name()
                            );
                            failure_list.push(addr);
                        }
                    }
                }
                Err(error) if response_code_from_error(&error) == Some(ResponseCode::SystemError) => {
                    success_list.push(addr);
                }
                Err(error) => {
                    warn!(
                        "resetOffsetNewConcurrent error. addr={}, topic={}, group={}, timestamp={}, {}",
                        addr, topic, group, timestamp, error
                    );
                    failure_list.push(addr);
                }
            }
        }

        let success = success_list.len() == route_data.broker_datas.len();
        let result = broker_operator_result(success_list, failure_list);
        if success {
            AdminToolResult::success(result)
        } else {
            AdminToolResult::failure_with_data(
                AdminToolsResultCodeEnum::MQBrokerError,
                "operator failure".into(),
                result,
            )
        }
    }

    async fn query_consume_time_span(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<QueueTimeSpan>> {
        let timeout = self.remoting_timeout_millis()?;
        let mut result = Vec::new();
        if let Some(route_data) = self.examine_topic_route_info(topic.clone()).await? {
            for broker_data in &route_data.broker_datas {
                if let Some(master_addr) = broker_data.broker_addrs().get(&mix_all::MASTER_ID) {
                    let spans = self
                        .mq_client_api()?
                        .query_consume_time_span(
                            master_addr,
                            QueryConsumeTimeSpanRequestHeader {
                                topic: topic.clone(),
                                group: group.clone(),
                                topic_request_header: None,
                            },
                            timeout,
                        )
                        .await?;
                    result.extend(spans);
                }
            }
        }
        Ok(result)
    }

    async fn query_consume_time_span_concurrent(
        &self,
        topic: CheetahString,
        group: CheetahString,
    ) -> AdminToolResult<Vec<QueueTimeSpan>> {
        match self.query_consume_time_span(topic, group).await {
            Ok(spans) => AdminToolResult::success(spans),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }
    #[allow(deprecated)]
    async fn message_track_detail(&self, msg: MessageExt) -> rocketmq_error::RocketMQResult<Vec<MessageTrack>> {
        let group_list = self.query_topic_consume_by_who(msg.topic().clone()).await?;
        let mut result = Vec::with_capacity(group_list.get_group_list().len());

        for group in group_list.get_group_list() {
            let mut track = build_message_track(group.as_str());
            let consumer_connection = match self.examine_consumer_connection_info(group.clone(), None).await {
                Ok(connection) => connection,
                Err(error) => {
                    apply_track_error(&mut track, &error);
                    result.push(track);
                    continue;
                }
            };

            match consumer_connection.get_consume_type() {
                Some(ConsumeType::ConsumeActively) => {
                    track.set_track_type(TrackType::Pull);
                }
                Some(ConsumeType::ConsumePassively) => {
                    if consumer_connection.get_message_model() == Some(MessageModel::Broadcasting) {
                        track.set_track_type(TrackType::ConsumeBroadcasting);
                        result.push(track);
                        continue;
                    }

                    let consumed = match self.message_consumed_by_group(&msg, group).await {
                        Ok(consumed) => consumed,
                        Err(error) => {
                            apply_track_error(&mut track, &error);
                            result.push(track);
                            continue;
                        }
                    };

                    if consumed {
                        track.set_track_type(resolve_consumed_track_type(&msg, &consumer_connection));
                    } else {
                        track.set_track_type(TrackType::NotConsumedYet);
                    }
                }
                _ => {}
            }

            result.push(track);
        }

        result.sort_by(|left, right| left.consumer_group.cmp(&right.consumer_group));
        Ok(result)
    }
    #[allow(deprecated)]
    async fn message_track_detail_concurrent(&self, msg: MessageExt) -> AdminToolResult<Vec<MessageTrack>> {
        match self.message_track_detail(msg).await {
            Ok(data) => AdminToolResult::success(data),
            Err(error) => AdminToolResult::failure(admin_result_code_for_error(&error), error.to_string()),
        }
    }

    async fn view_broker_stats_data(
        &self,
        broker_addr: CheetahString,
        stats_name: CheetahString,
        stats_key: CheetahString,
    ) -> rocketmq_error::RocketMQResult<BrokerStatsData> {
        let request_header = ViewBrokerStatsDataRequestHeader { stats_name, stats_key };
        self.mq_client_api()?
            .view_broker_stats_data(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn fetch_consume_stats_in_broker(
        &self,
        broker_addr: CheetahString,
        is_order: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ConsumeStatsList> {
        self.mq_client_api()?
            .fetch_consume_stats_in_broker(&broker_addr, GetConsumeStatsInBrokerHeader { is_order }, timeout_millis)
            .await
    }

    async fn get_all_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupWrapper> {
        self.mq_client_api()?
            .get_all_subscription_group_config(&broker_addr, timeout_millis)
            .await
    }

    async fn get_user_subscription_group(
        &self,
        broker_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SubscriptionGroupWrapper> {
        let mut subscription_group_wrapper = self.get_all_subscription_group(broker_addr, timeout_millis).await?;

        let system_group_set = get_system_group_set();
        let table = subscription_group_wrapper.get_subscription_group_table_mut();
        // Remove system consumer groups
        table.retain(|key, _| !mix_all::is_sys_consumer_group(key.as_str()) && !system_group_set.contains(key));

        Ok(subscription_group_wrapper)
    }

    async fn query_consume_queue(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        index: u64,
        count: i32,
        consumer_group: CheetahString,
    ) -> rocketmq_error::RocketMQResult<QueryConsumeQueueResponseBody> {
        let index = query_consume_queue_index_to_java_long(index)?;
        self.mq_client_api()?
            .query_consume_queue(
                &broker_addr,
                topic,
                queue_id,
                index,
                count,
                consumer_group,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn update_and_get_group_read_forbidden(
        &self,
        broker_addr: CheetahString,
        group_name: CheetahString,
        topic_name: CheetahString,
        readable: Option<bool>,
    ) -> rocketmq_error::RocketMQResult<GroupForbidden> {
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;
        let request_header = update_group_forbidden_request_header(group_name, topic_name, readable);

        client_instance
            .get_mq_client_api_impl()?
            .update_and_get_group_forbidden(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn query_message(
        &self,
        _cluster_name: CheetahString,
        topic: CheetahString,
        msg_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::ClientNotStarted)?;

        let msg_id_str = msg_id.as_str();

        if let Err(e) = MessageDecoder::validate_message_id(msg_id_str) {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(format!(
                "Invalid message ID: {}",
                e
            )));
        }

        let message_id = MessageDecoder::decode_message_id(msg_id_str).map_err(|e| {
            rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {}", e))
        })?;
        let broker_addr =
            CheetahString::from_string(format!("{}:{}", message_id.address.ip(), message_id.address.port()));

        let request_header = ViewMessageRequestHeader {
            topic: Some(topic),
            offset: message_id.offset,
        };

        client_instance
            .get_mq_client_api_impl()?
            .view_message(&broker_addr, request_header, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_broker_ha_status(&self, broker_addr: CheetahString) -> rocketmq_error::RocketMQResult<HARuntimeInfo> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_ha_status(broker_addr, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_in_sync_state_data(
        &self,
        controller_address: CheetahString,
        brokers: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerReplicasInfo> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_in_sync_state_data(controller_address, brokers, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_broker_epoch_cache(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<EpochEntryCache> {
        if let Some(ref mq_client_instance) = self.client_instance {
            Ok(mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_epoch_cache(broker_addr, self.remoting_timeout_millis()?)
                .await?)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn elect_master(
        &self,
        controller_addr: CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: Option<u64>,
    ) -> rocketmq_error::RocketMQResult<(ElectMasterResponseHeader, BrokerMemberGroup)> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .elect_master(
                    controller_addr,
                    cluster_name,
                    broker_name,
                    broker_id,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let user_info = UserInfo {
            username: Some(username),
            password: Some(password),
            user_type: None,
            user_status: None,
        };

        self.mq_client_api()?
            .create_user(broker_addr, &user_info, self.remoting_timeout_millis()?)
            .await
    }

    async fn update_user_with_info(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
        password: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let user_info = UserInfo {
            username: Some(username),
            password: Some(password),
            user_type: None,
            user_status: None,
        };

        self.mq_client_api()?
            .update_user(broker_addr, &user_info, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_user(
        &self,
        broker_addr: CheetahString,
        username: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<UserInfo>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let result = mq_client_api.get_user(broker_addr, username, timeout_millis).await?;
            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn list_users(
        &self,
        broker_addr: CheetahString,
        filter: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<UserInfo>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let result = mq_client_api.list_users(broker_addr, filter, timeout_millis).await?;
            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn create_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.create_acl_with_acl_info(
            broker_addr,
            AclInfo {
                subject: Some(subject),
                policies: None,
            },
        )
        .await
    }

    async fn update_acl_with_info(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.update_acl_with_acl_info(
            broker_addr,
            AclInfo {
                subject: Some(subject),
                policies: None,
            },
        )
        .await
    }

    async fn get_acl(
        &self,
        broker_addr: CheetahString,
        subject: CheetahString,
    ) -> rocketmq_error::RocketMQResult<AclInfo> {
        let acl_infos = self
            .list_acl(broker_addr.clone(), subject.clone(), CheetahString::default())
            .await?;
        acl_infos
            .into_iter()
            .find(|acl_info| acl_info.subject.as_ref() == Some(&subject))
            .ok_or_else(|| {
                RocketMQError::illegal_argument(format!(
                    "ACL with subject {} was not found on broker {}",
                    subject, broker_addr
                ))
            })
    }

    async fn list_acl(
        &self,
        broker_addr: CheetahString,
        subject_filter: CheetahString,
        resource_filter: CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<AclInfo>> {
        if let Some(ref mq_client_instance) = self.client_instance {
            let mq_client_api = mq_client_instance.get_mq_client_api_impl()?;
            let timeout_millis = self.remoting_timeout_millis()?;
            let result = mq_client_api
                .list_acl(broker_addr, subject_filter, resource_filter, timeout_millis)
                .await?;
            Ok(result)
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_broker_lite_info(
        &self,
        broker_addr: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetBrokerLiteInfoResponseBody> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .get_broker_lite_info(&broker_addr, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_parent_topic_info(
        &self,
        broker_addr: CheetahString,
        topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetParentTopicInfoResponseBody> {
        self.mq_client_api()?
            .get_parent_topic_info(&broker_addr, topic, self.remoting_timeout_millis()?)
            .await
    }

    async fn get_lite_topic_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        lite_topic: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteTopicInfoResponseBody> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .get_lite_topic_info(
                    &broker_addr,
                    &parent_topic,
                    &lite_topic,
                    self.remoting_timeout_millis()?,
                )
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn get_lite_client_info(
        &self,
        broker_addr: CheetahString,
        parent_topic: CheetahString,
        group: CheetahString,
        client_id: CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetLiteClientInfoResponseBody> {
        self.mq_client_api()?
            .get_lite_client_info(
                &broker_addr,
                parent_topic,
                group,
                client_id,
                self.remoting_timeout_millis()?,
            )
            .await
    }

    async fn get_lite_group_info(
        &self,
        broker_addr: CheetahString,
        group: CheetahString,
        lite_topic: CheetahString,
        top_k: i32,
    ) -> rocketmq_error::RocketMQResult<GetLiteGroupInfoResponseBody> {
        self.mq_client_api()?
            .get_lite_group_info(&broker_addr, group, lite_topic, top_k, self.remoting_timeout_millis()?)
            .await
    }

    async fn export_rocksdb_config_to_json(
        &self,
        broker_addr: CheetahString,
        config_types: Vec<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        if let Some(ref mq_client_instance) = self.client_instance {
            mq_client_instance
                .get_mq_client_api_impl()?
                .export_rocksdb_config_to_json(broker_addr, config_types, self.remoting_timeout_millis()?)
                .await
        } else {
            Err(rocketmq_error::RocketMQError::ClientNotStarted)
        }
    }

    async fn search_offset(
        &self,
        broker_addr: CheetahString,
        topic_name: CheetahString,
        queue_id: i32,
        timestamp: u64,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<u64> {
        let timestamp = search_offset_timestamp_to_java_long(timestamp)?;
        let mq = MessageQueue::from_parts(&topic_name, "", queue_id);
        let offset = self
            .mq_client_api()?
            .search_offset_by_timestamp(
                broker_addr.as_str(),
                &mq,
                timestamp,
                rocketmq_model::common::boundary_type::BoundaryType::Lower,
                timeout_millis,
            )
            .await?;
        java_long_to_u64("searchOffset", "offset", offset)
    }

    async fn min_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.mq_client_api()?
            .get_min_offset(broker_addr.as_str(), &message_queue, timeout_millis)
            .await
    }

    async fn max_offset(
        &self,
        broker_addr: CheetahString,
        message_queue: MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.mq_client_api()?
            .get_max_offset(broker_addr.as_str(), &message_queue, timeout_millis)
            .await
    }
}
