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

use super::*;
#[cfg(feature = "admin-mutation")]
use crate::admin::BrokerConfigPatchOutcome;
#[cfg(feature = "admin-mutation")]
use crate::admin::SubscriptionGroupConfigPatch;
#[cfg(feature = "admin-mutation")]
use crate::admin::SubscriptionGroupConfigPatchOutcome;
#[cfg(feature = "admin-read")]
use crate::admin::SubscriptionGroupConfigVersioned;
#[cfg(feature = "admin-mutation")]
use crate::admin::TopicConfigPatch;
#[cfg(feature = "admin-mutation")]
use crate::admin::TopicConfigPatchOutcome;
#[cfg(feature = "admin-read")]
use crate::admin::TopicConfigVersioned;
use rocketmq_protocol::protocol::header::get_broker_config_response_header::GetBrokerConfigResponseHeader;
#[cfg(feature = "admin-mutation")]
use rocketmq_protocol::protocol::header::update_broker_config_request_header::UpdateBrokerConfigRequestHeader;
#[cfg(feature = "admin-mutation")]
use rocketmq_protocol::protocol::header::update_broker_config_response_header::UpdateBrokerConfigResponseHeader;
#[cfg(feature = "admin-mutation")]
use rocketmq_protocol::protocol::header::update_subscription_group_config_cas_request_header::UpdateSubscriptionGroupConfigCasRequestHeader;
#[cfg(any(feature = "admin-read", feature = "admin-mutation"))]
use rocketmq_protocol::protocol::header::update_subscription_group_config_cas_response_header::UpdateSubscriptionGroupConfigCasResponseHeader;
#[cfg(feature = "admin-mutation")]
use rocketmq_protocol::protocol::header::update_topic_config_cas_request_header::UpdateTopicConfigCasRequestHeader;
#[cfg(any(feature = "admin-read", feature = "admin-mutation"))]
use rocketmq_protocol::protocol::header::update_topic_config_cas_response_header::UpdateTopicConfigCasResponseHeader;

pub(crate) struct BrokerConfigSnapshot {
    pub(crate) generation: Option<u64>,
    pub(crate) properties: HashMap<CheetahString, CheetahString>,
}

fn broker_config_snapshot_from_response(response: &RemotingCommand) -> RocketMQResult<BrokerConfigSnapshot> {
    let body = response
        .get_body()
        .ok_or_else(|| mq_client_err!("Broker config response body is empty".to_string()))?;
    let body_str = String::from_utf8_lossy(body.as_ref());
    let properties = mix_all::string_to_properties(body_str.as_ref())
        .ok_or_else(|| mq_client_err!("Failed to parse broker config response body".to_string()))?;
    let generation = response
        .decode_command_custom_header::<GetBrokerConfigResponseHeader>()
        .ok()
        .map(|header| header.config_generation)
        .filter(|generation| *generation > 0);
    Ok(BrokerConfigSnapshot { generation, properties })
}

#[cfg(feature = "admin-read")]
fn topic_config_versioned_from_response(response: &RemotingCommand) -> RocketMQResult<TopicConfigVersioned> {
    let body = response.get_body().ok_or(RocketMQError::ResponseProcessFailed {
        operation: "get_topic_config_with_version",
        reason: "Topic config response body is empty".to_owned(),
    })?;
    let mapping = serde_json::from_slice::<TopicConfigAndQueueMapping>(body.as_ref()).map_err(|error| {
        RocketMQError::ResponseProcessFailed {
            operation: "get_topic_config_with_version",
            reason: format!("Topic config response body is invalid: {error}"),
        }
    })?;
    let header = response
        .decode_command_custom_header::<UpdateTopicConfigCasResponseHeader>()
        .map_err(|error| RocketMQError::ResponseProcessFailed {
            operation: "get_topic_config_with_version",
            reason: format!("Topic config response version is missing: {error}"),
        })?;
    Ok(TopicConfigVersioned {
        version: header.topic_version,
        config: mapping.topic_config,
    })
}

#[cfg(feature = "admin-read")]
fn subscription_group_config_versioned_from_response(
    response: &RemotingCommand,
) -> RocketMQResult<SubscriptionGroupConfigVersioned> {
    let body = response.get_body().ok_or(RocketMQError::ResponseProcessFailed {
        operation: "get_subscription_group_config_with_version",
        reason: "Subscription Group config response body is empty".to_owned(),
    })?;
    let config = rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::decode(
        body.as_ref(),
    )
    .map_err(|error| RocketMQError::ResponseProcessFailed {
        operation: "get_subscription_group_config_with_version",
        reason: format!("Subscription Group config response body is invalid: {error}"),
    })?;
    let header = response
        .decode_command_custom_header::<UpdateSubscriptionGroupConfigCasResponseHeader>()
        .map_err(|error| RocketMQError::ResponseProcessFailed {
            operation: "get_subscription_group_config_with_version",
            reason: format!("Subscription Group config response version is missing: {error}"),
        })?;
    Ok(SubscriptionGroupConfigVersioned {
        version: header.subscription_group_version,
        config,
    })
}

#[cfg(feature = "admin-mutation")]
fn topic_config_patch_outcome_from_response(
    response: &RemotingCommand,
    expected_version: u64,
) -> RocketMQResult<TopicConfigPatchOutcome> {
    match ResponseCode::from(response.code()) {
        ResponseCode::Success => {
            let header = response
                .decode_command_custom_header::<UpdateTopicConfigCasResponseHeader>()
                .map_err(|error| RocketMQError::ResponseProcessFailed {
                    operation: "patch_topic_config_if_version",
                    reason: format!("missing committed Topic config version: {error}"),
                })?;
            let expected_next = expected_version
                .checked_add(1)
                .ok_or(RocketMQError::ResponseProcessFailed {
                    operation: "patch_topic_config_if_version",
                    reason: "Broker accepted a Topic patch after the version counter was exhausted".to_owned(),
                })?;
            if header.topic_version != expected_next {
                return Err(RocketMQError::ResponseProcessFailed {
                    operation: "patch_topic_config_if_version",
                    reason: format!(
                        "Broker returned Topic config version {}, expected {}",
                        header.topic_version, expected_next
                    ),
                });
            }
            Ok(TopicConfigPatchOutcome::Applied {
                previous_version: expected_version,
                version: header.topic_version,
            })
        }
        ResponseCode::InvalidParameter => {
            if let Ok(header) = response.decode_command_custom_header::<UpdateTopicConfigCasResponseHeader>() {
                return Ok(TopicConfigPatchOutcome::VersionConflict {
                    expected_version,
                    actual_version: header.topic_version,
                });
            }
            Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            ))
        }
        _ => Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |remark| remark.to_string())
        )),
    }
}

#[cfg(feature = "admin-mutation")]
fn subscription_group_config_patch_outcome_from_response(
    response: &RemotingCommand,
    expected_version: u64,
) -> RocketMQResult<SubscriptionGroupConfigPatchOutcome> {
    match ResponseCode::from(response.code()) {
        ResponseCode::Success => {
            let header = response
                .decode_command_custom_header::<UpdateSubscriptionGroupConfigCasResponseHeader>()
                .map_err(|error| RocketMQError::ResponseProcessFailed {
                    operation: "patch_subscription_group_config_if_version",
                    reason: format!("missing committed Subscription Group config version: {error}"),
                })?;
            let expected_next = expected_version
                .checked_add(1)
                .ok_or(RocketMQError::ResponseProcessFailed {
                    operation: "patch_subscription_group_config_if_version",
                    reason: "Broker accepted a Subscription Group patch after the version counter was exhausted"
                        .to_owned(),
                })?;
            if header.subscription_group_version != expected_next {
                return Err(RocketMQError::ResponseProcessFailed {
                    operation: "patch_subscription_group_config_if_version",
                    reason: format!(
                        "Broker returned Subscription Group config version {}, expected {}",
                        header.subscription_group_version, expected_next
                    ),
                });
            }
            Ok(SubscriptionGroupConfigPatchOutcome::Applied {
                previous_version: expected_version,
                version: header.subscription_group_version,
            })
        }
        ResponseCode::InvalidParameter => {
            if let Ok(header) =
                response.decode_command_custom_header::<UpdateSubscriptionGroupConfigCasResponseHeader>()
            {
                return Ok(SubscriptionGroupConfigPatchOutcome::VersionConflict {
                    expected_version,
                    actual_version: header.subscription_group_version,
                });
            }
            Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            ))
        }
        _ => Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |remark| remark.to_string())
        )),
    }
}

impl MQClientAPIImpl {
    #[cfg(feature = "admin-read")]
    pub(crate) async fn get_topic_config_with_version(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicConfigVersioned> {
        let request = RemotingCommand::create_request_command(
            RequestCode::GetTopicConfig,
            GetTopicConfigRequestHeader {
                topic,
                topic_request_header: None,
            },
        );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return topic_config_versioned_from_response(&response);
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |remark| remark.to_string())
        ))
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_broker_config_if_generation(
        &self,
        addr: &CheetahString,
        expected_generation: u64,
        properties: HashMap<CheetahString, CheetahString>,
        timeout_millis: u64,
    ) -> RocketMQResult<BrokerConfigPatchOutcome> {
        if expected_generation == 0 {
            return Err(RocketMQError::illegal_argument(
                "expected broker config generation must be greater than zero",
            ));
        }
        let validator_input = properties
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<String, String>>();
        crate::base::validators::Validators::check_broker_config(&validator_input)?;

        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "generation-checked broker config patch must not be empty",
            ));
        }

        let request = RemotingCommand::create_request_command(
            RequestCode::UpdateBrokerConfigCas,
            UpdateBrokerConfigRequestHeader { expected_generation },
        )
        .set_body(body.to_string());
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let header = response
                    .decode_command_custom_header::<UpdateBrokerConfigResponseHeader>()
                    .map_err(|error| RocketMQError::ResponseProcessFailed {
                        operation: "update_broker_config_if_generation",
                        reason: format!("missing committed config generation: {error}"),
                    })?;
                let expected_next = expected_generation
                    .checked_add(1)
                    .ok_or(RocketMQError::ResponseProcessFailed {
                        operation: "update_broker_config_if_generation",
                        reason: "broker accepted a patch after the generation counter was exhausted".to_string(),
                    })?;
                if header.config_generation != expected_next {
                    return Err(RocketMQError::ResponseProcessFailed {
                        operation: "update_broker_config_if_generation",
                        reason: format!(
                            "broker returned generation {}, expected {}",
                            header.config_generation, expected_next
                        ),
                    });
                }
                Ok(BrokerConfigPatchOutcome::Applied {
                    previous_generation: expected_generation,
                    generation: header.config_generation,
                })
            }
            ResponseCode::InvalidParameter => {
                if let Ok(header) = response.decode_command_custom_header::<UpdateBrokerConfigResponseHeader>() {
                    return Ok(BrokerConfigPatchOutcome::GenerationConflict {
                        expected_generation,
                        actual_generation: header.config_generation,
                    });
                }
                Err(mq_client_err!(
                    response.code(),
                    response.remark().map_or_else(String::new, |remark| remark.to_string())
                ))
            }
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_topic_config_if_version(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        expected_version: u64,
        patch: TopicConfigPatch,
        timeout_millis: u64,
    ) -> RocketMQResult<TopicConfigPatchOutcome> {
        if patch.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "version-checked Topic config patch must not be empty",
            ));
        }
        let read_queue_nums = patch
            .read_queue_nums
            .map(|value| {
                if !(1..=128).contains(&value) {
                    return Err(RocketMQError::illegal_argument(
                        "readQueueNums must be between 1 and 128",
                    ));
                }
                i32::try_from(value)
                    .map_err(|_| RocketMQError::illegal_argument("readQueueNums exceeds Java int range"))
            })
            .transpose()?;
        let write_queue_nums = patch
            .write_queue_nums
            .map(|value| {
                if !(1..=128).contains(&value) {
                    return Err(RocketMQError::illegal_argument(
                        "writeQueueNums must be between 1 and 128",
                    ));
                }
                i32::try_from(value)
                    .map_err(|_| RocketMQError::illegal_argument("writeQueueNums exceeds Java int range"))
            })
            .transpose()?;
        let request = RemotingCommand::create_request_command(
            RequestCode::UpdateTopicConfigCas,
            UpdateTopicConfigCasRequestHeader {
                topic,
                expected_version,
                read_queue_nums,
                write_queue_nums,
                order: patch.order,
            },
        );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        topic_config_patch_outcome_from_response(&response, expected_version)
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn update_subscription_group_config_if_version(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        expected_version: u64,
        patch: SubscriptionGroupConfigPatch,
        timeout_millis: u64,
    ) -> RocketMQResult<SubscriptionGroupConfigPatchOutcome> {
        if patch.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "version-checked Subscription Group config patch must not be empty",
            ));
        }
        let retry_max_times = patch
            .retry_max_times
            .map(|value| {
                if !(1..=16).contains(&value) {
                    return Err(RocketMQError::illegal_argument(
                        "retryMaxTimes must be between 1 and 16",
                    ));
                }
                i32::try_from(value)
                    .map_err(|_| RocketMQError::illegal_argument("retryMaxTimes exceeds Java int range"))
            })
            .transpose()?;
        let retry_queue_nums = patch
            .retry_queue_nums
            .map(|value| {
                if !(1..=8).contains(&value) {
                    return Err(RocketMQError::illegal_argument(
                        "retryQueueNums must be between 1 and 8",
                    ));
                }
                i32::try_from(value)
                    .map_err(|_| RocketMQError::illegal_argument("retryQueueNums exceeds Java int range"))
            })
            .transpose()?;
        let consume_timeout_minutes = patch
            .consume_timeout_minutes
            .map(|value| {
                if !(1..=1_440).contains(&value) {
                    return Err(RocketMQError::illegal_argument(
                        "consumeTimeoutMinutes must be between 1 and 1440",
                    ));
                }
                i32::try_from(value)
                    .map_err(|_| RocketMQError::illegal_argument("consumeTimeoutMinutes exceeds Java int range"))
            })
            .transpose()?;
        let request = RemotingCommand::create_request_command(
            RequestCode::UpdateSubscriptionGroupConfigCas,
            UpdateSubscriptionGroupConfigCasRequestHeader {
                group,
                expected_version,
                retry_max_times,
                retry_queue_nums,
                consume_timeout_minutes,
            },
        );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        subscription_group_config_patch_outcome_from_response(&response, expected_version)
    }

    pub(crate) async fn get_broker_config_snapshot(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<BrokerConfigSnapshot> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => broker_config_snapshot_from_response(&response),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-read")]
    pub(crate) async fn get_subscription_group_config_with_version(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<SubscriptionGroupConfigVersioned> {
        let request = RemotingCommand::create_request_command(
            RequestCode::GetSubscriptionGroupConfig,
            rocketmq_protocol::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader {
                group,
                rpc_request_header: None,
            },
        );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let response = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return subscription_group_config_versioned_from_response(&response);
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |remark| remark.to_string())
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_bound_to_the_same_allowlisted_response_body() {
        let mut response = RemotingCommand::create_response_command()
            .set_code(ResponseCode::Success)
            .set_command_custom_header(GetBrokerConfigResponseHeader {
                version: Some("{\"stateVersion\":0,\"timestamp\":6,\"counter\":7}".into()),
                config_generation: 7,
            })
            .set_body("sendMessageThreadPoolNums=32\naccessKey=must-not-escape");
        response.make_custom_header_to_net();

        let snapshot = broker_config_snapshot_from_response(&response).expect("snapshot");
        assert_eq!(snapshot.generation, Some(7));
        assert_eq!(
            snapshot
                .properties
                .get("sendMessageThreadPoolNums")
                .map(CheetahString::as_str),
            Some("32")
        );
    }

    #[test]
    fn legacy_response_remains_readable_without_claiming_a_generation() {
        let response = RemotingCommand::create_response_command()
            .set_code(ResponseCode::Success)
            .set_body("sendMessageThreadPoolNums=32");

        let snapshot = broker_config_snapshot_from_response(&response).expect("legacy snapshot");
        assert_eq!(snapshot.generation, None);
    }

    #[cfg(feature = "admin-read")]
    #[test]
    fn topic_config_response_binds_body_to_version() {
        let config = TopicConfig::with_queues("orders", 4, 6);
        let mut response = RemotingCommand::create_response_command()
            .set_code(ResponseCode::Success)
            .set_command_custom_header(UpdateTopicConfigCasResponseHeader { topic_version: 9 })
            .set_body(
                serde_json::to_vec(&TopicConfigAndQueueMapping::new(config.clone(), None))
                    .expect("Topic response body"),
            );
        response.make_custom_header_to_net();

        let snapshot = topic_config_versioned_from_response(&response).expect("versioned Topic config");
        assert_eq!(snapshot.version, 9);
        assert_eq!(snapshot.config, config);
    }

    #[cfg(feature = "admin-read")]
    #[test]
    fn subscription_group_config_response_binds_body_to_version() {
        let config = rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig::new(
            "orders-consumer".into(),
        );
        let mut response = RemotingCommand::create_response_command()
            .set_code(ResponseCode::Success)
            .set_command_custom_header(UpdateSubscriptionGroupConfigCasResponseHeader {
                subscription_group_version: 9,
            })
            .set_body(config.encode().expect("Subscription Group response body"));
        response.make_custom_header_to_net();

        let snapshot =
            subscription_group_config_versioned_from_response(&response).expect("versioned Subscription Group config");
        assert_eq!(snapshot.version, 9);
        assert_eq!(snapshot.config.group_name(), config.group_name());
    }

    #[cfg(feature = "admin-mutation")]
    #[test]
    fn topic_config_patch_response_distinguishes_commit_and_conflict() {
        let mut applied = RemotingCommand::create_response_command()
            .set_code(ResponseCode::Success)
            .set_command_custom_header(UpdateTopicConfigCasResponseHeader { topic_version: 10 });
        applied.make_custom_header_to_net();
        assert_eq!(
            topic_config_patch_outcome_from_response(&applied, 9).expect("applied outcome"),
            TopicConfigPatchOutcome::Applied {
                previous_version: 9,
                version: 10,
            }
        );

        let mut conflict = RemotingCommand::create_response_command()
            .set_code(ResponseCode::InvalidParameter)
            .set_command_custom_header(UpdateTopicConfigCasResponseHeader { topic_version: 11 });
        conflict.make_custom_header_to_net();
        assert_eq!(
            topic_config_patch_outcome_from_response(&conflict, 9).expect("conflict outcome"),
            TopicConfigPatchOutcome::VersionConflict {
                expected_version: 9,
                actual_version: 11,
            }
        );
    }

    #[cfg(feature = "admin-mutation")]
    #[test]
    fn subscription_group_patch_response_distinguishes_commit_and_conflict() {
        let mut applied = RemotingCommand::create_response_command()
            .set_code(ResponseCode::Success)
            .set_command_custom_header(UpdateSubscriptionGroupConfigCasResponseHeader {
                subscription_group_version: 10,
            });
        applied.make_custom_header_to_net();
        assert_eq!(
            subscription_group_config_patch_outcome_from_response(&applied, 9).expect("applied outcome"),
            SubscriptionGroupConfigPatchOutcome::Applied {
                previous_version: 9,
                version: 10,
            }
        );

        let mut conflict = RemotingCommand::create_response_command()
            .set_code(ResponseCode::InvalidParameter)
            .set_command_custom_header(UpdateSubscriptionGroupConfigCasResponseHeader {
                subscription_group_version: 11,
            });
        conflict.make_custom_header_to_net();
        assert_eq!(
            subscription_group_config_patch_outcome_from_response(&conflict, 9).expect("conflict outcome"),
            SubscriptionGroupConfigPatchOutcome::VersionConflict {
                expected_version: 9,
                actual_version: 11,
            }
        );
    }
}
