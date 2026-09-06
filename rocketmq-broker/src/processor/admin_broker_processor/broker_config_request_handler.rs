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
use std::collections::HashSet;
use std::sync::Arc;

use crate::config::error::BrokerConfigError;
use cheetah_string::CheetahString;
use rocketmq_error::PublicErrorView;
use rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::constant::file_readahead_mode::READ_AHEAD_MODE;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mq_version::CURRENT_VERSION;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::body::supervised_mutation::BrokerMutationConfigState;
use rocketmq_protocol::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigToJsonRequestHeader;
#[cfg(feature = "rocksdb_store")]
use rocketmq_protocol::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigType;
use rocketmq_protocol::protocol::header::get_broker_config_response_header::GetBrokerConfigResponseHeader;
use rocketmq_protocol::protocol::header::update_broker_config_request_header::UpdateBrokerConfigRequestHeader;
use rocketmq_protocol::protocol::header::update_broker_config_response_header::UpdateBrokerConfigResponseHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::DataVersion;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::BrokerAdminStore;
use rocketmq_store::CommitLogReadMode;
use rocketmq_transport::api::error_response;
use rocketmq_transport::api::RemotingErrorTarget;
use sysinfo::Disks;
use tracing::warn;

use crate::auth::auth_admin_service::AuthAdminService;
use crate::broker::broker_admin_runtime::BrokerAdminRuntime;
use crate::broker::log_filter_control::BrokerLogFilterRequest;
use crate::broker::log_filter_control::LOG_FILTER_KEYS;
use crate::topic::manager::topic_config_coordinator::TopicRegistrationAction;

use super::AdminRequestMetadata;

#[derive(Clone)]
pub(super) struct BrokerConfigRequestHandler<MS: BrokerAdminStore> {
    broker_runtime_inner: BrokerAdminRuntime<MS>,
    auth_admin_service: Option<Arc<AuthAdminService>>,
}

impl<MS: BrokerAdminStore> BrokerConfigRequestHandler<MS> {
    pub fn new(broker_runtime_inner: BrokerAdminRuntime<MS>) -> Self {
        BrokerConfigRequestHandler {
            broker_runtime_inner,
            auth_admin_service: None,
        }
    }

    pub fn new_with_auth(
        broker_runtime_inner: BrokerAdminRuntime<MS>,
        auth_admin_service: Arc<AuthAdminService>,
    ) -> Self {
        BrokerConfigRequestHandler {
            broker_runtime_inner,
            auth_admin_service: Some(auth_admin_service),
        }
    }

    pub(super) fn broker_runtime_inner(&self) -> &BrokerAdminRuntime<MS> {
        &self.broker_runtime_inner
    }

    pub(super) async fn persist_and_register_topic_updates(
        &self,
        topic_config_list: Vec<Arc<TopicConfig>>,
        data_version: DataVersion,
    ) -> rocketmq_error::RocketMQResult<()> {
        let runtime = self.broker_runtime_inner.clone();
        let single_topic_registration = runtime.broker_config().enable_single_topic_register;
        let registration: TopicRegistrationAction = Box::new(move || {
            Box::pin(async move {
                if single_topic_registration {
                    for topic_config in topic_config_list {
                        runtime.register_single_topic_all(topic_config).await;
                    }
                } else {
                    runtime
                        .register_increment_broker_data(topic_config_list, data_version)
                        .await;
                }
                Ok(())
            })
        });
        self.broker_runtime_inner
            .topic_config_coordinator()
            .persist_and_register_wait(registration)
            .await
    }

    pub(super) async fn apply_controller_role_change(
        &self,
        controller_leader_address: Option<CheetahString>,
        new_master_broker_id: Option<u64>,
        new_master_address: Option<CheetahString>,
        new_master_epoch: Option<i32>,
        sync_state_set_epoch: Option<i32>,
        sync_state_set: HashSet<i64>,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.broker_runtime_inner
            .clone()
            .apply_controller_role_change(
                controller_leader_address,
                new_master_broker_id,
                new_master_address,
                new_master_epoch,
                sync_state_set_epoch,
                sync_state_set,
            )
            .await
    }
}
impl<MS: BrokerAdminStore> BrokerConfigRequestHandler<MS> {
    pub async fn update_broker_config(
        &self,
        metadata: &AdminRequestMetadata,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command().set_opaque(request.opaque());
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

        let expected_generation = if request_code == RequestCode::UpdateBrokerConfigCas {
            match request.decode_command_custom_header::<UpdateBrokerConfigRequestHeader>() {
                Ok(header) if header.expected_generation > 0 => Some(header.expected_generation),
                Ok(_) => {
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::InvalidParameter)
                            .set_remark("expectedGeneration must be greater than zero"),
                    ));
                }
                Err(_) => {
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::InvalidParameter)
                            .set_remark("expectedGeneration is required and must be an unsigned integer"),
                    ));
                }
            }
        } else {
            None
        };

        if properties.keys().any(|key| LOG_FILTER_KEYS.contains(&key.as_str())) {
            if expected_generation.is_some() {
                return Ok(Some(response.set_code(ResponseCode::InvalidParameter).set_remark(
                    "generation-checked broker config updates do not accept log filter properties",
                )));
            }
            return self.update_log_filter(metadata, request, response, &properties).await;
        }

        let commit_result = match expected_generation {
            Some(expected_generation) => {
                self.broker_runtime_inner
                    .commit_broker_config_patch_if_generation(expected_generation, &properties)
                    .await
            }
            None => self.broker_runtime_inner.commit_broker_config_patch(&properties).await,
        };
        let generation = match commit_result {
            Ok(generation) => generation,
            Err(error @ BrokerConfigError::RestartRequired { .. })
            | Err(error @ BrokerConfigError::UnsupportedKeys { .. })
            | Err(error @ BrokerConfigError::InvalidProperty { .. })
            | Err(error @ BrokerConfigError::Invalid { .. })
            | Err(error @ BrokerConfigError::Contract { .. }) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark(error.to_string()),
                ));
            }
            Err(error @ BrokerConfigError::GenerationConflict { actual, .. }) => {
                return Ok(Some(
                    response
                        .set_command_custom_header(UpdateBrokerConfigResponseHeader {
                            config_generation: actual,
                        })
                        .set_code(ResponseCode::InvalidParameter)
                        .set_remark(error.to_string()),
                ));
            }
            Err(error @ BrokerConfigError::Load { .. })
            | Err(error @ BrokerConfigError::Runtime { .. })
            | Err(error @ BrokerConfigError::GenerationExhausted)
            | Err(error @ BrokerConfigError::RuntimeProjectionUnavailable { .. }) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(error.to_string()),
                ));
            }
            Err(error @ BrokerConfigError::RuntimeCoordination { .. }) => {
                warn!(%error, "runtime broker configuration coordination failed");
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("runtime configuration coordination failed"),
                ));
            }
        };

        Ok(Some(
            RemotingCommand::create_success_response_command_with_header(UpdateBrokerConfigResponseHeader {
                config_generation: generation.value(),
            })
            .set_opaque(request.opaque())
            .set_remark(format!(
                "update broker config success, generation={}",
                generation.value()
            )),
        ))
    }

    async fn update_log_filter(
        &self,
        metadata: &AdminRequestMetadata,
        request: &RemotingCommand,
        response: RemotingCommand,
        properties: &HashMap<CheetahString, CheetahString>,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let broker_config = self.broker_runtime_inner.broker_config();
        if !broker_config.authentication_enabled || !broker_config.authorization_enabled {
            return Ok(Some(response.set_code(ResponseCode::NoPermission).set_remark(
                "remote log filter reload requires authentication and authorization",
            )));
        }
        let Some(control) = self.broker_runtime_inner.log_filter_control() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark("remote log filter reload is disabled or unavailable"),
            ));
        };
        let source_ip = metadata.source_ip();
        let operator = request
            .get_ext_fields()
            .and_then(|fields| fields.get(&CheetahString::from_static_str("AccessKey")))
            .map(CheetahString::as_str)
            .unwrap_or_default();
        if operator.trim().is_empty() {
            control
                .audit_rejection(properties, operator, &source_ip, false, "missing_access_key")
                .await;
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark("remote log filter reload requires an authenticated AccessKey"),
            ));
        }
        let Some(auth_admin_service) = self.auth_admin_service.as_ref() else {
            control
                .audit_rejection(properties, operator, &source_ip, false, "authorization_unavailable")
                .await;
            return Ok(Some(
                response
                    .set_code(ResponseCode::NoPermission)
                    .set_remark("remote log filter authorization service is unavailable"),
            ));
        };
        let super_user = match auth_admin_service.is_super_user(operator).await {
            Ok(super_user) => super_user,
            Err(error) => {
                tracing::warn!(error = %error, "failed to resolve remote log filter operator role");
                control
                    .audit_rejection(properties, operator, &source_ip, false, "authorization_failure")
                    .await;
                return Ok(Some(
                    response
                        .set_code(ResponseCode::NoPermission)
                        .set_remark("remote log filter operator could not be authorized"),
                ));
            }
        };
        let request = match BrokerLogFilterRequest::parse(properties, operator, source_ip.as_str(), super_user) {
            Ok(request) => request,
            Err(remark) => {
                control
                    .audit_rejection(properties, operator, &source_ip, super_user, "validation_failure")
                    .await;
                return Ok(Some(
                    response.set_code(ResponseCode::InvalidParameter).set_remark(remark),
                ));
            }
        };
        match control.apply(request).await {
            Ok(resolved) => Ok(Some(
                RemotingCommand::create_success_response_command()
                    .set_opaque(response.opaque())
                    .set_remark(format!(
                        "log filter update success: effectiveFilter={}, baselineFilter={}",
                        resolved.filter(),
                        control.baseline().filter()
                    )),
            )),
            Err(error) => {
                tracing::error!(error = %error, "broker remote log filter update failed");
                Ok(Some(response.set_code(ResponseCode::SystemError).set_remark(
                    "remote log filter update failed; startup baseline is preserved",
                )))
            }
        }
    }

    pub async fn get_broker_config(
        &self,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_success_response_command();
        // broker config => broker config
        // default message store config => message store config
        let snapshot = self.broker_runtime_inner.runtime_config_snapshot();
        let broker_config_properties = snapshot.broker().get_properties();
        let message_store_config_properties = snapshot.store().get_properties();
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
        let generation = i64::try_from(snapshot.id().value())
            .map_err(|_| rocketmq_error::RocketMQError::invariant_violated("broker config generation fits i64"))?;
        let published_at_millis = i64::try_from(snapshot.published_at_millis())
            .map_err(|_| rocketmq_error::RocketMQError::invariant_violated("broker config timestamp fits i64"))?;
        let version = serde_json::to_string(&rocketmq_protocol::protocol::DataVersion::with_values(
            0,
            published_at_millis,
            generation,
        ))
        .map_err(|error| rocketmq_error::RocketMQError::internal("serialize broker config version", error))?;
        response.set_command_custom_header_ref(GetBrokerConfigResponseHeader {
            version: Some(version.into()),
            config_generation: snapshot.id().value(),
        });
        Ok(Some(response))
    }

    pub async fn get_broker_mutation_config(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let (snapshot, live_message_index_enabled) = self.broker_runtime_inner.runtime_config_and_live_index_snapshot();
        let broker = snapshot.broker();
        let store = snapshot.store();
        let state = BrokerMutationConfigState {
            generation: snapshot.id().value(),
            auto_create_topic_enable: broker.auto_create_topic_enable,
            auto_create_subscription_group: broker.auto_create_subscription_group,
            broker_permission: broker.broker_permission,
            default_topic_queue_nums: broker.topic_queue_config.default_topic_queue_nums,
            message_index_enable: live_message_index_enabled.unwrap_or(store.message_index_enable),
            trace_topic_enable: broker.trace_topic_enable,
        };
        Ok(Some(
            RemotingCommand::create_success_response_command()
                .set_opaque(request.opaque())
                .set_body(state.encode()?),
        ))
    }

    pub async fn get_broker_runtime_info(
        &self,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response = RemotingCommand::create_success_response_command();
        let runtime_info = self.prepare_runtime_info();
        let key_value_table = KVTable { table: runtime_info };
        response.set_body_mut_ref(serde_json::to_string(&key_value_table).unwrap());
        Ok(Some(response))
    }

    pub async fn set_commitlog_read_mode(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
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

        let Some(read_mode) = CommitLogReadMode::from_wire_value(mode) else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::InvalidParameter)
                    .set_remark("set commitlog readahead mode param value error"),
            ));
        };

        match self.broker_runtime_inner.set_commitlog_read_mode(read_mode).await {
            Ok(()) => Ok(Some(
                RemotingCommand::create_success_response_command()
                    .set_remark(format!("set commitlog readahead mode success, mode: {mode}")),
            )),
            Err(error) => Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark(format!("set commitlog readahead mode failed: {error}")),
            )),
        }
    }

    pub async fn export_rocksdb_config_to_json(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();
        let request_header = request.decode_command_custom_header::<ExportRocksdbConfigToJsonRequestHeader>()?;
        let config_types = match request_header.fetch_config_type() {
            Ok(config_types) => config_types,
            Err(error) => {
                return Ok(Some(
                    response.set_code(ResponseCode::InvalidParameter).set_remark(error),
                ));
            }
        };

        #[cfg(feature = "rocksdb_store")]
        {
            let mut exported_count = 0usize;
            for config_type in config_types {
                let export_result = match config_type {
                    ExportRocksdbConfigType::Topics
                        if self
                            .broker_runtime_inner
                            .topic_config_manager()
                            .is_rocksdb_config_enabled() =>
                    {
                        exported_count += 1;
                        self.broker_runtime_inner
                            .topic_config_coordinator()
                            .export_to_json()
                            .await
                    }
                    ExportRocksdbConfigType::SubscriptionGroups
                        if self
                            .broker_runtime_inner
                            .subscription_group_manager()
                            .is_rocksdb_config_enabled() =>
                    {
                        exported_count += 1;
                        self.broker_runtime_inner.subscription_group_manager().export_to_json()
                    }
                    ExportRocksdbConfigType::ConsumerOffsets
                        if self
                            .broker_runtime_inner
                            .consumer_offset_manager()
                            .is_rocksdb_config_enabled() =>
                    {
                        exported_count += 1;
                        self.broker_runtime_inner.consumer_offset_manager().export_to_json()
                    }
                    _ => Ok(()),
                };
                if let Err(error) = export_result {
                    return Ok(Some(
                        response
                            .set_code(ResponseCode::SystemError)
                            .set_remark(format!("export rocksdb config to json failed: {error}")),
                    ));
                }
            }
            if exported_count > 0 {
                return Ok(Some(
                    RemotingCommand::create_success_response_command().set_remark("export done."),
                ));
            }
        }

        #[cfg(not(feature = "rocksdb_store"))]
        let _ = config_types;

        let command_factory = application_remoting_command_factory();
        Ok(Some(error_response(
            PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
            RemotingErrorTarget::Reply {
                factory: &command_factory,
                opaque: request.opaque(),
            },
        )))
    }

    pub async fn get_timer_metrics(
        &self,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(self.build_timer_metrics_response()))
    }

    pub async fn get_timer_check_point(
        &self,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        Ok(Some(self.build_timer_checkpoint_response()))
    }

    pub async fn switch_timer_engine(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = RemotingCommand::create_java_default_error_response_command();

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

        let Some(timer_message_store) = self.broker_runtime_inner.timer_message_store() else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("switch timer engine error"),
            ));
        };

        timer_message_store.set_should_running_dequeue(true);
        timer_message_store.start();

        Ok(Some(
            RemotingCommand::create_success_response_command().set_remark("switch timer engine success"),
        ))
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
        let message_store = self.broker_runtime_inner.message_store().unwrap();
        let mut runtime_info = message_store.get_runtime_info();
        let generation = self.broker_runtime_inner.runtime_config_snapshot();
        let store_health = message_store.health_snapshot();
        let broker_shutdown = self.broker_runtime_inner.is_shutdown();
        let broker_active = self.is_special_service_running();
        runtime_info.insert(
            "sreDiagnosticsSchemaVersion".to_string(),
            "rocketmq.broker-diagnostics.legacy".to_string(),
        );
        runtime_info.insert(
            "sreDiagnosticsObservedAtMillis".to_string(),
            current_millis().to_string(),
        );
        runtime_info.insert(
            "brokerConfigGeneration".to_string(),
            generation.id().value().to_string(),
        );
        runtime_info.insert("brokerShutdown".to_string(), broker_shutdown.to_string());
        runtime_info.insert(
            "brokerRegistrationAccepting".to_string(),
            (!broker_shutdown).to_string(),
        );
        runtime_info.insert(
            "brokerRegistrationConfigured".to_string(),
            generation.broker().namesrv_addr.is_some().to_string(),
        );
        runtime_info.insert(
            "brokerReady".to_string(),
            (broker_active && !broker_shutdown && store_health.writable && !store_health.shutdown).to_string(),
        );
        runtime_info.insert(
            "brokerRole".to_string(),
            generation.store().broker_role.get_broker_role().to_string(),
        );
        runtime_info.insert(
            "storeType".to_string(),
            generation.store().store_type.get_store_type().to_string(),
        );
        runtime_info.insert(
            "timerWheelEnabled".to_string(),
            generation.store().timer_wheel_enable.to_string(),
        );
        runtime_info.insert(
            "transientStorePoolEnabled".to_string(),
            generation.store().transient_store_pool_enable.to_string(),
        );
        #[cfg(feature = "tieredstore")]
        runtime_info.insert(
            "tieredStoreConfigured".to_string(),
            generation.store().tiered_store_config.is_some().to_string(),
        );
        #[cfg(not(feature = "tieredstore"))]
        runtime_info.insert("tieredStoreConfigured".to_string(), "false".to_string());
        runtime_info.insert("storeWriteable".to_string(), store_health.writable.to_string());
        runtime_info.insert(
            "storeLastFlushError".to_string(),
            store_health.last_error.is_some().to_string(),
        );
        runtime_info.insert(
            "storeOsPageCacheBusy".to_string(),
            store_health.page_cache_busy.to_string(),
        );
        runtime_info.insert(
            "storeTransientPoolDeficient".to_string(),
            store_health.transient_pool_deficient.to_string(),
        );
        runtime_info.insert("storeShutdown".to_string(), store_health.shutdown.to_string());
        runtime_info.insert(
            "storeDispatchBehindBytes".to_string(),
            store_health.dispatch_behind_bytes.to_string(),
        );
        runtime_info.insert(
            "storeHaPendingRequestCount".to_string(),
            store_health.replication_pending_count.to_string(),
        );
        runtime_info.insert(
            "storeHaPendingOldestWaitMillis".to_string(),
            store_health.replication_oldest_wait_millis.to_string(),
        );
        runtime_info.insert(
            "storeSyncFlushQueueDepth".to_string(),
            store_health.flush_backlog.queue_depth.to_string(),
        );
        runtime_info.insert(
            "storeSyncFlushOldestWaitMillis".to_string(),
            store_health.flush_backlog.oldest_wait_millis.to_string(),
        );
        if let Some(ha) = message_store.get_ha_runtime_info() {
            runtime_info.insert("haDiagnosticsSupported".to_string(), "true".to_string());
            runtime_info.insert(
                "storeConfirmOffset".to_string(),
                message_store.get_confirm_offset().max(0).to_string(),
            );
            runtime_info.insert(
                "haLegalInSyncAckOffset".to_string(),
                legal_in_sync_ack_offset(&ha).to_string(),
            );
            runtime_info.insert(
                "haRole".to_string(),
                if ha.master { "master" } else { "replica" }.to_string(),
            );
            runtime_info.insert(
                "haMaxReplicaLagBytes".to_string(),
                ha.ha_connection_info
                    .iter()
                    .map(|connection| connection.diff.max(0) as u64)
                    .max()
                    .unwrap_or_default()
                    .to_string(),
            );
            runtime_info.insert(
                "haDecisionCode".to_string(),
                if ha.pending_group_transfer_request_count > 0 {
                    "waiting_for_replica_progress"
                } else {
                    "not_observed"
                }
                .to_string(),
            );
            let store_config = generation.store();
            let (ack_policy, required_ack_count) = if store_config.all_ack_in_sync_state_set {
                ("all_in_sync_set", None)
            } else if store_config.in_sync_replicas <= 1 {
                ("local_durable", Some(1_u64))
            } else {
                ("replica_count", u64::try_from(store_config.in_sync_replicas).ok())
            };
            runtime_info.insert("haAckPolicy".to_string(), ack_policy.to_string());
            if let Some(required_ack_count) = required_ack_count {
                runtime_info.insert("haRequiredAckCount".to_string(), required_ack_count.to_string());
            }
            if let Some(replicas_manager) = self.broker_runtime_inner.replicas_manager() {
                if let Some(authority) = replicas_manager.write_authority() {
                    runtime_info.insert("haMasterEpoch".to_string(), authority.master_epoch().get().to_string());
                }
                let sync_state_set_epoch = replicas_manager.sync_state_set_epoch();
                if sync_state_set_epoch > 0 {
                    runtime_info.insert("haSyncStateSetEpoch".to_string(), sync_state_set_epoch.to_string());
                }
                runtime_info.insert(
                    "haSyncStateSetSize".to_string(),
                    replicas_manager.sync_state_set().len().to_string(),
                );
            } else {
                runtime_info.insert(
                    "haSyncStateSetSize".to_string(),
                    (ha.in_sync_slave_nums.max(0) as u64 + u64::from(ha.master)).to_string(),
                );
            }
        } else {
            runtime_info.insert("haDiagnosticsSupported".to_string(), "false".to_string());
        }
        if let Some(auth) = self.auth_admin_service.as_ref() {
            let auth = auth.diagnostics_snapshot();
            runtime_info.insert("authDiagnosticsSupported".to_string(), "true".to_string());
            runtime_info.insert(
                "authAuthenticationEnabled".to_string(),
                auth.authentication_enabled.to_string(),
            );
            runtime_info.insert(
                "authAuthorizationEnabled".to_string(),
                auth.authorization_enabled.to_string(),
            );
            runtime_info.insert(
                "authAclFileWatchEnabled".to_string(),
                auth.acl_file_watch_enabled.to_string(),
            );
            runtime_info.insert("authAclGeneration".to_string(), auth.acl_generation.to_string());
            runtime_info.insert(
                "authAclReloadAttempts".to_string(),
                auth.acl_reload_attempts.to_string(),
            );
            runtime_info.insert(
                "authAclReloadSuccesses".to_string(),
                auth.acl_reload_successes.to_string(),
            );
            runtime_info.insert(
                "authAclReloadFailures".to_string(),
                auth.acl_reload_failures.to_string(),
            );
            runtime_info.insert("authAclReloadSkipped".to_string(), auth.acl_reload_skipped.to_string());
        } else {
            runtime_info.insert("authDiagnosticsSupported".to_string(), "false".to_string());
        }
        runtime_info.insert("authCredentialRotationSupported".to_string(), "false".to_string());
        if let Some(control) = self.broker_runtime_inner.log_filter_control() {
            let status = control.status();
            runtime_info.insert("sreLogFilterControlSupported".to_string(), "true".to_string());
            runtime_info.insert("sreLogFilterEffective".to_string(), status.effective_filter);
            if let Some(operation_id) = status.active_operation_id {
                runtime_info.insert("sreLogFilterActiveOperationId".to_string(), operation_id);
            }
            if let Some(operation_id) = status.last_completed_operation_id {
                runtime_info.insert("sreLogFilterLastCompletedOperationId".to_string(), operation_id);
            }
            if let Some(expires_at_millis) = status.expires_at_millis {
                runtime_info.insert("sreLogFilterExpiresAtMillis".to_string(), expires_at_millis.to_string());
            }
        } else {
            runtime_info.insert("sreLogFilterControlSupported".to_string(), "false".to_string());
        }
        self.broker_runtime_inner
            .schedule_message_service()
            .build_running_stats(&mut runtime_info);
        runtime_info.insert("brokerActive".to_string(), broker_active.to_string());
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
            "asyncTopicCreatePersistPendingCount".to_string(),
            self.broker_runtime_inner
                .topic_config_coordinator()
                .pending_count()
                .to_string(),
        );
        runtime_info.insert(
            "asyncTopicCreatePersistSpawnFailureCount".to_string(),
            self.broker_runtime_inner
                .topic_config_coordinator()
                .persist_failure_count()
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

fn legal_in_sync_ack_offset(runtime: &HARuntimeInfo) -> u64 {
    runtime
        .ha_connection_info
        .iter()
        .filter(|connection| connection.in_sync)
        .map(|connection| connection.slave_ack_offset)
        .chain(std::iter::once(runtime.master_commit_log_max_offset))
        .min()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use std::time::SystemTime;

    use super::legal_in_sync_ack_offset;

    use crate::config::broker_config::BrokerConfig;
    #[cfg(feature = "rocksdb_store")]
    use crate::config::config_manager::ConfigManager;
    use crate::config::transaction::ConfigUpdateTransaction;
    use crate::config::validated::ConfigGeneration;
    use crate::config::validated::ValidatedBrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::constant::file_readahead_mode::READ_AHEAD_MODE;
    use rocketmq_model::common::constant::PermName;
    use rocketmq_model::common::message::MessageConst;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::body::ha_runtime_info::HARuntimeInfo;
    use rocketmq_protocol::protocol::body::supervised_mutation::BrokerMutationConfigState;
    use rocketmq_protocol::protocol::header::export_rocksdb_config_to_json_request_header::ExportRocksdbConfigToJsonRequestHeader;
    use rocketmq_protocol::protocol::header::get_broker_config_response_header::GetBrokerConfigResponseHeader;
    use rocketmq_protocol::protocol::header::update_broker_config_request_header::UpdateBrokerConfigRequestHeader;
    use rocketmq_protocol::protocol::header::update_broker_config_response_header::UpdateBrokerConfigResponseHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    #[cfg(feature = "rocksdb_store")]
    use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
    use rocketmq_protocol::protocol::RemotingDeserializable;
    use rocketmq_runtime::common::time_utils::current_millis;
    use rocketmq_store::BrokerAdminStore;
    use rocketmq_store::BrokerReadStore;
    use rocketmq_store::CommitLogReadMode;
    use rocketmq_store::DispatchRequest;
    use rocketmq_store::MessageStoreConfig;
    #[cfg(feature = "rocksdb_store")]
    use rocketmq_store::StoreType;
    use rocketmq_store::TimerCheckpointSnapshot;
    use rocketmq_store::TimerMessageStore;
    use rocketmq_store::TimerMetricsSerializeWrapper;

    use crate::broker::broker_pre_online_capability::BrokerPreOnlinePolicy;
    use crate::broker_runtime::BrokerRuntime;

    use super::AdminRequestMetadata;
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
        assert!(runtime.initialize().await.is_ok());
        runtime
    }

    #[cfg(feature = "rocksdb_store")]
    async fn new_rocksdb_config_runtime(label: &str) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            store_type: StoreType::RocksDB,
            real_time_persist_rocksdb_config: true,
            ..MessageStoreConfig::default()
        });
        BrokerRuntime::new(broker_config, message_store_config)
    }

    #[tokio::test]
    async fn build_timer_metrics_response_returns_encoded_timer_metrics() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);

        let timer_message_store = TimerMessageStore::new_empty(crate::test_service_context("timer-store"));
        timer_message_store
            .timer_metrics
            .add_timing_count(&CheetahString::from_static_str("TimerTopicA"), 2);
        runtime.runtime_state_mut().set_timer_message_store(timer_message_store);

        let handler = BrokerConfigRequestHandler::new(runtime.admin_runtime_for_test());
        let response = handler.build_timer_metrics_response();

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = response.body().expect("timer metrics response body should exist");
        let wrapper: TimerMetricsSerializeWrapper = serde_json::from_slice(body.as_ref()).unwrap();
        assert_eq!(wrapper.timing_count_snapshot().get("TimerTopicA"), Some(&2));
    }

    #[tokio::test]
    async fn build_timer_checkpoint_response_returns_encoded_checkpoint_snapshot() {
        let temp_dir = std::env::temp_dir().join(format!("rmq-rust-timer-checkpoint-{}", current_millis()));
        let _ = fs::remove_dir_all(&temp_dir);
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig {
            timer_wheel_enable: true,
            store_path_root_dir: temp_dir.to_string_lossy().to_string().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config.clone());

        let timer_message_store = TimerMessageStore::new_with_message_store_config(
            message_store_config,
            crate::test_service_context("timer-store"),
        );
        assert!(timer_message_store.load());
        runtime.runtime_state_mut().set_timer_message_store(timer_message_store);

        let handler = BrokerConfigRequestHandler::new(runtime.admin_runtime_for_test());
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
        let runtime = new_test_runtime("commitlog-read-mode", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());

        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::SetCommitlogReadMode).set_ext_fields(HashMap::new());
        request.add_ext_field(
            CheetahString::from_static_str(READ_AHEAD_MODE),
            CommitLogReadMode::Normal.wire_value().to_string(),
        );

        let response = handler
            .set_commitlog_read_mode(RequestCode::SetCommitlogReadMode, &mut request)
            .await
            .expect("set commitlog read mode should succeed")
            .expect("set commitlog read mode should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(admin.message_store_config().data_read_ahead_enable);
        assert!(admin
            .message_store()
            .expect("message store should exist")
            .data_read_ahead_enabled());

        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::SetCommitlogReadMode).set_ext_fields(HashMap::new());
        request.add_ext_field(
            CheetahString::from_static_str(READ_AHEAD_MODE),
            CommitLogReadMode::Random.wire_value().to_string(),
        );

        let response = handler
            .set_commitlog_read_mode(RequestCode::SetCommitlogReadMode, &mut request)
            .await
            .expect("set commitlog read mode should succeed")
            .expect("set commitlog read mode should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(!admin.message_store_config().data_read_ahead_enable);
        assert!(!admin
            .message_store()
            .expect("message store should exist")
            .data_read_ahead_enabled());

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn runtime_diagnostics_generation_comes_from_the_atomic_config_snapshot() {
        let runtime = new_test_runtime("sre-diagnostics-generation", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());

        let initial = handler.prepare_runtime_info();
        assert_eq!(
            initial.get("sreDiagnosticsSchemaVersion").map(|value| value.as_str()),
            Some("rocketmq.broker-diagnostics.legacy")
        );
        assert_eq!(
            initial.get("brokerConfigGeneration").map(|value| value.as_str()),
            Some("1")
        );
        assert_eq!(initial.get("storeWriteable").map(|value| value.as_str()), Some("true"));
        assert!(initial.contains_key("storeConfirmOffset"));
        assert!(initial.contains_key("haLegalInSyncAckOffset"));
        assert_eq!(
            initial.get("sreLogFilterControlSupported").map(|value| value.as_str()),
            Some("false")
        );

        let mut next = admin.broker_config().as_ref().clone();
        next.max_client_event_count = next.max_client_event_count.saturating_add(1);
        let admin = admin;
        admin
            .set_broker_config(next)
            .expect("valid configuration replacement should advance generation");

        let updated = handler.prepare_runtime_info();
        assert_eq!(
            updated.get("brokerConfigGeneration").map(|value| value.as_str()),
            Some("2")
        );
        assert_eq!(
            updated.get("brokerRole").map(|value| value.as_str()),
            Some(admin.message_store_config().broker_role.get_broker_role())
        );

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn legal_in_sync_ack_uses_the_slowest_in_sync_replica() {
        use rocketmq_protocol::protocol::body::ha_connection_runtime_info::HAConnectionRuntimeInfo;

        let runtime = HARuntimeInfo {
            master: true,
            master_commit_log_max_offset: 300,
            ha_connection_info: vec![
                HAConnectionRuntimeInfo {
                    slave_ack_offset: 240,
                    in_sync: true,
                    ..Default::default()
                },
                HAConnectionRuntimeInfo {
                    slave_ack_offset: 180,
                    in_sync: true,
                    ..Default::default()
                },
                HAConnectionRuntimeInfo {
                    slave_ack_offset: 20,
                    in_sync: false,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(legal_in_sync_ack_offset(&runtime), 180);
    }

    #[tokio::test]
    async fn get_broker_config_binds_body_to_the_committed_generation() {
        let runtime = new_test_runtime("get-broker-config-generation", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin);
        let mut request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig);

        let response = handler
            .get_broker_config(RequestCode::GetBrokerConfig, &mut request)
            .await
            .expect("get broker config should return broker response")
            .expect("get broker config should return a response");

        let first_header = response
            .read_custom_header_ref::<GetBrokerConfigResponseHeader>()
            .expect("get broker config should include a typed response header")
            .clone();
        assert_eq!(
            first_header.config_generation, 1,
            "the response header should identify the body generation"
        );
        let first_version: rocketmq_protocol::protocol::DataVersion = serde_json::from_str(
            first_header
                .version
                .as_ref()
                .expect("Java-compatible version metadata should be present")
                .as_str(),
        )
        .expect("version metadata should use the Java DataVersion JSON shape");
        assert_eq!(first_version.get_state_version(), 0);
        assert_eq!(first_version.get_counter(), 1);
        assert!(first_version.get_timestamp() > 0);
        assert!(response
            .get_body()
            .is_some_and(|body| String::from_utf8_lossy(body).contains("flushDelayOffsetInterval")));

        let second_response = handler
            .get_broker_config(RequestCode::GetBrokerConfig, &mut request)
            .await
            .expect("repeated get broker config should return broker response")
            .expect("repeated get broker config should return a response");
        let second_header = second_response
            .read_custom_header_ref::<GetBrokerConfigResponseHeader>()
            .expect("repeated response should include a typed response header");
        assert_eq!(
            second_header, &first_header,
            "reads of one committed generation must publish a stable DataVersion"
        );

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_broker_config_applies_and_restores_each_reviewed_runtime_property() {
        let mut runtime = new_test_runtime("update-broker-config", false).await;
        let live_broker_permission = runtime.runtime_state_mut().broker_permission_state();
        let pre_online_policy = BrokerPreOnlinePolicy::from_configs(
            runtime.broker_config().as_ref(),
            runtime.message_store_config().as_ref(),
            CheetahString::from_static_str("127.0.0.1:10911"),
            CheetahString::new(),
            live_broker_permission.clone(),
        );
        let topic_config_manager = runtime.runtime_state_mut().topic_config_manager_handle();
        let registration = runtime.runtime_state_mut().build_registration_runtime();
        let escape_bridge = runtime.runtime_state_mut().escape_bridge();
        let transaction_registration = runtime.runtime_state_mut().build_transaction_topic_registration(
            crate::transaction::queue::transaction_message_store::TransactionMessageStore::new(&escape_bridge),
        );
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());
        let peer = "127.0.0.1:10911".parse().expect("test peer");
        let cases = [
            ("autoCreateTopicEnable", "false", "true"),
            ("autoCreateSubscriptionGroup", "false", "true"),
            ("brokerPermission", "4", "6"),
            ("defaultTopicQueueNums", "16", "8"),
            ("messageIndexEnable", "false", "true"),
            ("traceTopicEnable", "true", "false"),
        ];

        for (key, replacement, original) in cases {
            for value in [replacement, original] {
                let expected_generation = admin.runtime_config_snapshot().id().value();
                let topic_data_version_before = topic_config_manager.data_version();
                let mut request = RemotingCommand::create_request_command(
                    RequestCode::UpdateBrokerConfigCas,
                    UpdateBrokerConfigRequestHeader { expected_generation },
                )
                .set_body(format!("{key}={value}"));
                request.make_custom_header_to_net();
                let response = handler
                    .update_broker_config(
                        &AdminRequestMetadata::network_for_test(peer),
                        RequestCode::UpdateBrokerConfigCas,
                        &mut request,
                    )
                    .await
                    .expect("runtime update should return broker response")
                    .expect("runtime update should return a response");
                assert_eq!(
                    ResponseCode::from(response.code()),
                    ResponseCode::Success,
                    "{key}={value}"
                );
                assert_eq!(
                    response
                        .read_custom_header_ref::<UpdateBrokerConfigResponseHeader>()
                        .map(|header| header.config_generation),
                    Some(expected_generation + 1),
                    "{key}={value}"
                );

                let mut get = RemotingCommand::create_remoting_command(RequestCode::GetBrokerMutationConfig);
                let response = handler
                    .get_broker_mutation_config(RequestCode::GetBrokerMutationConfig, &mut get)
                    .await
                    .expect("post-read should return broker response")
                    .expect("post-read should return a response");
                let state = BrokerMutationConfigState::decode(
                    response.get_body().expect("post-read should contain state").as_ref(),
                )
                .expect("post-read state should decode");
                assert_eq!(state.generation, expected_generation + 1);
                match key {
                    "autoCreateTopicEnable" => assert_eq!(state.auto_create_topic_enable.to_string(), value),
                    "autoCreateSubscriptionGroup" => {
                        assert_eq!(state.auto_create_subscription_group.to_string(), value);
                    }
                    "brokerPermission" => {
                        assert_eq!(state.broker_permission.to_string(), value);
                        assert_ne!(topic_config_manager.data_version(), topic_data_version_before);
                        assert_eq!(live_broker_permission.get().to_string(), value);
                        let expected = value.parse::<u32>().expect("permission should parse");
                        let topic = TopicConfig::with_perm(
                            "RuntimePermissionTopic",
                            1,
                            1,
                            PermName::PERM_READ | PermName::PERM_WRITE,
                        );
                        assert_eq!(registration.topic_config_for_registration(&topic).perm, expected);
                        assert_eq!(
                            pre_online_policy.topic_config_for_registration_for_test(&topic).perm,
                            expected
                        );
                        assert_eq!(
                            transaction_registration
                                .topic_config_for_registration_for_test(&topic)
                                .perm,
                            expected
                        );
                    }
                    "defaultTopicQueueNums" => assert_eq!(state.default_topic_queue_nums.to_string(), value),
                    "messageIndexEnable" => {
                        assert_eq!(state.message_index_enable.to_string(), value);
                        let store = admin.message_store().expect("message store should remain available");
                        let snapshot = store
                            .message_index_runtime_snapshot()
                            .expect("initialized Store should expose one coherent index runtime");
                        assert_eq!(snapshot.enabled.to_string(), value);
                        if value == "true" {
                            assert!(!snapshot.incomplete, "a no-message disabled interval remains complete");
                        }
                    }
                    "traceTopicEnable" => assert_eq!(state.trace_topic_enable.to_string(), value),
                    _ => unreachable!(),
                }
            }
        }

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_broker_config_rejects_index_enable_after_disabled_dispatch() {
        let runtime = new_test_runtime("update-broker-config-index-gap", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());
        let peer = "127.0.0.1:10911".parse().expect("test peer");

        let mut disable = RemotingCommand::create_request_command(
            RequestCode::UpdateBrokerConfigCas,
            UpdateBrokerConfigRequestHeader {
                expected_generation: admin.runtime_config_snapshot().id().value(),
            },
        )
        .set_body("messageIndexEnable=false");
        disable.make_custom_header_to_net();
        let disabled = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfigCas,
                &mut disable,
            )
            .await
            .expect("disable should return a response")
            .expect("disable response");
        assert_eq!(ResponseCode::from(disabled.code()), ResponseCode::Success);

        let store = admin.message_store().expect("message store should remain available");
        let mut observed_index_dispatcher = false;
        for dispatcher in store.get_dispatcher_list() {
            if dispatcher.message_index_runtime_snapshot().is_some() {
                observed_index_dispatcher = true;
                dispatcher.dispatch(&mut DispatchRequest::default());
            }
        }
        assert!(observed_index_dispatcher);
        drop(store);

        let disabled_generation = admin.runtime_config_snapshot().id().value();
        let mut enable = RemotingCommand::create_request_command(
            RequestCode::UpdateBrokerConfigCas,
            UpdateBrokerConfigRequestHeader {
                expected_generation: disabled_generation,
            },
        )
        .set_body("messageIndexEnable=true");
        enable.make_custom_header_to_net();
        let rejected = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfigCas,
                &mut enable,
            )
            .await
            .expect("unsafe enable should return a response")
            .expect("unsafe enable response");

        assert_eq!(ResponseCode::from(rejected.code()), ResponseCode::SystemError);
        assert_eq!(
            rejected.remark().map(CheetahString::as_str),
            Some("runtime configuration coordination failed")
        );
        assert_eq!(admin.runtime_config_snapshot().id().value(), disabled_generation);
        let snapshot = admin
            .message_store()
            .and_then(|store| store.message_index_runtime_snapshot())
            .expect("Store should retain index runtime state");
        assert!(!snapshot.enabled);
        assert!(snapshot.incomplete);

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_broker_config_cas_commits_once_and_rejects_stale_generation() {
        let runtime = new_test_runtime("update-broker-config-cas", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());
        let peer = "127.0.0.1:10911".parse().expect("test peer");

        let mut request = RemotingCommand::create_request_command(
            RequestCode::UpdateBrokerConfigCas,
            UpdateBrokerConfigRequestHeader { expected_generation: 1 },
        )
        .set_body("defaultTopicQueueNums=16");
        request.make_custom_header_to_net();
        let request_opaque = request.opaque();

        let response = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfigCas,
                &mut request,
            )
            .await
            .expect("CAS update should return broker response")
            .expect("CAS update should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(response.opaque(), request_opaque);
        assert_eq!(
            response
                .read_custom_header_ref::<UpdateBrokerConfigResponseHeader>()
                .map(|header| header.config_generation),
            Some(2)
        );
        assert_eq!(
            response.remark().map(CheetahString::as_str),
            Some("update broker config success, generation=2")
        );
        assert_eq!(admin.broker_config().topic_queue_config.default_topic_queue_nums, 16);

        let mut stale_request = RemotingCommand::create_request_command(
            RequestCode::UpdateBrokerConfigCas,
            UpdateBrokerConfigRequestHeader { expected_generation: 1 },
        )
        .set_body("defaultTopicQueueNums=32");
        stale_request.make_custom_header_to_net();

        let stale_response = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfigCas,
                &mut stale_request,
            )
            .await
            .expect("stale CAS update should return broker response")
            .expect("stale CAS update should return a response");

        assert_eq!(
            ResponseCode::from(stale_response.code()),
            ResponseCode::InvalidParameter
        );
        assert_eq!(
            stale_response
                .read_custom_header_ref::<UpdateBrokerConfigResponseHeader>()
                .map(|header| header.config_generation),
            Some(2)
        );
        assert!(stale_response
            .remark()
            .is_some_and(|remark| remark.contains("expected 1, actual 2")));
        assert_eq!(admin.broker_config().topic_queue_config.default_topic_queue_nums, 16);

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn concurrent_broker_config_cas_has_exactly_one_winner() {
        let runtime = new_test_runtime("update-broker-config-cas-race", false).await;
        let admin = runtime.admin_runtime_for_test();
        let first_handler = BrokerConfigRequestHandler::new(admin.clone());
        let second_handler = BrokerConfigRequestHandler::new(admin.clone());
        let peer = "127.0.0.1:10911".parse().expect("test peer");
        let first = async move {
            let mut request = RemotingCommand::create_request_command(
                RequestCode::UpdateBrokerConfigCas,
                UpdateBrokerConfigRequestHeader { expected_generation: 1 },
            )
            .set_body("defaultTopicQueueNums=16");
            request.make_custom_header_to_net();
            first_handler
                .update_broker_config(
                    &AdminRequestMetadata::network_for_test(peer),
                    RequestCode::UpdateBrokerConfigCas,
                    &mut request,
                )
                .await
                .expect("first CAS should return broker response")
                .expect("first CAS should return a response")
        };
        let second = async move {
            let mut request = RemotingCommand::create_request_command(
                RequestCode::UpdateBrokerConfigCas,
                UpdateBrokerConfigRequestHeader { expected_generation: 1 },
            )
            .set_body("defaultTopicQueueNums=32");
            request.make_custom_header_to_net();
            second_handler
                .update_broker_config(
                    &AdminRequestMetadata::network_for_test(peer),
                    RequestCode::UpdateBrokerConfigCas,
                    &mut request,
                )
                .await
                .expect("second CAS should return broker response")
                .expect("second CAS should return a response")
        };

        let (first, second) = tokio::join!(first, second);
        let codes = [ResponseCode::from(first.code()), ResponseCode::from(second.code())];
        assert_eq!(codes.iter().filter(|code| **code == ResponseCode::Success).count(), 1);
        assert_eq!(
            codes
                .iter()
                .filter(|code| **code == ResponseCode::InvalidParameter)
                .count(),
            1
        );
        let snapshot = admin.runtime_config_snapshot();
        assert_eq!(snapshot.id().value(), 2);
        assert!([16, 32].contains(&snapshot.broker().topic_queue_config.default_topic_queue_nums));

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn mixed_invalid_broker_patch_preserves_generation_configs_and_live_projections() {
        let runtime = new_test_runtime("update-broker-config-rollback", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());
        let peer = "127.0.0.1:10911".parse().expect("test peer");
        let before_generation = admin.runtime_config_snapshot();
        let before_projection = admin.runtime_mutation_projection_snapshot();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::UpdateBrokerConfigCas,
            UpdateBrokerConfigRequestHeader { expected_generation: 1 },
        )
        .set_body(concat!(
            "autoCreateTopicEnable=false\n",
            "autoCreateSubscriptionGroup=false\n",
            "brokerPermission=4\n",
            "defaultTopicQueueNums=129\n",
            "messageIndexEnable=false\n",
            "traceTopicEnable=true",
        ));
        request.make_custom_header_to_net();

        let response = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfigCas,
                &mut request,
            )
            .await
            .expect("invalid patch should return broker response")
            .expect("invalid patch should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        let after_generation = admin.runtime_config_snapshot();
        assert!(Arc::ptr_eq(&before_generation, &after_generation));
        assert_eq!(before_generation.id(), after_generation.id());
        assert_eq!(
            before_generation.broker().get_properties(),
            after_generation.broker().get_properties()
        );
        assert_eq!(
            before_generation.store().get_properties(),
            after_generation.store().get_properties()
        );
        assert_eq!(before_projection, admin.runtime_mutation_projection_snapshot());

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn unavailable_index_projection_preserves_generation_configs_and_live_projections() {
        let temp_root = temp_test_root("update-broker-config-index-unavailable");
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let runtime = BrokerRuntime::new(broker_config, message_store_config);
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());
        let before_generation = admin.runtime_config_snapshot();
        let before_projection = admin.runtime_mutation_projection_snapshot();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::UpdateBrokerConfigCas,
            UpdateBrokerConfigRequestHeader { expected_generation: 1 },
        )
        .set_body("messageIndexEnable=false");
        request.make_custom_header_to_net();

        let response = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test("127.0.0.1:10911".parse().expect("test peer")),
                RequestCode::UpdateBrokerConfigCas,
                &mut request,
            )
            .await
            .expect("unavailable projection should return broker response")
            .expect("unavailable projection should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::SystemError);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("message-index dispatcher")));
        let after_generation = admin.runtime_config_snapshot();
        assert!(Arc::ptr_eq(&before_generation, &after_generation));
        assert_eq!(
            before_generation.broker().get_properties(),
            after_generation.broker().get_properties()
        );
        assert_eq!(
            before_generation.store().get_properties(),
            after_generation.store().get_properties()
        );
        assert_eq!(before_projection, admin.runtime_mutation_projection_snapshot());

        let _ = fs::remove_dir_all(temp_root);
    }

    #[tokio::test]
    async fn update_log_filter_requires_broker_authentication_and_authorization() {
        let runtime = new_test_runtime("update-log-filter-auth-disabled", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin);
        let peer = "127.0.0.1:10911".parse().expect("test peer");
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig).set_body(concat!(
            "logFilter=info,rocketmq_broker=debug\n",
            "logFilterReason=incident investigation\n",
            "logFilterRequestId=INC-42",
        ));

        let response = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfig,
                &mut request,
            )
            .await
            .expect("log filter update should return broker response")
            .expect("log filter update should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::NoPermission);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("authentication and authorization")));
        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn update_broker_config_rejects_unsupported_or_invalid_keys() {
        let runtime = new_test_runtime("update-broker-config-invalid", false).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());

        let peer = "127.0.0.1:10911".parse().expect("test peer");
        let mut request = RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig)
            .set_body("unknownKey=true\nmaxClientEventCount=0");

        let response = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfig,
                &mut request,
            )
            .await
            .expect("update broker config should return broker response")
            .expect("update broker config should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("maxClientEventCount")));
        assert_eq!(admin.broker_config().max_client_event_count, 100);

        let peer = "127.0.0.1:10911".parse().expect("test peer");
        let mut request =
            RemotingCommand::create_remoting_command(RequestCode::UpdateBrokerConfig).set_body("unknownKey=true");

        let response = handler
            .update_broker_config(
                &AdminRequestMetadata::network_for_test(peer),
                RequestCode::UpdateBrokerConfig,
                &mut request,
            )
            .await
            .expect("update broker config should return broker response")
            .expect("update broker config should return a response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);
        assert!(response
            .remark()
            .is_some_and(|remark| remark.contains("unsupported broker configuration keys: unknownKey")));

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[test]
    fn static_and_unknown_properties_are_rejected_before_publication() {
        let current = ValidatedBrokerConfig::default();
        let static_properties = HashMap::from([(
            CheetahString::from_static_str("brokerName"),
            CheetahString::from_static_str("renamed"),
        )]);
        let error =
            match ConfigUpdateTransaction::from_broker_patch(ConfigGeneration::INITIAL, &current, &static_properties) {
                Ok(_) => panic!("brokerName must require a restart"),
                Err(error) => error,
            };
        assert_eq!(
            error.to_string(),
            "broker configuration fields require restart: brokerName"
        );

        let other_static_properties = HashMap::from([
            (
                CheetahString::from_static_str("defaultMessageRequestMode"),
                CheetahString::from_static_str("POP"),
            ),
            (
                CheetahString::from_static_str("defaultPopShareQueueNum"),
                CheetahString::from_static_str("2"),
            ),
            (
                CheetahString::from_static_str("serverLoadBalancerEnable"),
                CheetahString::from_static_str("true"),
            ),
        ]);
        let error = match ConfigUpdateTransaction::from_broker_patch(
            ConfigGeneration::INITIAL,
            &current,
            &other_static_properties,
        ) {
            Ok(_) => panic!("other known static properties must require a restart"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            concat!(
                "broker configuration fields require restart: defaultMessageRequestMode,",
                "defaultPopShareQueueNum,serverLoadBalancerEnable"
            )
        );

        let unknown_properties = HashMap::from([(
            CheetahString::from_static_str("unknownKey"),
            CheetahString::from_static_str("true"),
        )]);
        let error = match ConfigUpdateTransaction::from_broker_patch(
            ConfigGeneration::INITIAL,
            &current,
            &unknown_properties,
        ) {
            Ok(_) => panic!("unknown properties must not enter a runtime generation"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "unsupported broker configuration keys: unknownKey");
    }

    #[tokio::test]
    async fn export_rocksdb_config_without_rocksdb_returns_not_supported() {
        let runtime = new_test_runtime("export-config", false).await;
        let handler = BrokerConfigRequestHandler::new(runtime.admin_runtime_for_test());
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ExportRocksdbConfigToJson,
            ExportRocksdbConfigToJsonRequestHeader {
                config_type: CheetahString::from_static_str("topics;subscriptionGroups;consumerOffsets;"),
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .export_rocksdb_config_to_json(RequestCode::ExportRocksdbConfigToJson, &mut request)
            .await
            .expect("export config should succeed")
            .expect("export config should return response");

        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::RequestCodeNotSupported
        );
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some("Protocol request is unsupported")
        );

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[cfg(feature = "rocksdb_store")]
    #[tokio::test]
    async fn export_rocksdb_config_to_json_writes_requested_json_files() {
        let mut runtime = new_rocksdb_config_runtime("export-rocksdb-config").await;
        let topic = CheetahString::from_static_str("ExportTopic");
        let group = CheetahString::from_static_str("ExportGroup");

        {
            let inner_mut = runtime.runtime_state_mut();
            let _ = inner_mut
                .topic_config_manager()
                .update_topic_config(TopicConfig::with_queues(topic.clone(), 2, 4), 0);
            inner_mut.consumer_offset_manager().commit_offset(
                CheetahString::from_static_str("127.0.0.1:10911"),
                &group,
                &topic,
                0,
                77,
            );
            inner_mut.consumer_offset_manager().advance_data_version();
            inner_mut.consumer_offset_manager().persist().unwrap();
            let mut group_config = SubscriptionGroupConfig::new(group.clone());
            group_config.set_consume_broadcast_enable(false);
            inner_mut
                .subscription_group_manager()
                .update_subscription_group_config(&mut group_config);
        }

        let handler = BrokerConfigRequestHandler::new(runtime.admin_runtime_for_test());
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ExportRocksdbConfigToJson,
            ExportRocksdbConfigToJsonRequestHeader {
                config_type: CheetahString::from_static_str("topics;subscriptionGroups;consumerOffsets;"),
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .export_rocksdb_config_to_json(RequestCode::ExportRocksdbConfigToJson, &mut request)
            .await
            .expect("export config should succeed")
            .expect("export config should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(response.remark().map(|remark| remark.as_str()), Some("export done."));

        let root = runtime.message_store_config().store_path_root_dir.as_str().to_string();
        let topics_json = fs::read_to_string(crate::broker_path_config_helper::get_topic_config_path(&root))
            .expect("topics json should be exported");
        let offsets_json = fs::read_to_string(crate::broker_path_config_helper::get_consumer_offset_path(&root))
            .expect("consumer offsets json should be exported");
        let subscription_json =
            fs::read_to_string(crate::broker_path_config_helper::get_subscription_group_path(&root))
                .expect("subscription groups json should be exported");

        assert!(topics_json.contains("ExportTopic"));
        assert!(offsets_json.contains("ExportTopic@ExportGroup"));
        assert!(subscription_json.contains("ExportGroup"));

        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), runtime.shutdown_basic_service()).await;
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn switch_timer_engine_accepts_file_time_wheel_and_rejects_rocksdb() {
        let runtime = new_test_runtime("switch-timer-engine", true).await;
        let admin = runtime.admin_runtime_for_test();
        let handler = BrokerConfigRequestHandler::new(admin.clone());

        let mut file_request =
            RemotingCommand::create_remoting_command(RequestCode::SwitchTimerEngine).set_ext_fields(HashMap::new());
        file_request.add_ext_field(
            CheetahString::from_static_str(MessageConst::TIMER_ENGINE_TYPE),
            MessageConst::TIMER_ENGINE_FILE_TIME_WHEEL,
        );

        let response = handler
            .switch_timer_engine(RequestCode::SwitchTimerEngine, &mut file_request)
            .await
            .expect("switch timer engine should succeed")
            .expect("switch timer engine should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(admin
            .timer_message_store()
            .expect("timer message store should exist")
            .is_should_running_dequeue());

        let mut rocksdb_request =
            RemotingCommand::create_remoting_command(RequestCode::SwitchTimerEngine).set_ext_fields(HashMap::new());
        rocksdb_request.add_ext_field(
            CheetahString::from_static_str(MessageConst::TIMER_ENGINE_TYPE),
            MessageConst::TIMER_ENGINE_ROCKSDB_TIMELINE,
        );

        let response = handler
            .switch_timer_engine(RequestCode::SwitchTimerEngine, &mut rocksdb_request)
            .await
            .expect("switch timer engine should succeed")
            .expect("switch timer engine should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

        let _ = fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
