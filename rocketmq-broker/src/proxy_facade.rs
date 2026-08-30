//  Copyright 2023 The RocketMQ Rust Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub use crate::config::broker_config::BrokerConfig;
use crate::config::error::BrokerConfigError;
use crate::config::validated::ValidatedBrokerConfig;
use cheetah_string::CheetahString;
use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::mix_all;
use rocketmq_observability::TelemetryHandle;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::remoting_command_defaults::RemotingCommandFactory;
use rocketmq_protocol::protocol::route::route_data_view::BrokerData;
use rocketmq_protocol::protocol::route::route_data_view::QueueData;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_runtime::TaskGroup;
use rocketmq_security_api::Principal;
use rocketmq_store::MessageStoreConfig;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v2::EmbeddedDispatchErrorKind;
use rocketmq_transport::api::v2::EmbeddedDispatchOutcome;

use crate::broker_runtime::BrokerRuntime;
use crate::lifecycle::BrokerReadiness;
use crate::lifecycle::BrokerStartupError;

pub struct ProxyBrokerFacade {
    runtime: BrokerRuntime,
    local_request_tasks: TaskGroup,
}

impl ProxyBrokerFacade {
    /// Creates an embedded Broker using the store root carried by `broker_config`.
    ///
    /// This is the narrow constructor for local Proxy composition. Callers that
    /// need non-default store tuning can continue to use [`Self::try_new`].
    pub fn try_new_from_broker_config(
        broker_config: BrokerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> Result<Self, BrokerConfigError> {
        Self::try_new_from_broker_config_with_factory(
            broker_config,
            service_context,
            telemetry_handle,
            application_remoting_command_factory(),
        )
    }

    /// Creates a proxy facade from broker configuration using explicit remoting defaults.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerConfigError`] when the broker or derived message-store configuration is invalid.
    pub fn try_new_from_broker_config_with_factory(
        broker_config: BrokerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
        command_factory: RemotingCommandFactory,
    ) -> Result<Self, BrokerConfigError> {
        let message_store_config = MessageStoreConfig {
            store_path_root_dir: broker_config.store_path_root_dir.clone(),
            ..MessageStoreConfig::default()
        };
        Self::try_new_with_factory(
            broker_config,
            message_store_config,
            service_context,
            telemetry_handle,
            command_factory,
        )
    }

    pub fn try_new(
        broker_config: BrokerConfig,
        message_store_config: MessageStoreConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> Result<Self, BrokerConfigError> {
        Self::try_new_with_factory(
            broker_config,
            message_store_config,
            service_context,
            telemetry_handle,
            application_remoting_command_factory(),
        )
    }

    /// Creates a proxy facade from broker and message-store configurations using explicit remoting defaults.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerConfigError`] when either configuration is invalid.
    pub fn try_new_with_factory(
        mut broker_config: BrokerConfig,
        message_store_config: MessageStoreConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
        command_factory: RemotingCommandFactory,
    ) -> Result<Self, BrokerConfigError> {
        broker_config.transfer_msg_by_heap = true;
        broker_config.broker_server_config.listen_port = broker_config.listen_port;
        let validated_config = ValidatedBrokerConfig::try_from_parts(broker_config, message_store_config)?;
        Ok(Self::from_validated_config_with_factory(
            validated_config,
            service_context,
            telemetry_handle,
            command_factory,
        ))
    }

    pub fn from_validated_config(
        validated_config: ValidatedBrokerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
    ) -> Self {
        Self::from_validated_config_with_factory(
            validated_config,
            service_context,
            telemetry_handle,
            application_remoting_command_factory(),
        )
    }

    /// Creates a proxy facade from validated configuration using explicit remoting defaults.
    pub fn from_validated_config_with_factory(
        validated_config: ValidatedBrokerConfig,
        service_context: ChildServiceContext,
        telemetry_handle: TelemetryHandle,
        command_factory: RemotingCommandFactory,
    ) -> Self {
        let runtime_context = service_context.component("embedded-broker");
        let local_request_tasks = service_context.component("local-request").task_group().clone();
        Self {
            runtime: BrokerRuntime::new_with_validated_config_telemetry_and_factory(
                Arc::new(validated_config),
                runtime_context,
                telemetry_handle,
                command_factory,
            ),
            local_request_tasks,
        }
    }

    pub async fn initialize(&mut self) -> Result<(), BrokerStartupError> {
        self.runtime.initialize().await
    }

    pub async fn start(&mut self) -> Result<BrokerReadiness, BrokerStartupError> {
        self.runtime.start().await
    }

    pub async fn shutdown(&mut self) {
        self.runtime.shutdown().await;
    }

    pub fn broker_config(&self) -> Arc<BrokerConfig> {
        self.runtime.broker_config()
    }

    pub fn query_route(&self, topic: &str) -> rocketmq_error::RocketMQResult<TopicRouteData> {
        let topic_name = CheetahString::from(topic);
        let topic_config =
            self.runtime
                .topic_config(&topic_name)
                .ok_or_else(|| rocketmq_error::RocketMQError::TopicNotExist {
                    topic: topic.to_owned(),
                })?;

        Ok(build_topic_route(&self.runtime.broker_config(), topic_config.as_ref()))
    }

    pub fn query_topic_message_type(&self, topic: &str) -> rocketmq_error::RocketMQResult<TopicMessageType> {
        let topic_name = CheetahString::from(topic);
        let topic_config =
            self.runtime
                .topic_config(&topic_name)
                .ok_or_else(|| rocketmq_error::RocketMQError::TopicNotExist {
                    topic: topic.to_owned(),
                })?;
        Ok(topic_config.get_topic_message_type())
    }

    pub fn query_subscription_group(
        &self,
        group: &str,
    ) -> rocketmq_error::RocketMQResult<Option<Arc<SubscriptionGroupConfig>>> {
        Ok(self.runtime.subscription_group(&CheetahString::from(group)))
    }

    /// Dispatches one local Proxy command through the prepared Broker V2 graph.
    ///
    /// The transport boundary fixes the origin to `EmbeddedCaller::BrokerProxy`;
    /// the principal below is a composition-owned service identity and is never
    /// read from command headers.
    ///
    /// # Errors
    ///
    /// Returns an error when the prepared V2 dispatcher is unavailable, the
    /// embedded request cannot be admitted or completed, or the response
    /// deadline elapses.
    pub async fn process_request_v2(
        &self,
        request: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        timeout: Duration,
    ) -> rocketmq_error::RocketMQResult<EmbeddedDispatchOutcome> {
        self.process_request_v2_with_deadline(request, RequestDeadline::after(timeout))
            .await
    }

    async fn process_request_v2_with_deadline(
        &self,
        mut request: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
        deadline: RequestDeadline,
    ) -> rocketmq_error::RocketMQResult<EmbeddedDispatchOutcome> {
        request.make_custom_header_to_net();
        let dispatcher = self
            .runtime
            .canonical_v2_dispatcher()
            .ok_or_else(embedded_broker_v2_request_processor_not_ready)?;
        dispatcher
            .dispatch_embedded_v2_wait_response(
                &self.local_request_tasks,
                Principal::new("broker-proxy"),
                Some(deadline),
                request,
            )
            .await
            .map_err(|error| embedded_dispatch_v2_error(error, deadline.budget()))
    }
}

#[cfg(test)]
fn embedded_broker_request_processor_not_ready() -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::not_initialized("embedded_broker_request_processor")
}

fn embedded_broker_v2_request_processor_not_ready() -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::not_initialized("embedded_broker_v2_request_processor")
}

fn embedded_dispatch_v2_error(
    error: rocketmq_transport::api::v2::EmbeddedDispatchError,
    timeout: Duration,
) -> rocketmq_error::RocketMQError {
    if matches!(error.kind(), EmbeddedDispatchErrorKind::DeadlineExceeded) {
        return rocketmq_error::RocketMQError::Timeout {
            operation: "embedded_broker_v2_response",
            timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        };
    }
    rocketmq_error::RocketMQError::response_process_failed("embedded_broker_v2_dispatch", error.to_string())
}

#[cfg(test)]
fn embedded_dispatch_error(
    error: rocketmq_transport::api::v1::DispatchError,
    timeout: Duration,
) -> rocketmq_error::RocketMQError {
    if matches!(
        &error,
        rocketmq_transport::api::v1::DispatchError::Response(
            rocketmq_transport::api::v1::ResponseSinkError::DeadlineExceeded,
        )
    ) {
        return rocketmq_error::RocketMQError::Timeout {
            operation: "embedded_broker_response",
            timeout_ms: timeout.as_millis().min(u128::from(u64::MAX)) as u64,
        };
    }
    rocketmq_error::RocketMQError::response_process_failed("embedded_broker_dispatch", error.to_string())
}

fn build_topic_route(broker_config: &BrokerConfig, topic_config: &TopicConfig) -> TopicRouteData {
    let broker_name = broker_config.broker_identity.broker_name.clone();
    let broker_addr = CheetahString::from(broker_config.get_broker_addr());
    let mut broker_addrs = HashMap::new();
    broker_addrs.insert(mix_all::MASTER_ID, broker_addr);

    TopicRouteData {
        queue_datas: vec![QueueData::new(
            broker_name.clone(),
            topic_config.read_queue_nums,
            topic_config.write_queue_nums,
            topic_config.perm,
            topic_config.topic_sys_flag,
        )],
        broker_datas: vec![BrokerData::new(
            broker_config.broker_identity.broker_cluster_name.clone(),
            broker_name,
            broker_addrs,
            None,
        )],
        ..TopicRouteData::default()
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_error::ErrorKind;

    use super::*;

    #[test]
    fn embedded_broker_request_processor_not_ready_uses_not_initialized_kind() {
        let error = embedded_broker_request_processor_not_ready();

        assert_eq!(error.kind(), ErrorKind::NotInitialized);
    }

    #[test]
    fn embedded_dispatch_timeout_reports_the_explicit_budget() {
        let error = embedded_dispatch_error(
            rocketmq_transport::api::v1::DispatchError::Response(
                rocketmq_transport::api::v1::ResponseSinkError::DeadlineExceeded,
            ),
            Duration::from_millis(15_500),
        );

        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::Timeout {
                operation: "embedded_broker_response",
                timeout_ms: 15_500
            }
        ));
    }
}
