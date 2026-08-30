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

#![allow(deprecated)]

use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQResult;
use rocketmq_protocol::protocol::heartbeat::message_model::MessageModel;
use rocketmq_transport::api::RPCHook;

use crate::base::client_options::ClientOptions;
use crate::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;
use crate::consumer::default_mq_pull_consumer::DefaultMQPullConsumer;
use crate::nameserver_discovery::NameServerDiscoveryConfig;
use crate::runtime::ClientRuntime;

/// Builds a Classic Pull compatibility facade on the shared client runtime.
pub struct DefaultMQPullConsumerBuilder {
    client_runtime: Arc<ClientRuntime>,
    client_options: Option<ClientOptions>,
    name_server_addr: Option<CheetahString>,
    consumer_group: Option<CheetahString>,
    message_model: MessageModel,
    unit_mode: bool,
    broker_suspend_max_time: Duration,
    consumer_timeout_when_suspend: Duration,
    consumer_pull_timeout: Duration,
    queue_refresh_interval: Duration,
    rpc_hook: Option<Arc<dyn RPCHook>>,
}

impl DefaultMQPullConsumerBuilder {
    /// Creates a builder backed by an application-owned client runtime.
    pub fn new(client_runtime: Arc<ClientRuntime>) -> Self {
        Self {
            client_runtime,
            client_options: None,
            name_server_addr: None,
            consumer_group: None,
            message_model: MessageModel::Clustering,
            unit_mode: false,
            broker_suspend_max_time: Duration::from_secs(20),
            consumer_timeout_when_suspend: Duration::from_secs(30),
            consumer_pull_timeout: Duration::from_secs(10),
            queue_refresh_interval: Duration::from_secs(30),
            rpc_hook: None,
        }
    }

    /// Sets the consumer group.
    pub fn consumer_group(mut self, consumer_group: impl Into<CheetahString>) -> Self {
        self.consumer_group = Some(consumer_group.into());
        self
    }

    /// Sets the complete client options, including typed NameServer discovery.
    pub fn client_options(mut self, options: ClientOptions) -> Self {
        self.client_options = Some(options);
        self
    }

    /// Sets typed NameServer discovery.
    pub fn nameserver_discovery(mut self, discovery: NameServerDiscoveryConfig) -> Self {
        let options = self.client_options.take().unwrap_or_default();
        self.client_options = Some(options.with_nameserver_discovery(discovery));
        self
    }

    /// Sets a static NameServer address list.
    pub fn name_server_addr(mut self, name_server_addr: impl Into<CheetahString>) -> Self {
        self.name_server_addr = Some(name_server_addr.into());
        self
    }

    /// Sets the consumer message model.
    pub fn message_model(mut self, message_model: MessageModel) -> Self {
        self.message_model = message_model;
        self
    }

    /// Sets unit mode.
    pub fn unit_mode(mut self, unit_mode: bool) -> Self {
        self.unit_mode = unit_mode;
        self
    }

    /// Sets the broker-side long-poll suspension limit.
    pub fn broker_suspend_max_time(mut self, timeout: Duration) -> Self {
        self.broker_suspend_max_time = timeout;
        self
    }

    /// Sets the client timeout used by block-if-not-found pulls.
    pub fn consumer_timeout_when_suspend(mut self, timeout: Duration) -> Self {
        self.consumer_timeout_when_suspend = timeout;
        self
    }

    /// Sets the default timeout used by ordinary pulls.
    pub fn consumer_pull_timeout(mut self, timeout: Duration) -> Self {
        self.consumer_pull_timeout = timeout;
        self
    }

    /// Sets how frequently registered queue listeners refresh topic routes.
    pub fn queue_refresh_interval(mut self, interval: Duration) -> Self {
        self.queue_refresh_interval = interval;
        self
    }

    /// Sets the request hook used by the shared client instance.
    pub fn rpc_hook(mut self, rpc_hook: Arc<dyn RPCHook>) -> Self {
        self.rpc_hook = Some(rpc_hook);
        self
    }

    /// Builds the Classic Pull facade.
    ///
    /// # Errors
    ///
    /// Returns an error when the consumer group is missing, a duration is zero or outside the
    /// supported millisecond range, the suspended-request timeout is not greater than the broker
    /// suspension limit, or the underlying consumer configuration is invalid.
    pub fn build(self) -> RocketMQResult<DefaultMQPullConsumer> {
        let consumer_group = self
            .consumer_group
            .ok_or_else(|| crate::mq_client_err!("consumer_group is required"))?;
        if self.broker_suspend_max_time.is_zero() {
            return Err(crate::mq_client_err!("broker suspend timeout must be positive"));
        }
        if self.consumer_timeout_when_suspend <= self.broker_suspend_max_time {
            return Err(crate::mq_client_err!(
                "consumer timeout when suspended must exceed broker suspend timeout"
            ));
        }
        if self.consumer_pull_timeout.is_zero() {
            return Err(crate::mq_client_err!("consumer pull timeout must be positive"));
        }
        if self.queue_refresh_interval.is_zero() {
            return Err(crate::mq_client_err!("queue refresh interval must be positive"));
        }

        let broker_suspend_max_time_millis = duration_millis("broker suspend timeout", self.broker_suspend_max_time)?;
        let consumer_timeout_when_suspend_millis =
            duration_millis("consumer timeout when suspended", self.consumer_timeout_when_suspend)?;
        let consumer_pull_timeout_millis = duration_millis("consumer pull timeout", self.consumer_pull_timeout)?;
        let queue_refresh_interval_millis = duration_millis("queue refresh interval", self.queue_refresh_interval)?;

        let mut lite_builder = DefaultLitePullConsumer::builder(self.client_runtime.clone())
            .consumer_group(consumer_group.clone())
            .message_model(self.message_model)
            .unit_mode(self.unit_mode)
            .classic_pull_manual_mode()
            .auto_commit(false)
            .broker_suspend_max_time_millis(broker_suspend_max_time_millis)
            .consumer_timeout_millis_when_suspend(consumer_timeout_when_suspend_millis)
            .consumer_pull_timeout_millis(consumer_pull_timeout_millis)
            .topic_metadata_check_interval_millis(queue_refresh_interval_millis);
        if let Some(options) = self.client_options {
            lite_builder = lite_builder.client_options(options);
        }
        if let Some(name_server_addr) = self.name_server_addr {
            lite_builder = lite_builder.name_server_addr(name_server_addr);
        }
        if let Some(rpc_hook) = self.rpc_hook {
            lite_builder = lite_builder.rpc_hook(rpc_hook);
        }

        DefaultMQPullConsumer::from_lite_consumer(
            consumer_group,
            lite_builder.build()?,
            self.consumer_pull_timeout,
            self.broker_suspend_max_time,
            self.consumer_timeout_when_suspend,
        )
    }
}

fn duration_millis(name: &str, duration: Duration) -> RocketMQResult<u64> {
    u64::try_from(duration.as_millis()).map_err(|_| crate::mq_client_err!(format!("{name} is too large")))
}
