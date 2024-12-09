/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;

use crate::topic::manager::topic_route_info_manager::TopicRouteInfoManager;

const SEND_TIMEOUT: u64 = 3_000;
const DEFAULT_PULL_TIMEOUT_MILLIS: u64 = 10_000;
///### RocketMQ's EscapeBridge for Dead Letter Queue (DLQ) Mechanism
///
/// In the context of message passing within RocketMQ, the `EscapeBridge` primarily handles the Dead
/// Letter Queue (DLQ) mechanism. When messages fail to be successfully consumed by a consumer after
/// multiple attempts, these messages are designated as "dead letters"—messages that cannot be
/// processed normally. To prevent such messages from indefinitely blocking the consumer's
/// processing flow, RocketMQ provides functionality to transfer these messages to a special queue
/// known as the dead letter queue.
///
/// The `EscapeBridge` acts as a bridge in this process, responsible for moving messages that have
/// failed consumption from their original queue into the DLQ. This action helps maintain system
/// health and prevents the entire consumption process from being obstructed by a few problematic
/// messages. Additionally, it provides developers with an opportunity to analyze and address these
/// abnormal messages at a later time.
///
/// #### Functions of EscapeBridge
///
/// - **Isolation of Problematic Messages:** Moves messages that cannot be consumed into the DLQ to
///   ensure they do not continue to affect normal consumption processes.
/// - **Preservation of Message Data:** Even if a message is considered unconsumable, its content is
///   preserved, allowing for subsequent diagnosis or specialized handling.
/// - **Support for Retry Logic:** For messages that may have failed due to transient issues, retry
///   logic can be applied by requeueing or specially processing messages in the DLQ, enabling
///   another attempt at consumption.
///
/// Through this approach, RocketMQ enhances the reliability and stability of the messaging system.
/// It also equips developers with better tools for managing and troubleshooting issues in message
/// transmission.
///
/// #### Conclusion
///
/// RocketMQ's `EscapeBridge` plays a critical role in maintaining the robustness of the messaging
/// system by effectively handling messages that cannot be processed. By isolating problematic
/// messages, preserving their data, and supporting retry mechanisms, it ensures that the overall
/// consumption process remains healthy and unobstructed. Developers gain valuable insights into
/// message failures, aiding in the diagnosis and resolution of potential issues.
///
/// **Note:** The specific configuration and usage methods may vary depending on the version of
/// RocketMQ. Please refer to the official documentation for the most accurate information.
pub(crate) struct EscapeBridge<MS> {
    inner_producer_group_name: CheetahString,
    inner_consumer_group_name: CheetahString,
    escape_bridge_runtime: RocketMQRuntime,
    message_store: ArcMut<MS>,
    broker_config: Arc<BrokerConfig>,
    topic_route_info_manager: Arc<TopicRouteInfoManager>,
}
