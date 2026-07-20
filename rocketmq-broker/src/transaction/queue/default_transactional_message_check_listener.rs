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

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use rocketmq_runtime::TaskGroupChildLease;
use rocketmq_runtime::TaskKind;
use tracing::error;
use tracing::warn;

use crate::client::manager::producer_manager::ProducerChannelRegistry;
use crate::client::net::broker_to_client::Broker2Client;
use crate::transaction::transactional_message_check_listener::TransactionalMessageCheckListener;

struct TransactionCheckTaskOwner {
    _lease: TaskGroupChildLease,
    group: TaskGroup,
}

impl TransactionCheckTaskOwner {
    fn new(lease: TaskGroupChildLease) -> Self {
        let group = lease.group().clone();
        Self { _lease: lease, group }
    }
}

#[derive(Clone)]
pub struct DefaultTransactionalMessageCheckListener {
    broker_name: CheetahString,
    producer_channels: ProducerChannelRegistry,
    broker_client: Arc<Broker2Client>,
    task_owner: Option<Arc<TransactionCheckTaskOwner>>,
}

impl DefaultTransactionalMessageCheckListener {
    pub(crate) fn new(
        broker_name: CheetahString,
        producer_channels: ProducerChannelRegistry,
        broker_client: Arc<Broker2Client>,
        task_group_lease: Option<TaskGroupChildLease>,
    ) -> Self {
        Self {
            broker_name,
            producer_channels,
            broker_client,
            task_owner: task_group_lease.map(TransactionCheckTaskOwner::new).map(Arc::new),
        }
    }

    pub async fn shutdown(&self, timeout: Duration) -> Option<ShutdownReport> {
        let task_owner = self.task_owner.as_ref()?;
        Some(task_owner.group.shutdown(timeout).await)
    }
}

impl TransactionalMessageCheckListener for DefaultTransactionalMessageCheckListener {
    async fn send_check_message(&self, mut msg_ext: MessageExt) -> rocketmq_error::RocketMQResult<()> {
        let msg_id = msg_ext.user_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
        ));
        let header = CheckTransactionStateRequestHeader {
            topic: Some(msg_ext.message.topic().clone()),
            commit_log_offset: msg_ext.commit_log_offset,
            offset_msg_id: Some(msg_ext.msg_id().clone()),
            msg_id: msg_id.clone(),
            transaction_id: msg_id,
            tran_state_table_offset: msg_ext.queue_offset,
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(self.broker_name.clone()),
                ..Default::default()
            }),
        };
        let topic = msg_ext.user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC));
        if let Some(topic) = topic {
            msg_ext.set_topic(topic);
        }
        let queue_id = msg_ext.user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_REAL_QUEUE_ID));
        if let Some(queue_id) = queue_id {
            msg_ext.set_queue_id(queue_id.as_str().parse::<i32>().unwrap_or_default());
        }
        msg_ext.store_size = 0;
        let group_id = msg_ext.user_property(&CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP));
        let channel = self.producer_channels.get_available_channel(group_id.as_ref());
        if let Some(mut channel) = channel {
            self.broker_client
                .check_producer_transaction_state(group_id.as_ref().unwrap(), &mut channel, header, msg_ext)
                .await;
        } else {
            warn!("Check transaction failed, channel is null. groupId={:?}", group_id);
        }
        Ok(())
    }

    async fn resolve_half_msg(&self, msg_ext: MessageExt) -> rocketmq_error::RocketMQResult<()> {
        let Some(task_owner) = self.task_owner.as_ref() else {
            self.send_check_message(msg_ext).await?;
            return Ok(());
        };

        let this = self.clone();
        task_owner
            .group
            .spawn(
                "broker.transaction-check.send-check-message",
                TaskKind::Worker,
                async move {
                    this.send_check_message(msg_ext).await.unwrap_or_else(|e| {
                        error!("Failed to send check message: {}", e);
                    });
                },
            )
            .map_err(|error| {
                rocketmq_error::RocketMQError::Service(rocketmq_error::UnifiedServiceError::StartupFailed(format!(
                    "failed to spawn transaction check message task: {error}"
                )))
            })?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
