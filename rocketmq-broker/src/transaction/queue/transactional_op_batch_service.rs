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

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::task::service_task::ServiceContext;
use rocketmq_rust::task::service_task::ServiceTask;
use rocketmq_rust::task::ServiceManager;
use rocketmq_rust::WeakArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService;

pub struct TransactionalOpBatchService<MS>
where
    MS: MessageStore,
{
    service_manager: ServiceManager<TransactionalOpBatchServiceInner<MS>>,
}

impl<MS> TransactionalOpBatchService<MS>
where
    MS: MessageStore,
{
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        transactional_message_service: WeakArcMut<DefaultTransactionalMessageService<MS>>,
    ) -> Self {
        let inner = TransactionalOpBatchServiceInner {
            broker_config,
            transactional_message_service,
            wakeup_timestamp: AtomicU64::new(0),
        };
        let service_manager = ServiceManager::new(inner);
        TransactionalOpBatchService { service_manager }
    }

    pub async fn start(&self) {
        self.service_manager.start().await.unwrap();
    }

    pub async fn shutdown(&self) {
        self.service_manager.shutdown().await.unwrap();
    }

    pub fn wakeup(&self) {
        self.service_manager.wakeup();
    }
}

struct TransactionalOpBatchServiceInner<MS>
where
    MS: MessageStore,
{
    broker_config: Arc<BrokerConfig>,
    transactional_message_service: WeakArcMut<DefaultTransactionalMessageService<MS>>,
    wakeup_timestamp: AtomicU64,
}

impl<MS> ServiceTask for TransactionalOpBatchServiceInner<MS>
where
    MS: MessageStore,
{
    fn get_service_name(&self) -> String {
        "TransactionalOpBatchService".to_string()
    }

    async fn run(&self, context: &ServiceContext) {
        info!("TransactionalOpBatchService started");
        let transaction_op_batch_interval = self.broker_config.transaction_op_batch_interval;
        self.wakeup_timestamp.store(
            get_current_millis() + transaction_op_batch_interval,
            std::sync::atomic::Ordering::Relaxed,
        );
        while !context.is_stopped() {
            let mut interval =
                self.wakeup_timestamp.load(std::sync::atomic::Ordering::Relaxed) as i64 - get_current_millis() as i64;
            if interval <= 0 {
                interval = 0;
                context.wakeup();
            }
            if context.wait_for_running(Duration::from_millis(interval as u64)).await {
                self.on_wait_end().await;
            }
        }
    }

    async fn on_wait_end(&self) {
        if let Some(transactional_message_service) = self.transactional_message_service.upgrade() {
            let time = transactional_message_service.batch_send_op_message().await;
            self.wakeup_timestamp.store(time, std::sync::atomic::Ordering::Relaxed);
        } else {
            const WARN_MESSAGE: &str =
                "TransactionalMessageService has been dropped, skipping batch send operation message.";
            warn!(WARN_MESSAGE);
        }
    }
}
