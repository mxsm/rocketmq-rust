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

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_runtime::task::service_task::ServiceContext;
use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::ServiceManager;
use rocketmq_store::base::message_store::MessageStore;
use tracing::info;
use tracing::warn;

use crate::transaction::queue::default_transactional_message_check_listener::DefaultTransactionalMessageCheckListener;
use crate::transaction::queue::default_transactional_message_service::DefaultTransactionalMessageService;
use crate::transaction::transactional_message_service::TransactionalMessageService;

pub struct TransactionalMessageCheckService<MS: MessageStore> {
    task_impl: ServiceManager<TransactionalMessageCheckServiceInner<MS>>,
}

struct TransactionalMessageCheckServiceInner<MS: MessageStore> {
    broker_config: Arc<BrokerConfig>,
    transactional_message_service: Arc<DefaultTransactionalMessageService<MS>>,
    transactional_message_check_listener: DefaultTransactionalMessageCheckListener,
}

impl<MS: MessageStore> ServiceTask for TransactionalMessageCheckServiceInner<MS> {
    fn get_service_name(&self) -> String {
        "TransactionalMessageCheckService".into()
    }

    async fn run(&self, context: &ServiceContext) {
        info!("Starting transactional check service");

        while !context.is_stopped() {
            let transaction_check_interval = self.broker_config.transaction_check_interval;
            context
                .wait_for_running(Duration::from_millis(transaction_check_interval))
                .await;
            if context.is_stopped() {
                break;
            }
            self.on_wait_end().await;
        }
        info!("Transactional check service stopped");
    }

    #[inline]
    async fn on_wait_end(&self) {
        let transaction_timeout = self.broker_config.transaction_timeout;
        let transaction_check_max = self.broker_config.transaction_check_max;
        let begin = Instant::now();
        info!(
            "Transactional check service is running, waiting for {} ms",
            transaction_timeout
        );
        self.transactional_message_service
            .check(
                transaction_timeout,
                transaction_check_max as i32,
                self.transactional_message_check_listener.clone(),
            )
            .await;
        info!(
            "End to check prepare message, consumed time:{}",
            begin.elapsed().as_millis()
        );
    }
}

impl<MS: MessageStore> TransactionalMessageCheckService<MS> {
    pub fn new(
        broker_config: Arc<BrokerConfig>,
        transactional_message_service: Arc<DefaultTransactionalMessageService<MS>>,
        transactional_message_check_listener: DefaultTransactionalMessageCheckListener,
    ) -> Self {
        let task_impl = ServiceManager::new(TransactionalMessageCheckServiceInner {
            broker_config,
            transactional_message_service,
            transactional_message_check_listener,
        });
        TransactionalMessageCheckService { task_impl }
    }
}

impl<MS: MessageStore> TransactionalMessageCheckService<MS> {
    pub async fn start(&self) -> rocketmq_error::RocketMQResult<()> {
        self.task_impl
            .start()
            .await
            .map_err(|source| rocketmq_error::RocketMQError::BrokerAsyncTaskFailed {
                task: "TransactionalMessageCheckService",
                context: "failed to start runtime-owned service task".to_string(),
                source: Box::new(source),
            })
    }

    pub async fn shutdown(&self) {
        if let Err(error) = self.task_impl.shutdown().await {
            warn!(error = %error, "TransactionalMessageCheckService shutdown failed");
        }
    }

    pub async fn shutdown_interrupt(&self, interrupt: bool) {
        if let Err(error) = self.task_impl.shutdown_with_interrupt(interrupt).await {
            warn!(error = %error, "TransactionalMessageCheckService interrupt shutdown failed");
        }
    }
}
