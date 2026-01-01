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

use rocketmq_common::common::broker::broker_role::BrokerRole;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_rust::task::service_task::ServiceContext;
use rocketmq_rust::task::service_task::ServiceTask;
use rocketmq_rust::task::ServiceManager;
use rocketmq_rust::ArcMut;
use tokio::sync::Mutex;
use tracing::error;

use crate::ha::general_ha_connection::GeneralHAConnection;
use crate::ha::general_ha_service::GeneralHAService;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection::HAConnection;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::message_store::local_file_message_store::LocalFileMessageStore;
use crate::store_error::HAError;
use crate::store_error::HAResult;

const CONNECTION_ESTABLISH_TIMEOUT: u64 = 10 * 1000;

pub struct HAConnectionStateNotificationService {
    inner: Arc<Inner>,
    service_manager: ServiceManager<Inner>,
}

struct Inner {
    ha_service: GeneralHAService,
    default_message_store: ArcMut<LocalFileMessageStore>,
    request: Mutex<Option<HAConnectionStateNotificationRequest>>,
    last_check_time_stamp: AtomicU64,
}

impl Inner {
    async fn do_wait_connection_state(&self) {
        let request = self.request.lock().await.take();
        if request.is_none() {
            return;
        }
        let request = request.unwrap();
        if request.is_completed() {
            return;
        }

        if self.default_message_store.get_message_store_config().broker_role == BrokerRole::Slave {
            let connection_state = self.ha_service.get_ha_client().unwrap().get_current_state();
            if connection_state == request.expect_state() {
                request.complete(true).await;
            } else if connection_state == HAConnectionState::Ready {
                if get_current_millis() - self.last_check_time_stamp.load(std::sync::atomic::Ordering::Relaxed)
                    > CONNECTION_ESTABLISH_TIMEOUT
                {
                    error!("Wait HA connection establish with {} timeout", request.remote_addr());
                    request.complete(false).await;
                }
            } else {
                self.last_check_time_stamp
                    .store(get_current_millis(), std::sync::atomic::Ordering::Relaxed);
            }
        } else {
            let mut connection_found = false;
            for connection in self.ha_service.get_connection_list().await {
                if self.check_connection_state_and_notify(connection.as_ref()).await {
                    connection_found = true;
                }
            }

            if connection_found {
                self.last_check_time_stamp
                    .store(get_current_millis(), std::sync::atomic::Ordering::Relaxed);
            }
            if !connection_found
                && (get_current_millis() - self.last_check_time_stamp.load(std::sync::atomic::Ordering::Relaxed)
                    > CONNECTION_ESTABLISH_TIMEOUT)
            {
                error!("Wait HA connection establish with {} timeout", request.remote_addr());
                request.complete(false).await;
            }
        }
    }

    async fn check_connection_state_and_notify(&self, connection: &GeneralHAConnection) -> bool {
        let request = self.request.lock().await.take();
        if request.is_none() {
            return false;
        }
        let request = request.unwrap();
        let remote_addr = connection.remote_address();
        if remote_addr == request.remote_addr() {
            let conn_state = connection.get_current_state().await;
            if conn_state == request.expect_state() {
                request.complete(true).await;
            } else if conn_state == HAConnectionState::Shutdown && request.notify_when_shutdown() {
                request.complete(false).await;
            }
            return true;
        }

        false
    }
}

impl ServiceTask for Inner {
    fn get_service_name(&self) -> String {
        String::from("HAConnectionStateNotificationService")
    }

    async fn run(&self, context: &ServiceContext) {
        while !context.is_stopped() {
            context.wait_for_running(Duration::from_secs(1)).await;
            self.do_wait_connection_state().await;
        }
    }
}

impl HAConnectionStateNotificationService {
    pub fn new(ha_service: GeneralHAService, default_message_store: ArcMut<LocalFileMessageStore>) -> Self {
        let inner = Arc::new(Inner {
            ha_service,
            default_message_store,
            request: Mutex::new(None),
            last_check_time_stamp: AtomicU64::new(0),
        });
        HAConnectionStateNotificationService {
            service_manager: ServiceManager::new_arc(inner.clone()),
            inner,
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.service_manager.shutdown().await;
    }

    pub async fn start(&mut self) -> HAResult<()> {
        match self.service_manager.start().await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to start HAConnectionStateNotificationService, error: {:?}", e);
                Err(HAError::Service(e.to_string()))
            }
        }
    }

    pub async fn check_connection_state_and_notify(&self, connection: &GeneralHAConnection) -> bool {
        self.inner.check_connection_state_and_notify(connection).await
    }

    pub async fn set_request(&self, request: HAConnectionStateNotificationRequest) {
        let mut guard = self.inner.request.lock().await;
        let request_inner = guard.take();
        if let Some(request_inner) = request_inner {
            request_inner.complete(true).await;
        }
        *guard = Some(request);
        self.inner
            .last_check_time_stamp
            .store(get_current_millis(), std::sync::atomic::Ordering::Relaxed);
    }
}
