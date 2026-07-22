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
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_runtime::task::service_task::ServiceContext;
use rocketmq_runtime::task::service_task::ServiceTask;
use rocketmq_runtime::task::ServiceManager;
use tokio::sync::Mutex;
use tracing::error;

use crate::config::message_store_config::MessageStoreConfig;
use crate::ha::general_ha_service::GeneralHAServiceReference;
use crate::ha::ha_client::HAClient;
use crate::ha::ha_connection_state::HAConnectionState;
use crate::ha::ha_connection_state_notification_request::HAConnectionStateNotificationRequest;
use crate::ha::ha_service::HAService;
use crate::store_error::HAError;
use crate::store_error::HAResult;

const CONNECTION_ESTABLISH_TIMEOUT: u64 = 10 * 1000;

pub struct HAConnectionStateNotificationService {
    inner: Arc<Inner>,
    service_manager: ServiceManager<Inner>,
}

struct Inner {
    ha_service: GeneralHAServiceReference,
    message_store_config: Arc<MessageStoreConfig>,
    request: Mutex<Option<HAConnectionStateNotificationRequest>>,
    last_check_time_stamp: AtomicU64,
}

impl Inner {
    async fn do_wait_connection_state(&self) {
        let remote_addr = {
            let mut request_guard = self.request.lock().await;
            let Some(request) = request_guard.as_ref() else {
                return;
            };
            if request.is_completed() {
                request_guard.take();
                return;
            }
            request.remote_addr().to_string()
        };

        let Some(ha_service) = self.ha_service.upgrade() else {
            self.complete_timed_out_request(&remote_addr).await;
            return;
        };

        if uses_slave_connection_path(&self.message_store_config) {
            let Some(ha_client) = ha_service.get_ha_client() else {
                self.complete_timed_out_request(&remote_addr).await;
                return;
            };
            let connection_state = ha_client.get_current_state();
            match self
                .check_connection_state_and_notify(&remote_addr, connection_state)
                .await
            {
                ConnectionNotificationMatch::Pending if connection_state == HAConnectionState::Ready => {
                    self.complete_timed_out_request(&remote_addr).await;
                }
                ConnectionNotificationMatch::Pending => {
                    self.last_check_time_stamp
                        .store(current_millis(), std::sync::atomic::Ordering::Relaxed);
                }
                ConnectionNotificationMatch::Missing | ConnectionNotificationMatch::Completed => {}
            }
        } else {
            match ha_service.connection_state(&remote_addr).await {
                Some(connection_state) => {
                    let connection_match = self
                        .check_connection_state_and_notify(&remote_addr, connection_state)
                        .await;
                    if connection_match != ConnectionNotificationMatch::Missing {
                        self.last_check_time_stamp
                            .store(current_millis(), std::sync::atomic::Ordering::Relaxed);
                    }
                }
                None => self.complete_timed_out_request(&remote_addr).await,
            }
        }
    }

    async fn check_connection_state_and_notify(
        &self,
        remote_addr: &str,
        connection_state: HAConnectionState,
    ) -> ConnectionNotificationMatch {
        notify_matching_request(&self.request, remote_addr, connection_state).await
    }

    async fn complete_timed_out_request(&self, remote_addr: &str) {
        if !self.connection_wait_timed_out() {
            return;
        }

        if fail_matching_request(&self.request, remote_addr).await {
            error!("Wait HA connection establish with {} timeout", remote_addr);
        }
    }

    fn connection_wait_timed_out(&self) -> bool {
        current_millis().saturating_sub(self.last_check_time_stamp.load(std::sync::atomic::Ordering::Relaxed))
            > CONNECTION_ESTABLISH_TIMEOUT
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionNotificationMatch {
    Missing,
    Pending,
    Completed,
}

async fn notify_matching_request(
    request_slot: &Mutex<Option<HAConnectionStateNotificationRequest>>,
    remote_addr: &str,
    connection_state: HAConnectionState,
) -> ConnectionNotificationMatch {
    let mut request_guard = request_slot.lock().await;
    let Some(request) = request_guard.as_ref() else {
        return ConnectionNotificationMatch::Missing;
    };
    if request.remote_addr() != remote_addr {
        return ConnectionNotificationMatch::Missing;
    }

    let result = if connection_state == request.expect_state() {
        Some(true)
    } else if connection_state == HAConnectionState::Shutdown && request.notify_when_shutdown() {
        Some(false)
    } else {
        None
    };
    let Some(result) = result else {
        return ConnectionNotificationMatch::Pending;
    };

    let Some(request) = request_guard.take() else {
        return ConnectionNotificationMatch::Missing;
    };
    request.complete(result).await;
    ConnectionNotificationMatch::Completed
}

async fn fail_matching_request(
    request_slot: &Mutex<Option<HAConnectionStateNotificationRequest>>,
    remote_addr: &str,
) -> bool {
    let mut request_guard = request_slot.lock().await;
    if !request_guard
        .as_ref()
        .is_some_and(|request| request.remote_addr() == remote_addr)
    {
        return false;
    }

    let Some(request) = request_guard.take() else {
        return false;
    };
    request.complete(false).await;
    true
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
    pub fn new(ha_service: GeneralHAServiceReference, message_store_config: Arc<MessageStoreConfig>) -> Self {
        let inner = Arc::new(Inner {
            ha_service,
            message_store_config,
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

    pub async fn start(&self) -> HAResult<()> {
        match self.service_manager.start().await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to start HAConnectionStateNotificationService, error: {:?}", e);
                Err(HAError::Service(e.to_string()))
            }
        }
    }

    pub async fn check_connection_state_and_notify(
        &self,
        remote_addr: &str,
        connection_state: HAConnectionState,
    ) -> bool {
        self.inner
            .check_connection_state_and_notify(remote_addr, connection_state)
            .await
            != ConnectionNotificationMatch::Missing
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
            .store(current_millis(), std::sync::atomic::Ordering::Relaxed);
    }
}

fn uses_slave_connection_path(message_store_config: &MessageStoreConfig) -> bool {
    message_store_config.broker_role == BrokerRole::Slave
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_connection_path_from_injected_store_config() {
        let mut config = MessageStoreConfig {
            broker_role: BrokerRole::Slave,
            ..Default::default()
        };
        assert!(uses_slave_connection_path(&config));

        config.broker_role = BrokerRole::AsyncMaster;
        assert!(!uses_slave_connection_path(&config));

        config.broker_role = BrokerRole::SyncMaster;
        assert!(!uses_slave_connection_path(&config));
    }

    #[test]
    fn notification_service_does_not_retain_concrete_store_ownership() {
        let source = include_str!("ha_connection_state_notification_service.rs");
        let service_source = include_str!("ha_service.rs");
        let forbidden_handle = concat!("Arc", "Mut");
        let forbidden_store = concat!("LocalFile", "MessageStore");
        let forbidden_connection_list = concat!("get_connection", "_list");

        assert!(!source.contains(forbidden_handle));
        assert!(!source.contains(forbidden_store));
        assert!(!source.contains(forbidden_connection_list));
        assert!(source.contains("message_store_config: Arc<MessageStoreConfig>"));
        assert!(!service_source.contains(forbidden_handle));
        assert!(!service_source.contains(forbidden_connection_list));
        assert!(service_source.contains("snapshot_acked_replicas"));
        assert!(service_source.contains("connection_state"));
    }

    #[tokio::test]
    async fn nonterminal_state_keeps_request_until_expected_state_arrives() {
        let (request, mut receiver) =
            HAConnectionStateNotificationRequest::new(HAConnectionState::Transfer, "127.0.0.1:10912", true);
        let request_slot = Mutex::new(Some(request));

        assert_eq!(
            notify_matching_request(&request_slot, "127.0.0.1:10912", HAConnectionState::Ready).await,
            ConnectionNotificationMatch::Pending
        );
        assert!(request_slot.lock().await.is_some());
        assert!(receiver.try_recv().is_err());

        assert_eq!(
            notify_matching_request(&request_slot, "127.0.0.1:10912", HAConnectionState::Transfer).await,
            ConnectionNotificationMatch::Completed
        );
        assert!(request_slot.lock().await.is_none());
        assert!(receiver.await.expect("expected-state notification"));
    }

    #[tokio::test]
    async fn mismatched_address_keeps_request_registered() {
        let (request, mut receiver) =
            HAConnectionStateNotificationRequest::new(HAConnectionState::Transfer, "127.0.0.1:10912", true);
        let request_slot = Mutex::new(Some(request));

        assert_eq!(
            notify_matching_request(&request_slot, "127.0.0.1:10913", HAConnectionState::Transfer).await,
            ConnectionNotificationMatch::Missing
        );
        assert!(request_slot.lock().await.is_some());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn shutdown_and_timeout_complete_matching_requests_with_failure() {
        let (shutdown_request, shutdown_receiver) =
            HAConnectionStateNotificationRequest::new(HAConnectionState::Transfer, "127.0.0.1:10912", true);
        let shutdown_slot = Mutex::new(Some(shutdown_request));
        assert_eq!(
            notify_matching_request(&shutdown_slot, "127.0.0.1:10912", HAConnectionState::Shutdown).await,
            ConnectionNotificationMatch::Completed
        );
        assert!(!shutdown_receiver.await.expect("shutdown notification"));

        let (timeout_request, timeout_receiver) =
            HAConnectionStateNotificationRequest::new(HAConnectionState::Transfer, "127.0.0.1:10912", false);
        let timeout_slot = Mutex::new(Some(timeout_request));
        assert!(fail_matching_request(&timeout_slot, "127.0.0.1:10912").await);
        assert!(!timeout_receiver.await.expect("timeout notification"));
    }
}
