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
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use rocketmq_runtime::ChildServiceContext;
use rocketmq_transport::api::v1::ConnectionNetEvent;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::runtime::spawn_client_tracked_task_with_context;
use crate::runtime::ClientRuntime;
use crate::runtime::ClientTrackedTaskHandle;

use super::MQClientInstance;

async fn run(
    mut rx: tokio::sync::broadcast::Receiver<ConnectionNetEvent>,
    weak_instance: Weak<MQClientInstance>,
    shutdown_token: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => break,
            event = rx.recv() => {
                let Ok(value) = event else {
                    warn!("ConnectionNetEvent recv error");
                    break;
                };

                if let Some(instance) = weak_instance.upgrade() {
                    match value {
                        ConnectionNetEvent::CONNECTED(remote_address) => {
                            info!("ConnectionNetEvent CONNECTED");
                            instance.on_channel_active(&remote_address.to_string()).await;
                        }
                        ConnectionNetEvent::DISCONNECTED => instance.on_channel_close(""),
                        ConnectionNetEvent::EXCEPTION => instance.on_channel_exception(""),
                        ConnectionNetEvent::IDLE => instance.on_channel_idle(""),
                    }
                }
            },
        }
    }
}

pub(super) fn spawn(
    service_context: &ChildServiceContext,
    rx: tokio::sync::broadcast::Receiver<ConnectionNetEvent>,
    weak_instance: Weak<MQClientInstance>,
    shutdown_token: CancellationToken,
) -> Option<ClientTrackedTaskHandle> {
    match spawn_client_tracked_task_with_context(
        service_context,
        "rocketmq-client-connection-events",
        run(rx, weak_instance, shutdown_token),
    ) {
        Ok(handle) => Some(handle),
        Err(error) => {
            error!(
                "Failed to spawn MQClientInstance connection event listener task: {}",
                error
            );
            None
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionEventListenerLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
}

#[doc(hidden)]
pub async fn run_lifecycle_probe(client_runtime: Arc<ClientRuntime>) -> ConnectionEventListenerLifecycleProbe {
    let client_config = ClientConfig {
        namesrv_addr: None,
        ..Default::default()
    };
    let instance = super::new_probe_client_instance(&client_runtime, client_config, "connection-event-listener-probe");
    let task_count_before_shutdown = instance.connection_event_task_count();

    let shutdown_started = Instant::now();
    instance
        .shutdown_connection_event_listener(Duration::from_secs(1))
        .await;
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = instance.connection_event_task_count();

    ConnectionEventListenerLifecycleProbe {
        healthy: task_count_before_shutdown == 1 && task_count_after_shutdown == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
    }
}
