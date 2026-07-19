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

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rand::RngExt;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::time::Duration;
use tokio::time::Instant;

use crate::common::nameserver_access_config::NameserverAccessConfig;
use crate::implementation::mq_client_api_impl::MQClientAPIImpl;
use crate::runtime::spawn_client_tracked_task;
use crate::runtime::ClientTrackedTaskHandle;

pub struct MQClientAPIFactory {
    nameserver_access_config: NameserverAccessConfig,
    name_prefix: CheetahString,
    clients: Vec<Arc<MQClientAPIImpl>>,
    namesrv_refresh_task: Option<NamesrvRefreshTaskHandle>,
}

enum NamesrvRefreshTaskHandle {
    Tracked {
        stop_signal: Arc<AtomicBool>,
        stop_notify: Arc<Notify>,
        handle: ClientTrackedTaskHandle,
    },
}

impl NamesrvRefreshTaskHandle {
    fn shutdown(self, timeout: Duration) -> bool {
        match self {
            Self::Tracked {
                stop_signal,
                stop_notify,
                handle,
            } => {
                stop_signal.store(true, Ordering::Release);
                stop_notify.notify_waiters();
                let (report, completed) = handle.shutdown_blocking(timeout);
                if !report.is_healthy() {
                    tracing::warn!(
                        report = %report.to_json(),
                        "nameserver domain refresh task shutdown report is unhealthy"
                    );
                }
                completed && report.is_healthy()
            }
        }
    }

    fn is_finished(&self) -> bool {
        match self {
            Self::Tracked { handle, .. } => handle.is_finished(),
        }
    }

    fn task_count(&self) -> usize {
        match self {
            Self::Tracked { handle, .. } => handle.task_count(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct NamesrvRefreshLifecycleProbe {
    pub healthy: bool,
    pub task_count_before_shutdown: usize,
    pub task_count_after_shutdown: usize,
    pub shutdown_elapsed_us: u128,
}

impl MQClientAPIFactory {
    const NAMESRV_DOMAIN_FETCH_INITIAL_DELAY: Duration = Duration::from_secs(10);
    const NAMESRV_DOMAIN_FETCH_INTERVAL: Duration = Duration::from_secs(120);
    const NAMESRV_REFRESH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

    pub fn new(
        nameserver_access_config: NameserverAccessConfig,
        name_prefix: impl Into<CheetahString>,
        clients: Vec<Arc<MQClientAPIImpl>>,
    ) -> RocketMQResult<Self> {
        validate_nameserver_access_config(&nameserver_access_config)?;
        if clients.is_empty() {
            return Err(RocketMQError::illegal_argument(
                "MQClientAPIFactory requires at least one MQClientAPIImpl",
            ));
        }

        Ok(Self {
            nameserver_access_config,
            name_prefix: name_prefix.into(),
            clients,
            namesrv_refresh_task: None,
        })
    }

    #[inline]
    pub fn nameserver_access_config(&self) -> &NameserverAccessConfig {
        &self.nameserver_access_config
    }

    #[inline]
    pub fn name_prefix(&self) -> &CheetahString {
        &self.name_prefix
    }

    #[inline]
    pub fn client_num(&self) -> usize {
        self.clients.len()
    }

    #[inline]
    pub fn get_clients(&self) -> &[Arc<MQClientAPIImpl>] {
        &self.clients
    }

    pub fn get_client(&self) -> RocketMQResult<Arc<MQClientAPIImpl>> {
        match self.clients.len() {
            0 => Err(RocketMQError::not_initialized("MQClientAPIFactory clients")),
            1 => Ok(self.clients[0].clone()),
            len => {
                let index = rand::rng().random_range(0..len);
                Ok(self.clients[index].clone())
            }
        }
    }

    pub async fn create_and_start(
        nameserver_access_config: NameserverAccessConfig,
        name_prefix: impl Into<CheetahString>,
        clients: Vec<Arc<MQClientAPIImpl>>,
    ) -> RocketMQResult<Self> {
        let mut factory = Self::new(nameserver_access_config, name_prefix, clients)?;
        factory.start().await?;
        Ok(factory)
    }

    pub async fn start(&mut self) -> RocketMQResult<()> {
        self.apply_nameserver_access_config().await?;
        self.start_nameserver_domain_refresh();
        for client in &self.clients {
            client.start().await;
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.stop_nameserver_domain_refresh();
        for client in &self.clients {
            client.shutdown();
        }
    }

    pub async fn on_name_server_address_change(&mut self, namesrv_address: impl Into<String>) {
        let namesrv_address = namesrv_address.into();
        for client in &self.clients {
            client
                .on_name_server_address_change(Some(namesrv_address.clone()))
                .await;
        }
    }

    pub async fn apply_nameserver_access_config(&mut self) -> RocketMQResult<()> {
        validate_nameserver_access_config(&self.nameserver_access_config)?;
        if !self.nameserver_access_config.namesrv_domain().is_empty() {
            for client in &self.clients {
                client.fetch_name_server_addr().await;
            }
            return Ok(());
        }

        let namesrv_addr = self.nameserver_access_config.namesrv_addr().as_str();
        for client in &self.clients {
            client.update_name_server_address_list(namesrv_addr).await;
        }
        Ok(())
    }

    fn start_nameserver_domain_refresh(&mut self) {
        self.stop_nameserver_domain_refresh();
        if self.nameserver_access_config.namesrv_domain().is_empty() {
            return;
        }

        let clients = self.clients.clone();
        let stop_signal = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());
        self.namesrv_refresh_task = spawn_namesrv_refresh_task(
            "rocketmq-client-namesrv-refresh",
            stop_signal.clone(),
            stop_notify.clone(),
            async move {
                if !sleep_or_stop(&stop_signal, &stop_notify, Self::NAMESRV_DOMAIN_FETCH_INITIAL_DELAY).await {
                    return;
                }

                loop {
                    for client in &clients {
                        client.fetch_name_server_addr().await;
                    }

                    if !sleep_or_stop(&stop_signal, &stop_notify, Self::NAMESRV_DOMAIN_FETCH_INTERVAL).await {
                        break;
                    }
                }
            },
        );
    }

    fn stop_nameserver_domain_refresh(&mut self) {
        if let Some(task) = self.namesrv_refresh_task.take() {
            if !task.shutdown(Self::NAMESRV_REFRESH_SHUTDOWN_TIMEOUT) {
                tracing::warn!("nameserver domain refresh task did not stop before timeout; aborted");
            }
        }
    }

    fn namesrv_refresh_task_count(&self) -> usize {
        self.namesrv_refresh_task
            .as_ref()
            .map(NamesrvRefreshTaskHandle::task_count)
            .unwrap_or_default()
    }
}

fn validate_nameserver_access_config(config: &NameserverAccessConfig) -> RocketMQResult<()> {
    if config.namesrv_domain().is_empty() && config.namesrv_addr().is_empty() {
        return Err(RocketMQError::illegal_argument(
            "The configuration item NamesrvAddr is not configured",
        ));
    }
    Ok(())
}

fn spawn_namesrv_refresh_task<F>(
    thread_name: &'static str,
    stop_signal: Arc<AtomicBool>,
    stop_notify: Arc<Notify>,
    task: F,
) -> Option<NamesrvRefreshTaskHandle>
where
    F: Future<Output = ()> + Send + 'static,
{
    match spawn_client_tracked_task(thread_name, task) {
        Ok(handle) => Some(NamesrvRefreshTaskHandle::Tracked {
            stop_signal,
            stop_notify,
            handle,
        }),
        Err(error) => {
            tracing::warn!("Failed to spawn {} background task: {}", thread_name, error);
            None
        }
    }
}

async fn sleep_or_stop(stop_signal: &Arc<AtomicBool>, stop_notify: &Arc<Notify>, delay: Duration) -> bool {
    if stop_signal.load(Ordering::Acquire) {
        return false;
    }

    tokio::time::timeout(delay, stop_notify.notified()).await.is_err() && !stop_signal.load(Ordering::Acquire)
}

#[doc(hidden)]
pub async fn run_namesrv_refresh_lifecycle_probe() -> NamesrvRefreshLifecycleProbe {
    let mut factory = MQClientAPIFactory {
        nameserver_access_config: NameserverAccessConfig::new("", "example.com", "default"),
        name_prefix: CheetahString::from_static_str("factory-lifecycle-probe"),
        clients: Vec::new(),
        namesrv_refresh_task: None,
    };

    factory.start_nameserver_domain_refresh();
    let task_count_before_shutdown = factory.namesrv_refresh_task_count();

    let shutdown_started = Instant::now();
    factory.stop_nameserver_domain_refresh();
    let shutdown_elapsed_us = shutdown_started.elapsed().as_micros();
    let task_count_after_shutdown = factory.namesrv_refresh_task_count();

    NamesrvRefreshLifecycleProbe {
        healthy: task_count_before_shutdown == 1 && task_count_after_shutdown == 0,
        task_count_before_shutdown,
        task_count_after_shutdown,
        shutdown_elapsed_us,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn factory_rejects_missing_nameserver_like_java() {
        let error = validate_nameserver_access_config(&NameserverAccessConfig::default())
            .expect_err("missing namesrvAddr and namesrvDomain should be invalid");

        assert!(error.to_string().contains("NamesrvAddr is not configured"));
    }

    #[test]
    fn factory_accepts_static_nameserver_address() {
        validate_nameserver_access_config(&NameserverAccessConfig::new("127.0.0.1:9876", "", ""))
            .expect("static namesrvAddr should be valid");
    }

    #[test]
    fn factory_accepts_nameserver_domain() {
        validate_nameserver_access_config(&NameserverAccessConfig::new("", "domain", "subgroup"))
            .expect("namesrvDomain should be valid");
    }

    #[tokio::test]
    async fn create_and_start_validates_before_starting() {
        let result = MQClientAPIFactory::create_and_start(
            NameserverAccessConfig::new("127.0.0.1:9876", "", ""),
            "factory-test",
            Vec::new(),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("empty client list should be rejected before start"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("requires at least one MQClientAPIImpl"));
    }

    #[tokio::test]
    async fn domain_nameserver_config_starts_periodic_refresh_like_java() {
        let mut factory = MQClientAPIFactory {
            nameserver_access_config: NameserverAccessConfig::new("", "example.com", "default"),
            name_prefix: CheetahString::from_static_str("factory-test"),
            clients: Vec::new(),
            namesrv_refresh_task: None,
        };

        factory.start_nameserver_domain_refresh();

        assert!(factory
            .namesrv_refresh_task
            .as_ref()
            .is_some_and(|task| !task.is_finished()));

        factory.shutdown();

        assert!(factory.namesrv_refresh_task.is_none());
    }

    #[tokio::test]
    async fn static_nameserver_config_does_not_start_periodic_refresh() {
        let mut factory = MQClientAPIFactory {
            nameserver_access_config: NameserverAccessConfig::new("127.0.0.1:9876", "", ""),
            name_prefix: CheetahString::from_static_str("factory-test"),
            clients: Vec::new(),
            namesrv_refresh_task: None,
        };

        factory.start_nameserver_domain_refresh();

        assert!(factory.namesrv_refresh_task.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn namesrv_refresh_task_shutdown_waits_for_worker_completion() {
        let stop_signal = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_task = completed.clone();
        let stop_signal_in_task = stop_signal.clone();
        let stop_notify_in_task = stop_notify.clone();
        let task = spawn_namesrv_refresh_task(
            "rocketmq-client-namesrv-refresh-test",
            stop_signal.clone(),
            stop_notify.clone(),
            async move {
                let _ = sleep_or_stop(&stop_signal_in_task, &stop_notify_in_task, Duration::from_secs(60)).await;
                completed_in_task.store(true, Ordering::Release);
            },
        )
        .expect("test task should spawn");

        assert!(task.shutdown(Duration::from_secs(1)));
        assert!(completed.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn namesrv_refresh_task_shutdown_aborts_after_timeout() {
        let stop_signal = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_in_task = dropped.clone();
        let task = spawn_namesrv_refresh_task(
            "rocketmq-client-namesrv-refresh-test",
            stop_signal,
            stop_notify,
            async move {
                let _drop_flag = DropFlag(dropped_in_task);
                pending::<()>().await;
            },
        )
        .expect("test task should spawn");

        assert!(!task.shutdown(Duration::from_millis(20)));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("aborted task should be dropped");
    }

    #[tokio::test]
    async fn sleep_or_stop_wakes_on_notify() {
        let stop_signal = Arc::new(AtomicBool::new(false));
        let stop_notify = Arc::new(Notify::new());
        let signal = stop_signal.clone();
        let notify = stop_notify.clone();

        let wait = tokio::spawn(async move { sleep_or_stop(&signal, &notify, Duration::from_secs(60)).await });

        stop_signal.store(true, Ordering::Release);
        stop_notify.notify_waiters();

        assert!(!tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("sleep_or_stop should wake promptly")
            .expect("sleep task should complete"));
    }

    #[tokio::test]
    async fn namesrv_refresh_lifecycle_probe_reports_clean_shutdown() {
        let probe = run_namesrv_refresh_lifecycle_probe().await;

        assert!(probe.healthy, "{probe:?}");
        assert_eq!(probe.task_count_before_shutdown, 1);
        assert_eq!(probe.task_count_after_shutdown, 0);
    }

    #[test]
    fn domain_refresh_without_tokio_runtime_does_not_panic() {
        let mut factory = MQClientAPIFactory {
            nameserver_access_config: NameserverAccessConfig::new("", "example.com", "default"),
            name_prefix: CheetahString::from_static_str("factory-test"),
            clients: Vec::new(),
            namesrv_refresh_task: None,
        };

        factory.start_nameserver_domain_refresh();

        assert!(factory
            .namesrv_refresh_task
            .as_ref()
            .is_some_and(|task| !task.is_finished()));

        factory.shutdown();

        assert!(factory.namesrv_refresh_task.is_none());
    }
}
