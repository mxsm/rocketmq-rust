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
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::time::Duration;

use rocketmq_admin_core::client_adapter::AdminGuard;
use rocketmq_runtime::TaskGroup;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::RwLock;

use super::AdminConfigSnapshot;
use super::admin_config_snapshot;
use crate::error::DashboardError;
use crate::model::DashboardConfigView;

impl TopicAdminSessionGuard for AdminGuard {
    fn shutdown_in_place<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.inner_mut().shutdown().await;
        })
    }
}

pub(super) trait TopicAdminSessionGuard: Send {
    fn shutdown_in_place<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

pub(super) struct TopicAdminSessionRegistry<G> {
    state: Mutex<TopicAdminSessionState<G>>,
    builders_drained: Notify,
}

struct TopicAdminSessionState<G> {
    lifecycle: TopicAdminSessionLifecycle,
    builders: usize,
    current: Option<Arc<ManagedTopicAdminSession<G>>>,
    retired: Vec<Arc<ManagedTopicAdminSession<G>>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TopicAdminSessionLifecycle {
    Open,
    Closing,
    Closed,
}

pub(super) struct ManagedTopicAdminSession<G> {
    guard: AsyncMutex<Option<G>>,
    snapshot: AdminConfigSnapshot,
}

struct TopicAdminBuildPermit<'a, G> {
    registry: &'a TopicAdminSessionRegistry<G>,
    active: bool,
}

impl<G> Default for TopicAdminSessionRegistry<G> {
    fn default() -> Self {
        Self {
            state: Mutex::new(TopicAdminSessionState {
                lifecycle: TopicAdminSessionLifecycle::Open,
                builders: 0,
                current: None,
                retired: Vec::new(),
            }),
            builders_drained: Notify::new(),
        }
    }
}

impl<G> TopicAdminSessionRegistry<G> {
    fn state(&self) -> MutexGuard<'_, TopicAdminSessionState<G>> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn abandon_build(&self) {
        let mut state = self.state();
        state.builders = state.builders.saturating_sub(1);
        if state.builders == 0 {
            self.builders_drained.notify_waiters();
        }
    }

    #[cfg(test)]
    pub(super) fn has_current(&self) -> bool {
        self.state().current.is_some()
    }
}

impl<G> TopicAdminSessionRegistry<G>
where
    G: TopicAdminSessionGuard,
{
    pub(super) async fn acquire<F, BuildFuture>(
        &self,
        snapshot: AdminConfigSnapshot,
        build: F,
    ) -> Result<Arc<ManagedTopicAdminSession<G>>, DashboardError>
    where
        F: FnOnce() -> BuildFuture,
        BuildFuture: Future<Output = Result<G, DashboardError>>,
    {
        self.reap_retired().await;
        if let Some(session) = self.current(&snapshot)? {
            return Ok(session);
        }

        let permit = self.begin_build()?;
        let guard = build().await?;
        let session = permit.finish(snapshot, guard)?;
        self.reap_retired().await;
        Ok(session)
    }

    pub(super) fn close_admission(&self) {
        let mut state = self.state();
        if state.lifecycle == TopicAdminSessionLifecycle::Open {
            state.lifecycle = TopicAdminSessionLifecycle::Closing;
        }
    }

    pub(super) async fn wait_for_builders(&self) {
        loop {
            let notified = self.builders_drained.notified();
            if self.state().builders == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn shutdown(&self) {
        let sessions = {
            let state = self.state();
            let mut all = state.retired.clone();
            all.extend(state.current.iter().cloned());
            all
        };
        for session in &sessions {
            session.shutdown().await;
        }
        let mut state = self.state();
        state.current = None;
        state.retired.clear();
        state.lifecycle = TopicAdminSessionLifecycle::Closed;
    }

    pub(super) async fn reap_retired(&self) {
        let retired = self.state().retired.clone();
        for session in &retired {
            session.shutdown().await;
        }
        if retired.is_empty() {
            return;
        }
        let mut state = self.state();
        state
            .retired
            .retain(|candidate| !retired.iter().any(|retired| Arc::ptr_eq(candidate, retired)));
    }

    fn current(
        &self,
        snapshot: &AdminConfigSnapshot,
    ) -> Result<Option<Arc<ManagedTopicAdminSession<G>>>, DashboardError> {
        let state = self.state();
        if state.lifecycle != TopicAdminSessionLifecycle::Open {
            return Err(closing_error());
        }
        Ok(state
            .current
            .as_ref()
            .filter(|session| session.snapshot == *snapshot)
            .cloned())
    }

    fn begin_build(&self) -> Result<TopicAdminBuildPermit<'_, G>, DashboardError> {
        let mut state = self.state();
        if state.lifecycle != TopicAdminSessionLifecycle::Open {
            return Err(closing_error());
        }
        state.builders += 1;
        Ok(TopicAdminBuildPermit {
            registry: self,
            active: true,
        })
    }

    fn complete_build(
        &self,
        snapshot: AdminConfigSnapshot,
        guard: G,
    ) -> Result<Arc<ManagedTopicAdminSession<G>>, DashboardError> {
        let candidate = Arc::new(ManagedTopicAdminSession {
            guard: AsyncMutex::new(Some(guard)),
            snapshot: snapshot.clone(),
        });
        let mut state = self.state();
        let selected = if state.lifecycle == TopicAdminSessionLifecycle::Open {
            if let Some(current) = state
                .current
                .as_ref()
                .filter(|current| current.snapshot == snapshot)
                .cloned()
            {
                state.retired.push(candidate);
                Ok(current)
            } else {
                if let Some(previous) = state.current.replace(Arc::clone(&candidate)) {
                    state.retired.push(previous);
                }
                Ok(candidate)
            }
        } else {
            state.retired.push(candidate);
            Err(closing_error())
        };
        state.builders = state.builders.saturating_sub(1);
        if state.builders == 0 {
            self.builders_drained.notify_waiters();
        }
        selected
    }

    pub(super) fn retire(&self, session: &Arc<ManagedTopicAdminSession<G>>) {
        let mut state = self.state();
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            state.current = None;
        }
        if !state.retired.iter().any(|retired| Arc::ptr_eq(retired, session)) {
            state.retired.push(Arc::clone(session));
        }
    }
}

impl<G> ManagedTopicAdminSession<G>
where
    G: TopicAdminSessionGuard,
{
    async fn shutdown(&self) {
        let mut guard = self.guard.lock().await;
        if let Some(guard) = guard.as_mut() {
            guard.shutdown_in_place().await;
        }
        guard.take();
    }
}

impl<G> TopicAdminBuildPermit<'_, G>
where
    G: TopicAdminSessionGuard,
{
    fn finish(
        mut self,
        snapshot: AdminConfigSnapshot,
        guard: G,
    ) -> Result<Arc<ManagedTopicAdminSession<G>>, DashboardError> {
        self.active = false;
        self.registry.complete_build(snapshot, guard)
    }
}

impl<G> Drop for TopicAdminBuildPermit<'_, G> {
    fn drop(&mut self) {
        if self.active {
            self.registry.abandon_build();
        }
    }
}

pub(super) async fn shutdown_topic_admin_services<G>(
    sessions: &TopicAdminSessionRegistry<G>,
    task_group: &TaskGroup,
    timeout: Duration,
) -> rocketmq_runtime::ShutdownReport
where
    G: TopicAdminSessionGuard,
{
    sessions.close_admission();
    task_group.cancel();
    let report = task_group.shutdown(timeout).await;
    sessions.wait_for_builders().await;
    sessions.shutdown().await;
    report
}

pub(super) async fn run_tracked_topic_admin_service<
    G,
    T,
    Build,
    BuildFuture,
    ConfigCancellation,
    GuardCancellation,
    OperationCancellation,
    Operation,
>(
    sessions: &TopicAdminSessionRegistry<G>,
    config: &RwLock<DashboardConfigView>,
    snapshot: AdminConfigSnapshot,
    config_cancellation: ConfigCancellation,
    guard_cancellation: GuardCancellation,
    operation_cancellation: OperationCancellation,
    build: Build,
    operation: Operation,
) -> Result<T, DashboardError>
where
    G: TopicAdminSessionGuard,
    Build: FnOnce() -> BuildFuture,
    BuildFuture: Future<Output = Result<G, DashboardError>>,
    ConfigCancellation: Future<Output = ()>,
    GuardCancellation: Future<Output = ()>,
    OperationCancellation: Future<Output = ()>,
    Operation:
        for<'guard> FnOnce(&'guard mut G) -> Pin<Box<dyn Future<Output = Result<T, DashboardError>> + Send + 'guard>>,
{
    let session = sessions.acquire(snapshot, build).await?;
    let operation_result: Result<Option<Result<T, DashboardError>>, DashboardError> = {
        let mut guard = tokio::select! {
            biased;
            _ = guard_cancellation => return Err(cancellation_error()),
            guard = session.guard.lock() => guard,
        };
        let guard = guard
            .as_mut()
            .ok_or_else(|| DashboardError::Internal("Topic admin session was already retired".to_string()))?;
        match tokio::select! {
            biased;
            _ = config_cancellation => Err(cancellation_error()),
            current = admin_config_snapshot(config) => current,
        } {
            Ok(current) if current == session.snapshot => Ok(Some(tokio::select! {
                biased;
                _ = operation_cancellation => Err(cancellation_error()),
                result = operation(guard) => result,
            })),
            Ok(_) => Ok(None),
            Err(error) => Err(error),
        }
    };
    let result = match operation_result {
        Ok(Some(result)) => result,
        Ok(None) => {
            return retire_and_return(
                sessions,
                &session,
                DashboardError::Config(
                    "Dashboard admin configuration changed while opening a topic admin session; retry the request"
                        .to_string(),
                ),
            )
            .await;
        }
        Err(error) => return retire_and_return(sessions, &session, error).await,
    };

    match admin_config_snapshot(config).await {
        Ok(current) if current == session.snapshot => {}
        Ok(_) => {
            return retire_and_return(
                sessions,
                &session,
                DashboardError::Config(
                    "Dashboard admin configuration changed while executing a topic admin session; retry the request"
                        .to_string(),
                ),
            )
            .await;
        }
        Err(error) => return retire_and_return(sessions, &session, error).await,
    }
    sessions.reap_retired().await;
    result
}

async fn retire_and_return<G, T>(
    sessions: &TopicAdminSessionRegistry<G>,
    session: &Arc<ManagedTopicAdminSession<G>>,
    error: DashboardError,
) -> Result<T, DashboardError>
where
    G: TopicAdminSessionGuard,
{
    sessions.retire(session);
    sessions.reap_retired().await;
    Err(error)
}

fn closing_error() -> DashboardError {
    DashboardError::Config("Topic admin operation was cancelled during shutdown".to_string())
}

fn cancellation_error() -> DashboardError {
    DashboardError::Config("Topic admin operation was cancelled during shutdown".to_string())
}
