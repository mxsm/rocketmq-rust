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

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;

use rocketmq_protocol::code::request_code::RequestCode;

/// Simultaneous item and retained-byte limit for one transport resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimit {
    pub count: usize,
    pub bytes: usize,
}

/// Explicit limits for every resource admitted by the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    pub connections: ResourceLimit,
    pub handshakes: ResourceLimit,
    pub inflight: ResourceLimit,
    pub queued: ResourceLimit,
    pub processors: ResourceLimit,
    pub per_ip: ResourceLimit,
    pub per_tenant: ResourceLimit,
    pub per_session: ResourceLimit,
    pub control_reserve: ResourceLimit,
    pub max_scope_keys: usize,
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            connections: ResourceLimit {
                count: 16_384,
                bytes: 256 * 1024 * 1024,
            },
            handshakes: ResourceLimit {
                count: 1_024,
                bytes: 64 * 1024 * 1024,
            },
            inflight: ResourceLimit {
                count: 65_536,
                bytes: 256 * 1024 * 1024,
            },
            queued: ResourceLimit {
                count: 65_536,
                bytes: 256 * 1024 * 1024,
            },
            processors: ResourceLimit {
                count: 4_096,
                bytes: 128 * 1024 * 1024,
            },
            per_ip: ResourceLimit {
                count: 4_096,
                bytes: 64 * 1024 * 1024,
            },
            per_tenant: ResourceLimit {
                count: 16_384,
                bytes: 128 * 1024 * 1024,
            },
            per_session: ResourceLimit {
                count: 4_096,
                bytes: 64 * 1024 * 1024,
            },
            control_reserve: ResourceLimit {
                count: 64,
                bytes: 1024 * 1024,
            },
            max_scope_keys: 16_384,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdmissionResource {
    Connection,
    Handshake,
    Inflight,
    Queued,
    Processor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionClass {
    Data,
    Control,
}

impl AdmissionClass {
    pub fn for_request_code(code: i32) -> Self {
        match RequestCode::from(code) {
            RequestCode::HeartBeat
            | RequestCode::RegisterBroker
            | RequestCode::UnregisterBroker
            | RequestCode::GetRouteinfoByTopic
            | RequestCode::GetBrokerClusterInfo
            | RequestCode::GetBrokerRuntimeInfo => Self::Control,
            _ => Self::Data,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullPolicy {
    Reject,
    CloseConnection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionScope {
    ip: IpAddr,
    tenant: Option<u64>,
    session: Option<u64>,
}

impl AdmissionScope {
    pub fn new(ip: IpAddr) -> Self {
        Self {
            ip,
            tenant: None,
            session: None,
        }
    }

    pub fn with_tenant(mut self, tenant: u64) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn with_session(mut self, session: u64) -> Self {
        self.session = Some(session);
        self
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceSnapshot {
    pub current_count: usize,
    pub current_bytes: usize,
    pub rejected_count: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    pub connections: ResourceSnapshot,
    pub handshakes: ResourceSnapshot,
    pub inflight: ResourceSnapshot,
    pub queued: ResourceSnapshot,
    pub processors: ResourceSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOutcome {
    Acquired,
    Rejected,
    Released,
}

/// Low-cardinality metric event. Scope identities are deliberately omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionEvent {
    pub resource: AdmissionResource,
    pub outcome: AdmissionOutcome,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionError {
    resource: AdmissionResource,
    policy: FullPolicy,
}

impl AdmissionError {
    pub fn policy(&self) -> FullPolicy {
        self.policy
    }
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?} admission capacity exhausted", self.resource)
    }
}

impl std::error::Error for AdmissionError {}

#[derive(Default)]
struct Usage {
    count: usize,
    bytes: usize,
    data_count: usize,
    data_bytes: usize,
    rejected: usize,
}

struct Budget {
    limit: ResourceLimit,
    reserve: ResourceLimit,
    usage: Mutex<Usage>,
}

impl Budget {
    fn new(limit: ResourceLimit, reserve: ResourceLimit) -> Self {
        let reserve = ResourceLimit {
            count: if reserve.count < limit.count { reserve.count } else { 0 },
            bytes: if reserve.bytes < limit.bytes { reserve.bytes } else { 0 },
        };
        Self {
            limit,
            reserve,
            usage: Mutex::new(Usage::default()),
        }
    }

    fn try_acquire(self: &Arc<Self>, bytes: usize, class: AdmissionClass) -> Option<BudgetPermit> {
        let mut usage = self.usage.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let total_fits = usage.count < self.limit.count
            && usage
                .bytes
                .checked_add(bytes)
                .is_some_and(|value| value <= self.limit.bytes);
        let data_fits = class == AdmissionClass::Control
            || (usage.data_count < self.limit.count.saturating_sub(self.reserve.count)
                && usage
                    .data_bytes
                    .checked_add(bytes)
                    .is_some_and(|value| value <= self.limit.bytes.saturating_sub(self.reserve.bytes)));
        if !total_fits || !data_fits {
            usage.rejected += 1;
            return None;
        }
        usage.count += 1;
        usage.bytes += bytes;
        if class == AdmissionClass::Data {
            usage.data_count += 1;
            usage.data_bytes += bytes;
        }
        drop(usage);
        Some(BudgetPermit {
            budget: self.clone(),
            bytes,
            class,
        })
    }

    fn snapshot(&self) -> ResourceSnapshot {
        let usage = self.usage.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        ResourceSnapshot {
            current_count: usage.count,
            current_bytes: usage.bytes,
            rejected_count: usage.rejected,
        }
    }

    fn is_idle(&self) -> bool {
        let usage = self.usage.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        usage.count == 0
    }
}

struct BudgetPermit {
    budget: Arc<Budget>,
    bytes: usize,
    class: AdmissionClass,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        let mut usage = self
            .budget
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        usage.count -= 1;
        usage.bytes -= self.bytes;
        if self.class == AdmissionClass::Data {
            usage.data_count -= 1;
            usage.data_bytes -= self.bytes;
        }
    }
}

struct GlobalBudgets {
    connections: Arc<Budget>,
    handshakes: Arc<Budget>,
    inflight: Arc<Budget>,
    queued: Arc<Budget>,
    processors: Arc<Budget>,
}

impl GlobalBudgets {
    fn new(limits: AdmissionLimits) -> Self {
        Self {
            connections: Arc::new(Budget::new(limits.connections, limits.control_reserve)),
            handshakes: Arc::new(Budget::new(limits.handshakes, limits.control_reserve)),
            inflight: Arc::new(Budget::new(limits.inflight, limits.control_reserve)),
            queued: Arc::new(Budget::new(limits.queued, limits.control_reserve)),
            processors: Arc::new(Budget::new(limits.processors, limits.control_reserve)),
        }
    }

    fn get(&self, resource: AdmissionResource) -> Arc<Budget> {
        match resource {
            AdmissionResource::Connection => self.connections.clone(),
            AdmissionResource::Handshake => self.handshakes.clone(),
            AdmissionResource::Inflight => self.inflight.clone(),
            AdmissionResource::Queued => self.queued.clone(),
            AdmissionResource::Processor => self.processors.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ScopeKey {
    Ip(AdmissionResource, IpAddr),
    Tenant(AdmissionResource, u64),
    Session(AdmissionResource, u64),
}

/// RAII ownership of global and scoped admission capacity.
pub struct AdmissionPermit {
    _permits: Vec<BudgetPermit>,
    observer: Option<tokio::sync::mpsc::Sender<AdmissionEvent>>,
    resource: AdmissionResource,
    bytes: usize,
}

impl fmt::Debug for AdmissionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionPermit")
            .field("resource", &self.resource)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if let Some(observer) = &self.observer {
            let _ = observer.try_send(AdmissionEvent {
                resource: self.resource,
                outcome: AdmissionOutcome::Released,
                bytes: self.bytes,
            });
        }
    }
}

/// Owner of global/per-IP/per-tenant/per-session transport budgets and metrics.
pub struct AdmissionController {
    limits: AdmissionLimits,
    global: GlobalBudgets,
    scoped: Mutex<HashMap<ScopeKey, Arc<Budget>>>,
    observer: Option<tokio::sync::mpsc::Sender<AdmissionEvent>>,
}

impl AdmissionController {
    pub fn new(limits: AdmissionLimits) -> Self {
        Self::build(limits, None)
    }

    pub fn with_observer(limits: AdmissionLimits, observer: tokio::sync::mpsc::Sender<AdmissionEvent>) -> Self {
        Self::build(limits, Some(observer))
    }

    fn build(limits: AdmissionLimits, observer: Option<tokio::sync::mpsc::Sender<AdmissionEvent>>) -> Self {
        Self {
            limits,
            global: GlobalBudgets::new(limits),
            scoped: Mutex::new(HashMap::new()),
            observer,
        }
    }

    pub fn try_acquire(
        &self,
        resource: AdmissionResource,
        scope: AdmissionScope,
        bytes: usize,
        class: AdmissionClass,
    ) -> Result<AdmissionPermit, AdmissionError> {
        let policy = match resource {
            AdmissionResource::Connection | AdmissionResource::Handshake => FullPolicy::CloseConnection,
            AdmissionResource::Inflight | AdmissionResource::Queued | AdmissionResource::Processor => {
                FullPolicy::Reject
            }
        };
        let error = || AdmissionError { resource, policy };
        let mut permits = Vec::with_capacity(4);
        permits.push(self.global.get(resource).try_acquire(bytes, class).ok_or_else(error)?);
        permits.push(
            self.scoped_budget(ScopeKey::Ip(resource, scope.ip), self.limits.per_ip)?
                .try_acquire(bytes, class)
                .ok_or_else(error)?,
        );
        if let Some(tenant) = scope.tenant {
            permits.push(
                self.scoped_budget(ScopeKey::Tenant(resource, tenant), self.limits.per_tenant)?
                    .try_acquire(bytes, class)
                    .ok_or_else(error)?,
            );
        }
        if let Some(session) = scope.session {
            permits.push(
                self.scoped_budget(ScopeKey::Session(resource, session), self.limits.per_session)?
                    .try_acquire(bytes, class)
                    .ok_or_else(error)?,
            );
        }
        if let Some(observer) = &self.observer {
            let _ = observer.try_send(AdmissionEvent {
                resource,
                outcome: AdmissionOutcome::Acquired,
                bytes,
            });
        }
        Ok(AdmissionPermit {
            _permits: permits,
            observer: self.observer.clone(),
            resource,
            bytes,
        })
    }

    fn scoped_budget(&self, key: ScopeKey, limit: ResourceLimit) -> Result<Arc<Budget>, AdmissionError> {
        let resource = match key {
            ScopeKey::Ip(resource, _) | ScopeKey::Tenant(resource, _) | ScopeKey::Session(resource, _) => resource,
        };
        let mut scoped = self.scoped.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(budget) = scoped.get(&key) {
            return Ok(budget.clone());
        }
        if scoped.len() >= self.limits.max_scope_keys {
            scoped.retain(|_, budget| Arc::strong_count(budget) > 1 || !budget.is_idle());
        }
        if scoped.len() >= self.limits.max_scope_keys {
            return Err(AdmissionError {
                resource,
                policy: FullPolicy::Reject,
            });
        }
        let budget = Arc::new(Budget::new(limit, ResourceLimit { count: 0, bytes: 0 }));
        scoped.insert(key, budget.clone());
        Ok(budget)
    }

    pub fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            connections: self.global.connections.snapshot(),
            handshakes: self.global.handshakes.snapshot(),
            inflight: self.global.inflight.snapshot(),
            queued: self.global.queued.snapshot(),
            processors: self.global.processors.snapshot(),
        }
    }
}
