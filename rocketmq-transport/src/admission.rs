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
use std::sync::Mutex;

use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_runtime::BudgetCapacity;
use rocketmq_runtime::BudgetClass;
use rocketmq_runtime::BudgetConfigError;
use rocketmq_runtime::BudgetDimension;
use rocketmq_runtime::BudgetLimit;
pub use rocketmq_runtime::FullPolicy;
use rocketmq_runtime::ResourceBudget;
use rocketmq_runtime::ResourceBudgetTree;
use rocketmq_runtime::ResourcePermit;

const ESTABLISHED_CONNECTION_RETAINED_BYTES: usize = 16 * 1024;
const HANDSHAKE_RETAINED_BYTES: usize = 64 * 1024;

#[must_use]
pub(crate) const fn estimated_connection_retained_bytes() -> usize {
    ESTABLISHED_CONNECTION_RETAINED_BYTES
}

#[must_use]
pub(crate) const fn estimated_handshake_retained_bytes() -> usize {
    HANDSHAKE_RETAINED_BYTES
}

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

#[derive(Debug)]
pub enum AdmissionConfigError {
    Budget(BudgetConfigError),
    ZeroScopeCapacity {
        scope: &'static str,
        dimension: BudgetDimension,
    },
    ZeroMaxScopeKeys,
}

impl fmt::Display for AdmissionConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(error) => error.fmt(formatter),
            Self::ZeroScopeCapacity { scope, dimension } => {
                write!(formatter, "{scope} admission limit has zero {dimension:?} capacity")
            }
            Self::ZeroMaxScopeKeys => {
                formatter.write_str("transport admission max_scope_keys must be greater than zero")
            }
        }
    }
}

impl std::error::Error for AdmissionConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Budget(error) => Some(error),
            Self::ZeroScopeCapacity { .. } | Self::ZeroMaxScopeKeys => None,
        }
    }
}

impl From<BudgetConfigError> for AdmissionConfigError {
    fn from(error: BudgetConfigError) -> Self {
        Self::Budget(error)
    }
}

struct GlobalBudgets {
    connections: ResourceBudget,
    handshakes: ResourceBudget,
    inflight: ResourceBudget,
    queued: ResourceBudget,
    processors: ResourceBudget,
}

impl GlobalBudgets {
    fn new(limits: AdmissionLimits, process_budget: &ResourceBudget) -> Result<Self, AdmissionConfigError> {
        let resources = [
            limits.connections,
            limits.handshakes,
            limits.inflight,
            limits.queued,
            limits.processors,
        ];
        let root_count = resources
            .iter()
            .fold(0usize, |total, limit| total.saturating_add(limit.count));
        let requested_root_bytes = resources
            .iter()
            .fold(0usize, |total, limit| total.saturating_add(limit.bytes));
        let process_capacity = process_budget.limit().capacity;
        let root_count = root_count.min(process_capacity.count).max(1);
        let root_bytes = requested_root_bytes.min(process_capacity.bytes).max(1);
        let reserve_count = resources
            .iter()
            .fold(0usize, |total, limit| {
                total.saturating_add(effective_reserve(limits.control_reserve.count, limit.count))
            })
            .min(root_count);
        let requested_reserve_bytes = resources.iter().fold(0usize, |total, limit| {
            total.saturating_add(effective_reserve(limits.control_reserve.bytes, limit.bytes))
        });
        let reserve_bytes = effective_reserve(requested_reserve_bytes, root_bytes);
        let root = process_budget.child(
            "transport",
            BudgetLimit::new(root_count, root_bytes, FullPolicy::Reject)
                .with_control_reserve(BudgetCapacity::new(reserve_count, reserve_bytes)),
        )?;
        Ok(Self {
            connections: global_budget(
                &root,
                "connections",
                limits.connections,
                limits.control_reserve,
                FullPolicy::CloseSlowConsumer,
            )?,
            handshakes: global_budget(
                &root,
                "handshakes",
                limits.handshakes,
                limits.control_reserve,
                FullPolicy::CloseSlowConsumer,
            )?,
            inflight: global_budget(
                &root,
                "inflight",
                limits.inflight,
                limits.control_reserve,
                FullPolicy::Reject,
            )?,
            queued: global_budget(
                &root,
                "queued",
                limits.queued,
                limits.control_reserve,
                FullPolicy::Reject,
            )?,
            processors: global_budget(
                &root,
                "processors",
                limits.processors,
                limits.control_reserve,
                FullPolicy::Reject,
            )?,
        })
    }

    fn get(&self, resource: AdmissionResource) -> ResourceBudget {
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
    Tenant(AdmissionResource, IpAddr, u64),
    Session(AdmissionResource, IpAddr, Option<u64>, u64),
}

/// RAII ownership of global and scoped admission capacity.
pub struct AdmissionPermit {
    _permit: ResourcePermit,
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
    scoped: Mutex<HashMap<ScopeKey, ResourceBudget>>,
    observer: Option<tokio::sync::mpsc::Sender<AdmissionEvent>>,
}

impl AdmissionController {
    /// Creates a transport admission controller.
    ///
    /// # Panics
    ///
    /// Panics when a limit is zero or a control reserve exceeds its owning
    /// resource. Production configuration paths should prefer [`Self::try_new`].
    pub fn new(limits: AdmissionLimits) -> Self {
        Self::try_new(limits).unwrap_or_else(|error| panic!("invalid transport admission limits: {error}"))
    }

    pub fn try_new(limits: AdmissionLimits) -> Result<Self, AdmissionConfigError> {
        let process_budget = standalone_process_budget(limits)?;
        Self::build(limits, &process_budget, None)
    }

    /// Creates a controller whose transport budgets derive from the injected
    /// process resource root.
    ///
    /// # Errors
    ///
    /// Returns an error when any transport or derived child limit is invalid.
    pub fn try_new_with_budget(
        limits: AdmissionLimits,
        process_budget: &ResourceBudget,
    ) -> Result<Self, AdmissionConfigError> {
        Self::build(limits, process_budget, None)
    }

    /// Creates an observed transport admission controller.
    ///
    /// # Panics
    ///
    /// Panics under the same invalid-limit conditions as [`Self::new`].
    pub fn with_observer(limits: AdmissionLimits, observer: tokio::sync::mpsc::Sender<AdmissionEvent>) -> Self {
        Self::try_with_observer(limits, observer)
            .unwrap_or_else(|error| panic!("invalid transport admission limits: {error}"))
    }

    pub fn try_with_observer(
        limits: AdmissionLimits,
        observer: tokio::sync::mpsc::Sender<AdmissionEvent>,
    ) -> Result<Self, AdmissionConfigError> {
        let process_budget = standalone_process_budget(limits)?;
        Self::build(limits, &process_budget, Some(observer))
    }

    /// Creates an observed controller under an injected process resource root.
    ///
    /// # Errors
    ///
    /// Returns an error when any transport or derived child limit is invalid.
    pub fn try_with_budget_and_observer(
        limits: AdmissionLimits,
        process_budget: &ResourceBudget,
        observer: tokio::sync::mpsc::Sender<AdmissionEvent>,
    ) -> Result<Self, AdmissionConfigError> {
        Self::build(limits, process_budget, Some(observer))
    }

    fn build(
        limits: AdmissionLimits,
        process_budget: &ResourceBudget,
        observer: Option<tokio::sync::mpsc::Sender<AdmissionEvent>>,
    ) -> Result<Self, AdmissionConfigError> {
        validate_scope_limits(limits)?;
        Ok(Self {
            limits,
            global: GlobalBudgets::new(limits, process_budget)?,
            scoped: Mutex::new(HashMap::new()),
            observer,
        })
    }

    pub fn try_acquire(
        &self,
        resource: AdmissionResource,
        scope: AdmissionScope,
        bytes: usize,
        class: AdmissionClass,
    ) -> Result<AdmissionPermit, AdmissionError> {
        let policy = policy_for(resource);
        let budget = self.scoped_budget(resource, scope)?;
        let class = match class {
            AdmissionClass::Data => BudgetClass::Data,
            AdmissionClass::Control => BudgetClass::Control,
        };
        let permit = budget.try_acquire(bytes, class).map_err(|_| {
            if let Some(observer) = &self.observer {
                let _ = observer.try_send(AdmissionEvent {
                    resource,
                    outcome: AdmissionOutcome::Rejected,
                    bytes,
                });
            }
            AdmissionError { resource, policy }
        })?;
        if let Some(observer) = &self.observer {
            let _ = observer.try_send(AdmissionEvent {
                resource,
                outcome: AdmissionOutcome::Acquired,
                bytes,
            });
        }
        Ok(AdmissionPermit {
            _permit: permit,
            observer: self.observer.clone(),
            resource,
            bytes,
        })
    }

    pub(crate) fn rebind_permit(
        &self,
        resource: AdmissionResource,
        scope: AdmissionScope,
        mut permit: ResourcePermit,
    ) -> Result<AdmissionPermit, AdmissionError> {
        let budget = self.scoped_budget(resource, scope)?;
        permit.try_rebind(&budget).map_err(|_| {
            if let Some(observer) = &self.observer {
                let _ = observer.try_send(AdmissionEvent {
                    resource,
                    outcome: AdmissionOutcome::Rejected,
                    bytes: permit.bytes(),
                });
            }
            AdmissionError {
                resource,
                policy: policy_for(resource),
            }
        })?;
        let bytes = permit.bytes();
        if let Some(observer) = &self.observer {
            let _ = observer.try_send(AdmissionEvent {
                resource,
                outcome: AdmissionOutcome::Acquired,
                bytes,
            });
        }
        Ok(AdmissionPermit {
            _permit: permit,
            observer: self.observer.clone(),
            resource,
            bytes,
        })
    }

    fn scoped_budget(
        &self,
        resource: AdmissionResource,
        scope: AdmissionScope,
    ) -> Result<ResourceBudget, AdmissionError> {
        let mut scoped = self.scoped.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let global = self.global.get(resource);
        let ip_key = ScopeKey::Ip(resource, scope.ip);
        let tenant_key = scope.tenant.map(|tenant| ScopeKey::Tenant(resource, scope.ip, tenant));
        let session_key = scope
            .session
            .map(|session| ScopeKey::Session(resource, scope.ip, scope.tenant, session));
        let scope_keys = [Some(ip_key), tenant_key, session_key];
        let mut missing_scope_count = scope_keys
            .into_iter()
            .flatten()
            .filter(|key| !scoped.contains_key(key))
            .count();
        if scoped.len().saturating_add(missing_scope_count) > self.limits.max_scope_keys {
            scoped.retain(|_, budget| budget.snapshot().current_count > 0);
            missing_scope_count = scope_keys
                .into_iter()
                .flatten()
                .filter(|key| !scoped.contains_key(key))
                .count();
        }
        if scoped.len().saturating_add(missing_scope_count) > self.limits.max_scope_keys {
            return Err(AdmissionError {
                resource,
                policy: policy_for(resource),
            });
        }
        let ip_budget = scoped_child(
            &mut scoped,
            self.limits.max_scope_keys,
            ip_key,
            &global,
            format!("ip-{}", scope.ip),
            self.limits.per_ip,
            self.limits.control_reserve,
            resource,
        )?;
        let tenant_budget = match (scope.tenant, tenant_key) {
            (Some(tenant), Some(tenant_key)) => scoped_child(
                &mut scoped,
                self.limits.max_scope_keys,
                tenant_key,
                &ip_budget,
                format!("tenant-{tenant}"),
                self.limits.per_tenant,
                self.limits.control_reserve,
                resource,
            )?,
            (None, None) => ip_budget,
            _ => unreachable!("tenant scope key is derived from the tenant identifier"),
        };
        match (scope.session, session_key) {
            (Some(session), Some(session_key)) => scoped_child(
                &mut scoped,
                self.limits.max_scope_keys,
                session_key,
                &tenant_budget,
                format!("session-{session}"),
                self.limits.per_session,
                self.limits.control_reserve,
                resource,
            ),
            (None, None) => Ok(tenant_budget),
            _ => unreachable!("session scope key is derived from the session identifier"),
        }
    }

    pub fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            connections: resource_snapshot(&self.global.connections),
            handshakes: resource_snapshot(&self.global.handshakes),
            inflight: resource_snapshot(&self.global.inflight),
            queued: resource_snapshot(&self.global.queued),
            processors: resource_snapshot(&self.global.processors),
        }
    }
}

fn standalone_process_budget(limits: AdmissionLimits) -> Result<ResourceBudget, AdmissionConfigError> {
    let resources = [
        limits.connections,
        limits.handshakes,
        limits.inflight,
        limits.queued,
        limits.processors,
    ];
    let count = resources
        .iter()
        .fold(0usize, |total, limit| total.saturating_add(limit.count))
        .max(1);
    let bytes = resources
        .iter()
        .fold(0usize, |total, limit| total.saturating_add(limit.bytes))
        .max(1);
    Ok(ResourceBudgetTree::new(
        "standalone-transport-process",
        BudgetLimit::new(count, bytes, FullPolicy::Reject),
    )?
    .root())
}

fn global_budget(
    root: &ResourceBudget,
    name: &str,
    limit: ResourceLimit,
    reserve: ResourceLimit,
    policy: FullPolicy,
) -> Result<ResourceBudget, BudgetConfigError> {
    let root_capacity = root.limit().capacity;
    let limit = ResourceLimit {
        count: limit.count.min(root_capacity.count),
        bytes: limit.bytes.min(root_capacity.bytes),
    };
    let reserve = ResourceLimit {
        count: effective_reserve(reserve.count, limit.count),
        bytes: effective_reserve(reserve.bytes, limit.bytes),
    };
    root.child(
        name,
        BudgetLimit::new(limit.count, limit.bytes, policy)
            .with_control_reserve(BudgetCapacity::new(reserve.count, reserve.bytes)),
    )
}

fn effective_reserve(requested: usize, capacity: usize) -> usize {
    if requested < capacity {
        requested
    } else {
        0
    }
}

fn scoped_child(
    scoped: &mut HashMap<ScopeKey, ResourceBudget>,
    max_scope_keys: usize,
    key: ScopeKey,
    parent: &ResourceBudget,
    name: String,
    requested: ResourceLimit,
    requested_reserve: ResourceLimit,
    resource: AdmissionResource,
) -> Result<ResourceBudget, AdmissionError> {
    if let Some(budget) = scoped.get(&key) {
        return Ok(budget.clone());
    }
    if scoped.len() >= max_scope_keys {
        return Err(AdmissionError {
            resource,
            policy: policy_for(resource),
        });
    }
    let parent_limit = parent.limit().capacity;
    let limit = ResourceLimit {
        count: requested.count.min(parent_limit.count),
        bytes: requested.bytes.min(parent_limit.bytes),
    };
    let reserve = ResourceLimit {
        count: effective_reserve(requested_reserve.count, limit.count),
        bytes: effective_reserve(requested_reserve.bytes, limit.bytes),
    };
    let budget = parent
        .child(
            name,
            BudgetLimit::new(limit.count, limit.bytes, parent.limit().full_policy)
                .with_control_reserve(BudgetCapacity::new(reserve.count, reserve.bytes)),
        )
        .map_err(|_| AdmissionError {
            resource,
            policy: policy_for(resource),
        })?;
    scoped.insert(key, budget.clone());
    Ok(budget)
}

fn validate_scope_limits(limits: AdmissionLimits) -> Result<(), AdmissionConfigError> {
    if limits.max_scope_keys == 0 {
        return Err(AdmissionConfigError::ZeroMaxScopeKeys);
    }
    for (scope, limit) in [
        ("per_ip", limits.per_ip),
        ("per_tenant", limits.per_tenant),
        ("per_session", limits.per_session),
    ] {
        if limit.count == 0 {
            return Err(AdmissionConfigError::ZeroScopeCapacity {
                scope,
                dimension: BudgetDimension::Count,
            });
        }
        if limit.bytes == 0 {
            return Err(AdmissionConfigError::ZeroScopeCapacity {
                scope,
                dimension: BudgetDimension::Bytes,
            });
        }
    }
    Ok(())
}

fn policy_for(resource: AdmissionResource) -> FullPolicy {
    match resource {
        AdmissionResource::Connection | AdmissionResource::Handshake => FullPolicy::CloseSlowConsumer,
        AdmissionResource::Inflight | AdmissionResource::Queued | AdmissionResource::Processor => FullPolicy::Reject,
    }
}

fn resource_snapshot(budget: &ResourceBudget) -> ResourceSnapshot {
    let snapshot = budget.snapshot();
    ResourceSnapshot {
        current_count: snapshot.current_count,
        current_bytes: snapshot.current_bytes,
        rejected_count: usize::try_from(snapshot.rejected_count).unwrap_or(usize::MAX),
    }
}
