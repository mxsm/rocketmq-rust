// Copyright 2026 The RocketMQ Rust Authors
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

//! Supervised changes that one caller-chosen operation starts and the same operation ends.
//!
//! A Proxy drain and a temporary Broker log filter are not metadata. Each puts one target into a
//! state that lasts until the operation that started it ends it or, for the log filter, until its
//! time to live runs out. They follow the preflight and execute pairs of
//! [`SupervisedMutationAdmin`](super::supervised_mutation::SupervisedMutationAdmin): a preflight
//! reads the target and seals a plan to its session, an execution changes the target and reads it
//! again.
//!
//! A plan or report never carries a network address. It also never carries the identifier of an
//! operation, because the one that holds a target may be text that another caller chose.

use std::sync::Arc;

use crate::core::broker::BrokerLogLevel;
use crate::core::proxy::ProxyDrainPending;
use crate::core::proxy::ProxyDrainPhase;
use crate::core::proxy::ProxyDrainState;
use crate::core::supervised_mutation::MutationFailureCode;
use crate::core::supervised_mutation::MutationPlanSeal;
use crate::core::supervised_mutation::MutationVerificationState;
use crate::core::AdminError;
use crate::core::AdminFuture;
use crate::core::AdminResult;

/// Longest operation identifier that both a Proxy and a Broker accept.
pub const MAX_OPERATION_ID_BYTES: usize = 128;
/// Shortest time to live of a supervised log-filter override, in seconds.
pub const MIN_LOG_FILTER_TTL_SECONDS: u32 = 60;
/// Longest time to live of a supervised log-filter override, in seconds.
pub const MAX_LOG_FILTER_TTL_SECONDS: u32 = 900;

const LOGGER_ROOT: &str = "rocketmq_broker::";
const MAX_LOGGER_BYTES: usize = 128;
/// Module name parts whose logs can carry message content, credentials or storage internals.
const PROTECTED_LOGGER_PARTS: &[&str] = &[
    "message", "body", "security", "auth", "acl", "secret", "payload", "store",
];

/// How the operation that holds a target relates to the operation a request names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveOperation {
    /// No operation holds the target.
    None,
    /// The operation the request names holds the target.
    Requested,
    /// Another operation holds the target.
    Other,
}

impl ActiveOperation {
    /// Classifies the operation a target reports against the one a request names.
    #[must_use]
    pub fn classify(active: Option<&str>, requested: &str) -> Self {
        match active {
            None => Self::None,
            Some(active) if active == requested => Self::Requested,
            Some(_) => Self::Other,
        }
    }
}

/// What an action has to do to a target in the state a preflight read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationStep {
    /// The target has to change.
    Change,
    /// The target already is where the action leaves it.
    Unchanged,
    /// Another operation holds the target, or the same operation holds it for something else.
    Conflict,
}

/// Why a supervised operation cannot be executed, or did not reach its end state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationFailure {
    pub code: MutationFailureCode,
    pub retryable: bool,
}

#[cfg(any(feature = "mutation-client-adapter", test))]
impl OperationFailure {
    const CONFLICT: Self = Self {
        code: MutationFailureCode::Conflict,
        retryable: false,
    };
}

/// An action on one target that is undone by a matching action of the same operation.
pub trait OperationAction {
    /// The target state this action reads and changes.
    type Observation: Copy;

    /// Which operation holds a target in the observed state.
    fn holder(observed: Self::Observation) -> ActiveOperation;

    /// What this action has to do to a target in the observed state.
    fn step(&self, observed: Self::Observation) -> OperationStep;

    /// Whether a target in the observed state is where this action leaves it.
    fn satisfied_by(&self, observed: Self::Observation) -> bool;
}

/// What a supervised drain call asks one Proxy to do.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyDrainAction {
    /// Stop admitting new work so that the Proxy can be restarted.
    Begin,
    /// Admit work again.
    Cancel,
}

/// A Proxy drain state without the identifier of the operation that holds it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProxyDrainObservation {
    pub phase: ProxyDrainPhase,
    pub operation: ActiveOperation,
    pub admission_open: bool,
    pub routing_open: bool,
    pub readiness_published: bool,
    pub zero_pending: bool,
    pub pending: ProxyDrainPending,
}

impl ProxyDrainObservation {
    /// Projects a decoded drain state for a request that names `operation_id`.
    #[must_use]
    pub fn of(state: &ProxyDrainState, operation_id: &str) -> Self {
        Self {
            phase: state.phase,
            operation: ActiveOperation::classify(state.operation_id.as_deref(), operation_id),
            admission_open: state.admission_open,
            routing_open: state.routing_open,
            readiness_published: state.readiness_published,
            zero_pending: state.zero_pending,
            pending: state.pending,
        }
    }
}

/// The decisions mirror the Proxy: it accepts a begin for the drain this operation already holds
/// and a cancel while nothing is draining without changing anything, and it refuses both for a
/// drain that anything else holds.
impl OperationAction for ProxyDrainAction {
    type Observation = ProxyDrainObservation;

    fn holder(observed: ProxyDrainObservation) -> ActiveOperation {
        observed.operation
    }

    fn step(&self, observed: ProxyDrainObservation) -> OperationStep {
        match (self, observed.phase, observed.operation) {
            (Self::Begin, ProxyDrainPhase::Accepting, _) => OperationStep::Change,
            (Self::Begin, _, ActiveOperation::Requested) => OperationStep::Unchanged,
            (Self::Cancel, ProxyDrainPhase::Accepting, ActiveOperation::None) => OperationStep::Unchanged,
            (Self::Cancel, _, ActiveOperation::Requested) => OperationStep::Change,
            _ => OperationStep::Conflict,
        }
    }

    fn satisfied_by(&self, observed: ProxyDrainObservation) -> bool {
        match self {
            Self::Begin => {
                observed.phase != ProxyDrainPhase::Accepting && observed.operation == ActiveOperation::Requested
            }
            Self::Cancel => observed.phase == ProxyDrainPhase::Accepting && observed.operation == ActiveOperation::None,
        }
    }
}

/// One bounded level override for one Broker logger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerLogFilterOverride {
    logger: String,
    level: BrokerLogLevel,
    ttl_seconds: u32,
}

impl BrokerLogFilterOverride {
    /// Creates a level override that the Broker ends by itself after `ttl_seconds`.
    ///
    /// # Errors
    ///
    /// Rejects a logger that [`is_supervised_broker_logger`] does not accept and a time to live
    /// outside [`MIN_LOG_FILTER_TTL_SECONDS`] through [`MAX_LOG_FILTER_TTL_SECONDS`].
    pub fn try_new(logger: impl Into<String>, level: BrokerLogLevel, ttl_seconds: u32) -> AdminResult<Self> {
        let logger = logger.into();
        if !is_supervised_broker_logger(&logger) {
            return Err(AdminError::invalid_argument(
                "logger",
                "must be a lower-case rocketmq_broker:: module path that names no protected module",
            ));
        }
        if !(MIN_LOG_FILTER_TTL_SECONDS..=MAX_LOG_FILTER_TTL_SECONDS).contains(&ttl_seconds) {
            return Err(AdminError::invalid_argument(
                "ttl_seconds",
                "must be between 60 and 900",
            ));
        }
        Ok(Self {
            logger,
            level,
            ttl_seconds,
        })
    }

    #[must_use]
    pub fn logger(&self) -> &str {
        &self.logger
    }

    #[must_use]
    pub const fn level(&self) -> BrokerLogLevel {
        self.level
    }

    #[must_use]
    pub const fn ttl_seconds(&self) -> u32 {
        self.ttl_seconds
    }
}

/// Returns whether a logger may get a supervised temporary level override.
///
/// Accepts a `rocketmq_broker::` module path of at most 128 bytes, written in lower-case letters,
/// digits and underscores, that names no module for message content, credentials or storage. The
/// Broker client accepts every such logger; it refuses others before it sends anything, which a
/// supervised caller could not tell from an unavailable Broker.
#[must_use]
pub fn is_supervised_broker_logger(logger: &str) -> bool {
    logger.len() <= MAX_LOGGER_BYTES
        && logger.strip_prefix(LOGGER_ROOT).is_some_and(|path| {
            path.split("::").all(|segment| {
                let mut bytes = segment.bytes();
                bytes
                    .next()
                    .is_some_and(|first| first.is_ascii_lowercase() || first == b'_')
                    && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            })
        })
        && !PROTECTED_LOGGER_PARTS.iter().any(|part| logger.contains(part))
}

/// What a supervised log-filter call asks one Broker to do.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerLogFilterAction {
    /// Set one logger to a level for a bounded time.
    Set(BrokerLogFilterOverride),
    /// End the override this operation started.
    Restore,
}

/// A Broker log-filter control state without the identifier of the operation that holds it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerLogFilterObservation {
    /// Effective level of the logger a set names. Absent for a restore, which names no logger, and
    /// when the effective level is neither `INFO` nor `DEBUG`.
    pub level: Option<BrokerLogLevel>,
    pub operation: ActiveOperation,
    /// When the Broker ends the active override by itself.
    pub expires_at_millis: Option<u64>,
}

/// A Broker keeps one override at a time and replaces it without asking. A supervised set
/// therefore never replaces one: an override that is not exactly the requested one is a conflict,
/// also when the same operation holds it, and has to be restored first. A restore ends only the
/// override of its own operation.
impl OperationAction for BrokerLogFilterAction {
    type Observation = BrokerLogFilterObservation;

    fn holder(observed: BrokerLogFilterObservation) -> ActiveOperation {
        observed.operation
    }

    fn step(&self, observed: BrokerLogFilterObservation) -> OperationStep {
        match (self, observed.operation) {
            (Self::Set(_), ActiveOperation::None) | (Self::Restore, ActiveOperation::Requested) => {
                OperationStep::Change
            }
            (Self::Set(_), ActiveOperation::Requested) if self.satisfied_by(observed) => OperationStep::Unchanged,
            (Self::Restore, ActiveOperation::None) => OperationStep::Unchanged,
            _ => OperationStep::Conflict,
        }
    }

    fn satisfied_by(&self, observed: BrokerLogFilterObservation) -> bool {
        match self {
            Self::Set(requested) => {
                observed.operation == ActiveOperation::Requested && observed.level == Some(requested.level)
            }
            Self::Restore => observed.operation == ActiveOperation::None,
        }
    }
}

/// One supervised Proxy drain request.
///
/// `proxy_addr` comes from trusted configuration, never from a caller. No plan or report returns
/// it.
#[derive(Clone, Eq, PartialEq)]
pub struct ProxyDrainPreflightRequest {
    pub cluster: String,
    pub proxy_addr: String,
    pub operation_id: String,
    pub action: ProxyDrainAction,
}

impl std::fmt::Debug for ProxyDrainPreflightRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProxyDrainPreflightRequest")
            .field("cluster", &self.cluster)
            .field("action", &self.action)
            .finish_non_exhaustive()
    }
}

/// One supervised log-filter request for the master of one logical Broker.
#[derive(Clone, Eq, PartialEq)]
pub struct BrokerLogFilterPreflightRequest {
    pub cluster: String,
    pub broker_name: String,
    pub operation_id: String,
    pub action: BrokerLogFilterAction,
}

impl std::fmt::Debug for BrokerLogFilterPreflightRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrokerLogFilterPreflightRequest")
            .field("cluster", &self.cluster)
            .field("broker_name", &self.broker_name)
            .field("action", &self.action)
            .finish_non_exhaustive()
    }
}

/// Validates an operation identifier for either target.
///
/// # Errors
///
/// Rejects an identifier that is empty, longer than [`MAX_OPERATION_ID_BYTES`], or holds anything
/// but ASCII letters, digits and `-_.:/`, which is what the Broker client accepts.
pub fn validate_operation_id(operation_id: &str) -> AdminResult<&str> {
    let valid = !operation_id.is_empty()
        && operation_id.len() <= MAX_OPERATION_ID_BYTES
        && operation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/'));
    if valid {
        Ok(operation_id)
    } else {
        Err(AdminError::invalid_argument(
            "operation_id",
            "must be 1 to 128 ASCII letters, digits or -_.:/",
        ))
    }
}

/// One action planned against the state a preflight read, sealed to the session that read it.
#[derive(Clone)]
pub struct OperationPlan<A: OperationAction> {
    pub(crate) seal: Arc<MutationPlanSeal>,
    pub(crate) cluster: String,
    #[allow(
        dead_code,
        reason = "resolved addresses are consumed only by the optional mutation adapter"
    )]
    pub(crate) target_addr: String,
    #[allow(
        dead_code,
        reason = "the operation identifier is consumed only by the optional mutation adapter"
    )]
    pub(crate) operation_id: String,
    pub(crate) action: A,
    pub(crate) before: Option<A::Observation>,
    pub(crate) failure: Option<OperationFailure>,
}

/// A sealed drain plan for one Proxy.
pub type ProxyDrainPlan = OperationPlan<ProxyDrainAction>;
/// A sealed log-filter plan for one Broker master.
pub type BrokerLogFilterPlan = OperationPlan<BrokerLogFilterAction>;

impl<A: OperationAction> OperationPlan<A> {
    /// Plans `action` from the state a preflight read, or from why it could not read one.
    #[cfg(any(feature = "mutation-client-adapter", test))]
    pub(crate) fn new(
        seal: Arc<MutationPlanSeal>,
        cluster: String,
        target_addr: String,
        operation_id: String,
        action: A,
        read: Result<A::Observation, OperationFailure>,
    ) -> Self {
        let (before, failure) = match read {
            Ok(observed) => (
                Some(observed),
                (action.step(observed) == OperationStep::Conflict).then_some(OperationFailure::CONFLICT),
            ),
            Err(failure) => (None, Some(failure)),
        };
        Self {
            seal,
            cluster,
            target_addr,
            operation_id,
            action,
            before,
            failure,
        }
    }

    #[must_use]
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    #[must_use]
    pub fn action(&self) -> &A {
        &self.action
    }

    /// The state the preflight read; absent when it could not read one.
    #[must_use]
    pub fn before(&self) -> Option<A::Observation> {
        self.before
    }

    /// Why this plan cannot be executed, when it cannot.
    #[must_use]
    pub fn failure(&self) -> Option<OperationFailure> {
        self.failure
    }

    /// Whether executing this plan changes the target.
    #[must_use]
    pub fn changes(&self) -> bool {
        self.failure.is_none()
            && self
                .before
                .is_some_and(|before| self.action.step(before) == OperationStep::Change)
    }

    /// The report of this plan when it sends nothing: it cannot be executed, or the target already
    /// is where the action leaves it. Returns `None` when the plan has a change to send.
    #[cfg(any(feature = "mutation-client-adapter", test))]
    pub(crate) fn settled_report(&self) -> Option<OperationReport<A::Observation>> {
        if let Some(failure) = self.failure {
            return Some(OperationReport {
                before: self.before,
                after: None,
                applied: false,
                changed: false,
                verification: MutationVerificationState::NotPerformed,
                failure: Some(failure.code),
                retryable: failure.retryable,
            });
        }
        (!self.changes()).then_some(OperationReport {
            before: self.before,
            after: self.before,
            applied: true,
            changed: false,
            verification: MutationVerificationState::NotPerformed,
            failure: None,
            retryable: false,
        })
    }

    /// The report of a change that was sent, from what the write and the read after it returned.
    ///
    /// A write the target refused is a conflict when the read shows that another operation holds
    /// the target, and leaves `applied` false also when the read shows the requested state: the
    /// write was not acknowledged, so only `changed` and `after` say that the target moved.
    #[cfg(any(feature = "mutation-client-adapter", test))]
    pub(crate) fn attempted_report(
        &self,
        write: Result<(), OperationFailure>,
        observed: Result<A::Observation, OperationFailure>,
    ) -> OperationReport<A::Observation> {
        let after = observed.ok();
        match write {
            Ok(()) => {
                let verified = after.is_some_and(|after| self.action.satisfied_by(after));
                OperationReport {
                    before: self.before,
                    after,
                    applied: true,
                    changed: true,
                    verification: if verified {
                        MutationVerificationState::Verified
                    } else {
                        MutationVerificationState::Failed
                    },
                    failure: (!verified).then_some(MutationFailureCode::VerificationFailed),
                    retryable: observed.err().is_some_and(|failure| failure.retryable),
                }
            }
            Err(refused) => {
                let conflict = after.is_some_and(|after| A::holder(after) == ActiveOperation::Other);
                OperationReport {
                    before: self.before,
                    after,
                    applied: false,
                    changed: after.is_some_and(|after| self.action.satisfied_by(after)),
                    verification: MutationVerificationState::NotPerformed,
                    failure: Some(if conflict {
                        MutationFailureCode::Conflict
                    } else {
                        refused.code
                    }),
                    retryable: !conflict && refused.retryable,
                }
            }
        }
    }
}

impl<A> std::fmt::Debug for OperationPlan<A>
where
    A: OperationAction + std::fmt::Debug,
    A::Observation: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _session_bound = &self.seal;
        formatter
            .debug_struct("OperationPlan")
            .field("cluster", &self.cluster)
            .field("action", &self.action)
            .field("before", &self.before)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

/// What one supervised operation did to its target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationReport<S> {
    /// State the preflight read; absent when it could not read one.
    pub before: Option<S>,
    /// State read after the attempt, or `before` again when nothing had to change.
    pub after: Option<S>,
    /// Whether the target acknowledged the change or already was in the requested state.
    pub applied: bool,
    /// Whether the target moved into the requested state during this call.
    pub changed: bool,
    pub verification: MutationVerificationState,
    pub failure: Option<MutationFailureCode>,
    pub retryable: bool,
}

/// What a drain call did to its Proxy.
pub type ProxyDrainReport = OperationReport<ProxyDrainObservation>;
/// What a log-filter call did to its Broker.
pub type BrokerLogFilterReport = OperationReport<BrokerLogFilterObservation>;

/// Operation-bound changes of one Proxy or one Broker master.
///
/// Implementations validate the complete topology of the selected cluster before they read a
/// target, execute only a plan of their own session, and read the target again after every write.
pub trait SupervisedOperationAdmin: Send {
    /// Reads the drain state of one Proxy and plans the requested transition.
    fn preflight_proxy_drain<'a>(
        &'a mut self,
        request: &'a ProxyDrainPreflightRequest,
    ) -> AdminFuture<'a, ProxyDrainPlan>;

    /// Executes one sealed drain plan and reads the Proxy again.
    fn execute_proxy_drain<'a>(&'a mut self, plan: &'a ProxyDrainPlan) -> AdminFuture<'a, ProxyDrainReport>;

    /// Reads the log-filter state of one Broker master and plans the requested transition.
    fn preflight_broker_log_filter<'a>(
        &'a mut self,
        request: &'a BrokerLogFilterPreflightRequest,
    ) -> AdminFuture<'a, BrokerLogFilterPlan>;

    /// Executes one sealed log-filter plan and reads the Broker again.
    fn execute_broker_log_filter<'a>(
        &'a mut self,
        plan: &'a BrokerLogFilterPlan,
    ) -> AdminFuture<'a, BrokerLogFilterReport>;
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNAVAILABLE: OperationFailure = OperationFailure {
        code: MutationFailureCode::Unavailable,
        retryable: true,
    };

    fn drain(phase: ProxyDrainPhase, operation: ActiveOperation) -> ProxyDrainObservation {
        let accepting = phase == ProxyDrainPhase::Accepting;
        ProxyDrainObservation {
            phase,
            operation,
            admission_open: accepting,
            routing_open: accepting,
            readiness_published: accepting,
            zero_pending: true,
            pending: ProxyDrainPending::default(),
        }
    }

    fn filter(level: BrokerLogLevel, operation: ActiveOperation) -> BrokerLogFilterObservation {
        BrokerLogFilterObservation {
            level: Some(level),
            operation,
            expires_at_millis: (operation != ActiveOperation::None).then_some(1_000),
        }
    }

    fn plan<A: OperationAction>(action: A, read: Result<A::Observation, OperationFailure>) -> OperationPlan<A> {
        OperationPlan::new(
            Arc::new(MutationPlanSeal),
            "cluster-a".to_owned(),
            "192.0.2.7:8080".to_owned(),
            "operation-1".to_owned(),
            action,
            read,
        )
    }

    fn debug_filter() -> BrokerLogFilterAction {
        BrokerLogFilterAction::Set(
            BrokerLogFilterOverride::try_new("rocketmq_broker::processor", BrokerLogLevel::Debug, 120).unwrap(),
        )
    }

    #[test]
    fn drain_steps_mirror_what_the_proxy_accepts_and_refuses() {
        use ActiveOperation::{None, Other, Requested};
        use OperationStep::{Change, Conflict, Unchanged};
        use ProxyDrainPhase::{Accepting, Drained, Draining};

        for (action, phase, operation, expected) in [
            (ProxyDrainAction::Begin, Accepting, None, Change),
            (ProxyDrainAction::Begin, Draining, Requested, Unchanged),
            (ProxyDrainAction::Begin, Drained, Requested, Unchanged),
            (ProxyDrainAction::Begin, Draining, Other, Conflict),
            (ProxyDrainAction::Begin, Drained, None, Conflict),
            (ProxyDrainAction::Cancel, Accepting, None, Unchanged),
            (ProxyDrainAction::Cancel, Draining, Requested, Change),
            (ProxyDrainAction::Cancel, Drained, Requested, Change),
            (ProxyDrainAction::Cancel, Draining, Other, Conflict),
            (ProxyDrainAction::Cancel, Draining, None, Conflict),
        ] {
            assert_eq!(
                action.step(drain(phase, operation)),
                expected,
                "{action:?} {phase:?} {operation:?}"
            );
        }
        assert!(ProxyDrainAction::Begin.satisfied_by(drain(Drained, Requested)));
        assert!(!ProxyDrainAction::Begin.satisfied_by(drain(Draining, Other)));
        assert!(ProxyDrainAction::Cancel.satisfied_by(drain(Accepting, None)));
        assert!(!ProxyDrainAction::Cancel.satisfied_by(drain(Draining, Requested)));
    }

    #[test]
    fn a_set_never_replaces_an_override_and_a_restore_ends_only_its_own() {
        use ActiveOperation::{None, Other, Requested};
        use BrokerLogLevel::{Debug, Info};
        use OperationStep::{Change, Conflict, Unchanged};

        let set = debug_filter();
        assert_eq!(set.step(filter(Info, None)), Change);
        assert_eq!(set.step(filter(Debug, None)), Change);
        assert_eq!(set.step(filter(Debug, Requested)), Unchanged);
        assert_eq!(set.step(filter(Info, Requested)), Conflict);
        assert_eq!(set.step(filter(Debug, Other)), Conflict);

        let restore = BrokerLogFilterAction::Restore;
        assert_eq!(restore.step(filter(Debug, Requested)), Change);
        assert_eq!(restore.step(filter(Info, None)), Unchanged);
        assert_eq!(restore.step(filter(Debug, Other)), Conflict);
        assert!(restore.satisfied_by(filter(Info, None)));
        assert!(!restore.satisfied_by(filter(Debug, Requested)));
    }

    #[test]
    fn a_plan_refuses_a_conflict_and_an_unread_target_without_a_change() {
        let conflict = plan(
            ProxyDrainAction::Begin,
            Ok(drain(ProxyDrainPhase::Draining, ActiveOperation::Other)),
        );
        assert!(!conflict.changes());
        assert_eq!(conflict.failure(), Some(OperationFailure::CONFLICT));
        let report = conflict.settled_report().expect("a conflict sends nothing");
        assert_eq!(report.failure, Some(MutationFailureCode::Conflict));
        assert!(!report.applied && !report.changed && !report.retryable);
        assert_eq!(report.before, conflict.before());
        assert_eq!(report.after, None);

        let unread = plan(ProxyDrainAction::Begin, Err(UNAVAILABLE));
        assert_eq!(unread.before(), None);
        let report = unread.settled_report().expect("an unread target sends nothing");
        assert_eq!(report.failure, Some(MutationFailureCode::Unavailable));
        assert!(report.retryable && !report.applied);

        let unchanged = plan(
            ProxyDrainAction::Cancel,
            Ok(drain(ProxyDrainPhase::Accepting, ActiveOperation::None)),
        );
        let report = unchanged.settled_report().expect("nothing to cancel sends nothing");
        assert!(report.applied && !report.changed);
        assert_eq!(report.failure, None);
        assert_eq!(report.after, report.before);
        assert_eq!(report.verification, MutationVerificationState::NotPerformed);

        let change = plan(
            ProxyDrainAction::Begin,
            Ok(drain(ProxyDrainPhase::Accepting, ActiveOperation::None)),
        );
        assert!(change.changes());
        assert_eq!(change.settled_report(), None);
    }

    #[test]
    fn an_acknowledged_write_is_verified_only_by_the_read_after_it() {
        let begin = plan(
            ProxyDrainAction::Begin,
            Ok(drain(ProxyDrainPhase::Accepting, ActiveOperation::None)),
        );
        let draining = drain(ProxyDrainPhase::Draining, ActiveOperation::Requested);

        let verified = begin.attempted_report(Ok(()), Ok(draining));
        assert!(verified.applied && verified.changed);
        assert_eq!(verified.verification, MutationVerificationState::Verified);
        assert_eq!(verified.failure, None);
        assert_eq!(verified.after, Some(draining));

        let still_accepting =
            begin.attempted_report(Ok(()), Ok(drain(ProxyDrainPhase::Accepting, ActiveOperation::None)));
        assert!(still_accepting.applied);
        assert_eq!(still_accepting.verification, MutationVerificationState::Failed);
        assert_eq!(still_accepting.failure, Some(MutationFailureCode::VerificationFailed));

        let unread = begin.attempted_report(Ok(()), Err(UNAVAILABLE));
        assert!(unread.applied && unread.retryable);
        assert_eq!(unread.after, None);
        assert_eq!(unread.failure, Some(MutationFailureCode::VerificationFailed));
    }

    #[test]
    fn a_refused_write_is_a_conflict_only_when_another_operation_holds_the_target() {
        let begin = plan(
            ProxyDrainAction::Begin,
            Ok(drain(ProxyDrainPhase::Accepting, ActiveOperation::None)),
        );

        let raced = begin.attempted_report(
            Err(UNAVAILABLE),
            Ok(drain(ProxyDrainPhase::Draining, ActiveOperation::Other)),
        );
        assert_eq!(raced.failure, Some(MutationFailureCode::Conflict));
        assert!(!raced.applied && !raced.changed && !raced.retryable);

        let unavailable = begin.attempted_report(
            Err(UNAVAILABLE),
            Ok(drain(ProxyDrainPhase::Accepting, ActiveOperation::None)),
        );
        assert_eq!(unavailable.failure, Some(MutationFailureCode::Unavailable));
        assert!(unavailable.retryable && !unavailable.changed);

        // The write was not acknowledged, yet the Proxy drains under this operation: the report
        // does not claim the write, it says that the target moved.
        let lost_response = begin.attempted_report(
            Err(UNAVAILABLE),
            Ok(drain(ProxyDrainPhase::Draining, ActiveOperation::Requested)),
        );
        assert!(!lost_response.applied && lost_response.changed);
        assert_eq!(lost_response.failure, Some(MutationFailureCode::Unavailable));
        assert_eq!(lost_response.verification, MutationVerificationState::NotPerformed);

        let unread = begin.attempted_report(Err(UNAVAILABLE), Err(UNAVAILABLE));
        assert_eq!(unread.after, None);
        assert_eq!(unread.failure, Some(MutationFailureCode::Unavailable));
    }

    #[test]
    fn supervised_loggers_are_lower_case_broker_modules_outside_protected_ones() {
        for accepted in [
            "rocketmq_broker::processor",
            "rocketmq_broker::processor::send_2",
            "rocketmq_broker::_internal",
        ] {
            assert!(is_supervised_broker_logger(accepted), "{accepted}");
        }
        let too_long = format!("rocketmq_broker::{}", "a".repeat(MAX_LOGGER_BYTES));
        for rejected in [
            "rocketmq_broker",
            "rocketmq_broker::",
            "rocketmq_broker::processor::",
            "rocketmq_broker::Processor",
            "rocketmq_broker::2fast",
            "rocketmq_broker::processor=trace",
            "rocketmq_broker::processor,rocketmq_store=trace",
            "rocketmq_store::commit_log",
            "rocketmq_broker::message",
            "rocketmq_broker::processor::send_message",
            "rocketmq_broker::auth",
            "rocketmq_broker::storehouse",
            " rocketmq_broker::processor",
            too_long.as_str(),
        ] {
            assert!(!is_supervised_broker_logger(rejected), "{rejected}");
        }
    }

    #[test]
    fn an_override_is_bounded_to_sixty_through_nine_hundred_seconds() {
        let with_ttl = |ttl| BrokerLogFilterOverride::try_new("rocketmq_broker::processor", BrokerLogLevel::Debug, ttl);
        for ttl in [MIN_LOG_FILTER_TTL_SECONDS, MAX_LOG_FILTER_TTL_SECONDS] {
            assert_eq!(with_ttl(ttl).expect("inclusive bound").ttl_seconds(), ttl);
        }
        for ttl in [0, 59, 901, u32::MAX] {
            assert!(with_ttl(ttl).is_err(), "{ttl}");
        }
        assert!(BrokerLogFilterOverride::try_new("rocketmq_broker::message", BrokerLogLevel::Debug, 120).is_err());
    }

    #[test]
    fn operation_identifiers_are_bounded_and_closed() {
        let longest = "a".repeat(MAX_OPERATION_ID_BYTES);
        for accepted in ["op-1", "INC-42/drain.1:a_b", longest.as_str()] {
            assert_eq!(validate_operation_id(accepted).expect("accepted"), accepted);
        }
        let too_long = "a".repeat(MAX_OPERATION_ID_BYTES + 1);
        for rejected in ["", " op-1", "op 1", "op\n1", "op@1", "op=1", "op,1", too_long.as_str()] {
            assert!(validate_operation_id(rejected).is_err(), "{rejected:?}");
        }
    }

    #[test]
    fn plans_and_requests_never_expose_an_address_or_an_operation_through_debug() {
        let request = ProxyDrainPreflightRequest {
            cluster: "cluster-a".to_owned(),
            proxy_addr: "192.0.2.7:8080".to_owned(),
            operation_id: "operation-1".to_owned(),
            action: ProxyDrainAction::Begin,
        };
        let sealed = plan(
            ProxyDrainAction::Begin,
            Ok(drain(ProxyDrainPhase::Accepting, ActiveOperation::None)),
        );
        for rendered in [format!("{request:?}"), format!("{sealed:?}")] {
            assert!(!rendered.contains("192.0.2.7"), "{rendered}");
            assert!(!rendered.contains("operation-1"), "{rendered}");
        }
    }

    #[test]
    fn an_observation_names_the_holder_without_its_identifier() {
        let state = ProxyDrainState::try_from_wire_parts(
            "rocketmq.proxy-drain.v1".to_owned(),
            "draining",
            Some("http://secret.internal/token".to_owned()),
            false,
            false,
            false,
            true,
            ProxyDrainPending::default(),
        )
        .expect("consistent state");
        let observed = ProxyDrainObservation::of(&state, "operation-1");
        assert_eq!(observed.operation, ActiveOperation::Other);
        let rendered = format!("{observed:?}");
        assert!(!rendered.contains("secret.internal"), "{rendered}");
        assert_eq!(
            ProxyDrainObservation::of(&state, "http://secret.internal/token").operation,
            ActiveOperation::Requested
        );
    }
}
