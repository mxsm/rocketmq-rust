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

//! Operation-bound changes of one Proxy or one Broker master.
//!
//! The session has no query capability, so the reads a plan and its verification need go through
//! the narrow state reads of the mutation capability itself.

use super::*;
use crate::core::broker::effective_log_level;
use crate::core::broker::is_valid_remoting_endpoint;
use crate::core::supervised_operation::validate_operation_id;
use crate::core::supervised_operation::ActiveOperation;
use crate::core::supervised_operation::BrokerLogFilterAction;
use crate::core::supervised_operation::BrokerLogFilterObservation;
use crate::core::supervised_operation::BrokerLogFilterPlan;
use crate::core::supervised_operation::BrokerLogFilterPreflightRequest;
use crate::core::supervised_operation::BrokerLogFilterReport;
use crate::core::supervised_operation::OperationFailure;
use crate::core::supervised_operation::OperationPlan;
use crate::core::supervised_operation::ProxyDrainAction;
use crate::core::supervised_operation::ProxyDrainObservation;
use crate::core::supervised_operation::ProxyDrainPlan;
use crate::core::supervised_operation::ProxyDrainPreflightRequest;
use crate::core::supervised_operation::ProxyDrainReport;
use crate::core::supervised_operation::SupervisedOperationAdmin;

impl SupervisedOperationAdmin for MutationAdminSession {
    fn preflight_proxy_drain<'a>(
        &'a mut self,
        request: &'a ProxyDrainPreflightRequest,
    ) -> AdminFuture<'a, ProxyDrainPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_proxy_drain_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), request).await
        })
    }

    fn execute_proxy_drain<'a>(&'a mut self, plan: &'a ProxyDrainPlan) -> AdminFuture<'a, ProxyDrainReport> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_proxy_drain_with_admin(&self.inner.inner, &self.plan_seal, plan).await
        })
    }

    fn preflight_broker_log_filter<'a>(
        &'a mut self,
        request: &'a BrokerLogFilterPreflightRequest,
    ) -> AdminFuture<'a, BrokerLogFilterPlan> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            preflight_broker_log_filter_with_admin(&self.inner.inner, Arc::clone(&self.plan_seal), request).await
        })
    }

    fn execute_broker_log_filter<'a>(
        &'a mut self,
        plan: &'a BrokerLogFilterPlan,
    ) -> AdminFuture<'a, BrokerLogFilterReport> {
        Box::pin(async move {
            self.inner.ensure_open()?;
            execute_broker_log_filter_with_admin(&self.inner.inner, &self.plan_seal, plan).await
        })
    }
}

async fn preflight_proxy_drain_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &ProxyDrainPreflightRequest,
) -> AdminResult<ProxyDrainPlan> {
    let cluster = require_non_empty("cluster", &request.cluster)?.to_owned();
    let operation_id = validate_operation_id(&request.operation_id)?.to_owned();
    if !is_valid_remoting_endpoint(&request.proxy_addr) {
        return Err(AdminError::invalid_argument(
            "proxyAddr",
            "must be one bounded remoting endpoint",
        ));
    }
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    // A Proxy is no member of the topology. It is still validated first, so that no drain is
    // planned through a session whose view of the selected cluster is broken.
    master_targets_by_cluster_name(&cluster_info, &cluster)?;
    let before = read_proxy_drain(admin, &request.proxy_addr, &operation_id).await;
    Ok(OperationPlan::new(
        seal,
        cluster,
        request.proxy_addr.clone(),
        operation_id,
        request.action,
        before,
    ))
}

async fn execute_proxy_drain_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &ProxyDrainPlan,
) -> AdminResult<ProxyDrainReport> {
    ensure_same_plan_seal(session_seal, &plan.seal)?;
    if let Some(report) = plan.settled_report() {
        return Ok(report);
    }
    let proxy_addr = CheetahString::from(plan.target_addr.as_str());
    let operation_id = CheetahString::from(plan.operation_id.as_str());
    let write = match plan.action {
        ProxyDrainAction::Begin => admin.begin_proxy_drain(proxy_addr, operation_id).await,
        ProxyDrainAction::Cancel => admin.cancel_proxy_drain(proxy_addr, operation_id).await,
    };
    // The state a write answers with is not the postcondition; the Proxy is read again.
    let write = write.map(drop).map_err(|error| unavailable(&error));
    let observed = read_proxy_drain(admin, &plan.target_addr, &plan.operation_id).await;
    Ok(plan.attempted_report(write, observed))
}

async fn read_proxy_drain<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    proxy_addr: &str,
    operation_id: &str,
) -> Result<ProxyDrainObservation, OperationFailure> {
    let state = admin
        .mutation_proxy_drain_state(CheetahString::from(proxy_addr))
        .await
        .map_err(|error| unavailable(&error))?;
    map_proxy_drain_state(state)
        .map(|state| ProxyDrainObservation::of(&state, operation_id))
        .map_err(|_| OperationFailure {
            code: MutationFailureCode::InvalidData,
            retryable: false,
        })
}

async fn preflight_broker_log_filter_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    seal: Arc<MutationPlanSeal>,
    request: &BrokerLogFilterPreflightRequest,
) -> AdminResult<BrokerLogFilterPlan> {
    let cluster = require_non_empty("cluster", &request.cluster)?.to_owned();
    let broker_name = require_non_empty("brokerName", &request.broker_name)?.to_owned();
    let operation_id = validate_operation_id(&request.operation_id)?.to_owned();
    let cluster_info = admin
        .mutation_cluster_info()
        .await
        .map_err(|error| backend_error("mutation_cluster_info", error))?;
    let all_targets = master_targets_by_cluster_name(&cluster_info, &cluster)?;
    let Some((_, broker_addr)) = select_metadata_targets(all_targets, Some(std::slice::from_ref(&broker_name)))?.pop()
    else {
        return Err(AdminError::invalid_argument(
            "brokerName",
            "is outside the selected cluster master topology",
        ));
    };
    let before = read_broker_log_filter(admin, broker_addr.as_str(), &operation_id, &request.action).await;
    Ok(OperationPlan::new(
        seal,
        cluster,
        broker_addr.to_string(),
        operation_id,
        request.action.clone(),
        before,
    ))
}

async fn execute_broker_log_filter_with_admin<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    session_seal: &Arc<MutationPlanSeal>,
    plan: &BrokerLogFilterPlan,
) -> AdminResult<BrokerLogFilterReport> {
    ensure_same_plan_seal(session_seal, &plan.seal)?;
    if let Some(report) = plan.settled_report() {
        return Ok(report);
    }
    let broker_addr = CheetahString::from(plan.target_addr.as_str());
    let operation_id = CheetahString::from(plan.operation_id.as_str());
    let write = match &plan.action {
        BrokerLogFilterAction::Set(requested) => {
            admin
                .set_broker_log_filter_ttl(
                    broker_addr,
                    CheetahString::from(requested.logger()),
                    CheetahString::from(requested.level().as_uppercase()),
                    requested.ttl_seconds(),
                    operation_id,
                )
                .await
        }
        BrokerLogFilterAction::Restore => admin.restore_broker_log_filter(broker_addr, operation_id).await,
    };
    let write = write.map_err(|error| unavailable(&error));
    let observed = read_broker_log_filter(admin, &plan.target_addr, &plan.operation_id, &plan.action).await;
    Ok(plan.attempted_report(write, observed))
}

async fn read_broker_log_filter<A: MQAdminMutationExt + ?Sized>(
    admin: &A,
    broker_addr: &str,
    operation_id: &str,
    action: &BrokerLogFilterAction,
) -> Result<BrokerLogFilterObservation, OperationFailure> {
    let status = admin
        .mutation_broker_log_filter_status(CheetahString::from(broker_addr))
        .await
        .map_err(|error| unavailable(&error))?;
    if !status.supported {
        // A Broker that lacks the control does not gain it by a retry.
        return Err(OperationFailure {
            code: MutationFailureCode::Unavailable,
            retryable: false,
        });
    }
    let level = match action {
        BrokerLogFilterAction::Set(requested) => status
            .effective_filter
            .as_deref()
            .and_then(|filter| effective_log_level(filter, requested.logger())),
        BrokerLogFilterAction::Restore => None,
    };
    Ok(BrokerLogFilterObservation {
        level,
        operation: ActiveOperation::classify(status.active_operation_id.as_deref(), operation_id),
        expires_at_millis: status.expires_at_millis,
    })
}

fn unavailable(error: &CanonicalError) -> OperationFailure {
    OperationFailure {
        code: MutationFailureCode::Unavailable,
        retryable: crate::canonical_is_retryable(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use rocketmq_client_rust::ClientResult;
    use rocketmq_client_rust::MutationBrokerLogFilterStatus;
    use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainPendingBody;
    use rocketmq_protocol::protocol::body::proxy_drain::ProxyDrainStateResponseBody;
    use rocketmq_protocol::protocol::route::route_data_view::BrokerData;

    use super::*;
    use crate::core::broker::BrokerLogLevel;
    use crate::core::proxy::ProxyDrainPhase;
    use crate::core::supervised_operation::BrokerLogFilterOverride;

    const PROXY: &str = "192.0.2.7:8080";
    const MASTER: &str = "10.0.0.1:10911";
    const SLAVE: &str = "10.0.0.1:10912";
    const BASELINE: &str = "info";

    /// What the fake does when a named call arrives.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Fault {
        /// Fail the call with a connection failure.
        Fail(&'static str),
        /// Let another operation take the target just before the call is served.
        Race(&'static str),
    }

    /// A Proxy and a Broker master that behave like the real ones for drain and log-filter calls.
    struct OperationAdmin {
        cluster_info: ClusterInfo,
        drain: Mutex<ProxyDrainStateResponseBody>,
        filter: Mutex<MutationBrokerLogFilterStatus>,
        calls: Mutex<Vec<(&'static str, String)>>,
        faults: Mutex<Vec<Fault>>,
    }

    impl OperationAdmin {
        fn new() -> Self {
            let broker = BrokerData::new(
                "cluster-a".into(),
                "broker-0".into(),
                HashMap::from([(MASTER_ID, MASTER.into()), (1, SLAVE.into())]),
                None,
            );
            Self {
                cluster_info: ClusterInfo::new(
                    Some(HashMap::from([("broker-0".into(), broker)])),
                    Some(HashMap::from([(
                        "cluster-a".into(),
                        HashSet::from([CheetahString::from("broker-0")]),
                    )])),
                ),
                drain: Mutex::new(drain_body("accepting", None)),
                filter: Mutex::new(MutationBrokerLogFilterStatus {
                    supported: true,
                    effective_filter: Some(BASELINE.to_owned()),
                    ..MutationBrokerLogFilterStatus::default()
                }),
                calls: Mutex::new(Vec::new()),
                faults: Mutex::new(Vec::new()),
            }
        }

        fn inject(&self, fault: Fault) {
            self.faults.lock().expect("faults").push(fault);
        }

        /// Records the call, then applies and consumes the fault registered for it.
        fn enter(&self, call: &'static str, endpoint: &CheetahString) -> ClientResult<()> {
            self.calls.lock().expect("calls").push((call, endpoint.to_string()));
            let mut faults = self.faults.lock().expect("faults");
            let Some(index) = faults
                .iter()
                .position(|fault| matches!(fault, Fault::Fail(name) | Fault::Race(name) if *name == call))
            else {
                return Ok(());
            };
            match faults.remove(index) {
                Fault::Fail(_) => Err(connection_failure().into()),
                Fault::Race(_) => {
                    *self.drain.lock().expect("drain") = drain_body("draining", Some("other-operation"));
                    let mut filter = self.filter.lock().expect("filter");
                    filter.effective_filter = Some("info,rocketmq_broker::client=debug".to_owned());
                    filter.active_operation_id = Some("other-operation".to_owned());
                    Ok(())
                }
            }
        }

        fn calls(&self) -> Vec<(&'static str, String)> {
            self.calls.lock().expect("calls").clone()
        }

        fn writes(&self) -> usize {
            self.calls()
                .iter()
                .filter(|(call, _)| !matches!(*call, "cluster_info" | "drain_state" | "filter_status"))
                .count()
        }

        fn drain_phase(&self) -> String {
            self.drain.lock().expect("drain").phase.clone()
        }

        fn effective_filter(&self) -> Option<String> {
            self.filter.lock().expect("filter").effective_filter.clone()
        }
    }

    fn drain_body(phase: &str, operation_id: Option<&str>) -> ProxyDrainStateResponseBody {
        let accepting = phase == "accepting";
        ProxyDrainStateResponseBody {
            schema_version: "rocketmq.proxy-drain.v1".to_owned(),
            phase: phase.to_owned(),
            operation_id: operation_id.map(ToOwned::to_owned),
            admission_open: accepting,
            routing_open: accepting,
            readiness_published: accepting,
            zero_pending: true,
            pending: ProxyDrainPendingBody::default(),
        }
    }

    fn connection_failure() -> CanonicalError {
        rocketmq_error::Error::caused_by(
            &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
            std::io::Error::other("test connection failure"),
        )
        .with_context(rocketmq_error::ErrorContext::new().with_text(rocketmq_error::fields::PHASE, "connect"))
    }

    fn refused<T>() -> ClientResult<T> {
        Err(crate::canonical_admin_validation_failed("test", "refused by the fake target").into())
    }

    /// Declares the capability methods these tests never reach.
    macro_rules! unreached {
        ($($name:ident($($argument:ident: $kind:ty),*) -> $output:ty;)*) => {
            $(
                async fn $name(&self, $($argument: $kind),*) -> ClientResult<$output> {
                    $(let _ = $argument;)*
                    refused()
                }
            )*
        };
    }

    impl MQAdminMutationExt for OperationAdmin {
        async fn mutation_cluster_info(&self) -> ClientResult<ClusterInfo> {
            self.enter("cluster_info", &CheetahString::from_static_str(""))?;
            Ok(self.cluster_info.clone())
        }

        async fn mutation_proxy_drain_state(
            &self,
            proxy_addr: CheetahString,
        ) -> ClientResult<ProxyDrainStateResponseBody> {
            self.enter("drain_state", &proxy_addr)?;
            Ok(self.drain.lock().expect("drain").clone())
        }

        async fn begin_proxy_drain(
            &self,
            proxy_addr: CheetahString,
            operation_id: CheetahString,
        ) -> ClientResult<ProxyDrainStateResponseBody> {
            self.enter("begin_drain", &proxy_addr)?;
            let mut drain = self.drain.lock().expect("drain");
            if drain.phase != "accepting" {
                return match drain.operation_id.as_deref() {
                    Some(active) if active == operation_id.as_str() => Ok(drain.clone()),
                    _ => refused(),
                };
            }
            *drain = drain_body("draining", Some(operation_id.as_str()));
            Ok(drain.clone())
        }

        async fn cancel_proxy_drain(
            &self,
            proxy_addr: CheetahString,
            operation_id: CheetahString,
        ) -> ClientResult<ProxyDrainStateResponseBody> {
            self.enter("cancel_drain", &proxy_addr)?;
            let mut drain = self.drain.lock().expect("drain");
            if drain.phase == "accepting" && drain.operation_id.is_none() {
                return Ok(drain.clone());
            }
            if drain.operation_id.as_deref() != Some(operation_id.as_str()) {
                return refused();
            }
            *drain = drain_body("accepting", None);
            Ok(drain.clone())
        }

        async fn mutation_broker_log_filter_status(
            &self,
            broker_addr: CheetahString,
        ) -> ClientResult<MutationBrokerLogFilterStatus> {
            self.enter("filter_status", &broker_addr)?;
            Ok(self.filter.lock().expect("filter").clone())
        }

        // Like the Broker, a set replaces whatever override is active without asking.
        async fn set_broker_log_filter_ttl(
            &self,
            broker_addr: CheetahString,
            logger: CheetahString,
            level: CheetahString,
            ttl_seconds: u32,
            operation_id: CheetahString,
        ) -> ClientResult<()> {
            self.enter("set_filter", &broker_addr)?;
            let mut filter = self.filter.lock().expect("filter");
            filter.effective_filter = Some(format!("{BASELINE},{logger}={}", level.as_str().to_ascii_lowercase()));
            filter.active_operation_id = Some(operation_id.to_string());
            filter.expires_at_millis = Some(u64::from(ttl_seconds) * 1_000);
            Ok(())
        }

        // Like the Broker, a restore ends the active override whichever operation asks.
        async fn restore_broker_log_filter(
            &self,
            broker_addr: CheetahString,
            operation_id: CheetahString,
        ) -> ClientResult<()> {
            let _ = operation_id;
            self.enter("restore_filter", &broker_addr)?;
            let mut filter = self.filter.lock().expect("filter");
            filter.last_completed_operation_id = filter.active_operation_id.take();
            filter.effective_filter = Some(BASELINE.to_owned());
            filter.expires_at_millis = None;
            Ok(())
        }

        unreached! {
            broker_config_generation(broker_addr: CheetahString) -> u64;
            patch_broker_config_if_generation(
                broker_addr: CheetahString,
                expected_generation: u64,
                properties: HashMap<CheetahString, CheetahString>
            ) -> ClientBrokerConfigPatchResult;
            patch_topic_config_if_version(
                broker_addr: CheetahString,
                topic: CheetahString,
                expected_version: u64,
                patch: ClientTopicConfigPatch
            ) -> ClientTopicConfigPatchResult;
            patch_subscription_group_config_if_version(
                broker_addr: CheetahString,
                group: CheetahString,
                expected_version: u64,
                patch: ClientSubscriptionGroupConfigPatch
            ) -> ClientSubscriptionGroupConfigPatchResult;
            upsert_topic_config(broker_addr: CheetahString, config: TopicConfig) -> ();
            remove_topic(topic_name: CheetahString, cluster_name: CheetahString) -> ();
            reset_consumer_offset(
                cluster_name: Option<CheetahString>,
                topic: CheetahString,
                consumer_group: CheetahString,
                timestamp: u64,
                force: bool
            ) -> HashMap<rocketmq_model::common::message::message_queue::MessageQueue, u64>;
            upsert_subscription_group(broker_addr: CheetahString, config: SubscriptionGroupConfig) -> ();
            remove_subscription_group(
                broker_addr: CheetahString,
                group_name: CheetahString,
                remove_offset: Option<bool>
            ) -> ();
            remove_subscription_groups(
                broker_addr: CheetahString,
                group_names: Vec<CheetahString>,
                clean_offset: bool
            ) -> ();
            configure_message_request_mode(
                broker_addr: CheetahString,
                topic: CheetahString,
                consumer_group: CheetahString,
                mode: rocketmq_model::common::message::message_enum::MessageRequestMode,
                pop_work_group_size: i32,
                timeout_millis: u64
            ) -> ();
            consume_directly(
                consumer_group: CheetahString,
                client_id: CheetahString,
                topic: CheetahString,
                message_id: CheetahString
            ) -> rocketmq_protocol::protocol::body::consume_message_directly_result::ConsumeMessageDirectlyResult;
            clone_consumer_group_offset(
                source_group: CheetahString,
                destination_group: CheetahString,
                topic: CheetahString,
                offline: bool
            ) -> ();
            mutation_topic_route(topic: CheetahString) -> Option<TopicRouteData>;
            mutation_topic_config(broker_addr: CheetahString, topic: CheetahString) -> TopicConfig;
            remove_topic_from_brokers(broker_addrs: HashSet<CheetahString>, topic: CheetahString) -> ();
            remove_topics_from_broker(broker_addr: CheetahString, topics: Vec<CheetahString>) -> ();
            remove_topic_from_name_servers(
                namesrv_addrs: HashSet<CheetahString>,
                cluster_name: Option<CheetahString>,
                topic: CheetahString
            ) -> ();
            mutation_name_server_addresses() -> Vec<CheetahString>;
            upsert_order_topic_config(topic: CheetahString, value: CheetahString, cluster_wide: bool) -> ();
            reset_consumer_offset_legacy(
                cluster_name: Option<CheetahString>,
                consumer_group: CheetahString,
                topic: CheetahString,
                timestamp: u64,
                force: bool
            ) -> Vec<rocketmq_protocol::protocol::admin::rollback_stats::RollbackStats>;
            view_message_for_mutation(topic: CheetahString, message_id: CheetahString) -> MessageExt;
        }
    }

    fn drain_request(action: ProxyDrainAction, operation_id: &str) -> ProxyDrainPreflightRequest {
        ProxyDrainPreflightRequest {
            cluster: "cluster-a".to_owned(),
            proxy_addr: PROXY.to_owned(),
            operation_id: operation_id.to_owned(),
            action,
        }
    }

    async fn drain(
        admin: &OperationAdmin,
        seal: &Arc<MutationPlanSeal>,
        action: ProxyDrainAction,
        operation_id: &str,
    ) -> ProxyDrainReport {
        let plan = preflight_proxy_drain_with_admin(admin, Arc::clone(seal), &drain_request(action, operation_id))
            .await
            .expect("drain preflight");
        execute_proxy_drain_with_admin(admin, seal, &plan)
            .await
            .expect("drain execution")
    }

    fn filter_request(action: BrokerLogFilterAction, operation_id: &str) -> BrokerLogFilterPreflightRequest {
        BrokerLogFilterPreflightRequest {
            cluster: "cluster-a".to_owned(),
            broker_name: "broker-0".to_owned(),
            operation_id: operation_id.to_owned(),
            action,
        }
    }

    fn debug_processor() -> BrokerLogFilterAction {
        BrokerLogFilterAction::Set(
            BrokerLogFilterOverride::try_new("rocketmq_broker::processor", BrokerLogLevel::Debug, 120)
                .expect("bounded override"),
        )
    }

    async fn filter(
        admin: &OperationAdmin,
        seal: &Arc<MutationPlanSeal>,
        action: BrokerLogFilterAction,
        operation_id: &str,
    ) -> BrokerLogFilterReport {
        let plan =
            preflight_broker_log_filter_with_admin(admin, Arc::clone(seal), &filter_request(action, operation_id))
                .await
                .expect("log-filter preflight");
        execute_broker_log_filter_with_admin(admin, seal, &plan)
            .await
            .expect("log-filter execution")
    }

    #[tokio::test]
    async fn begin_drain_dry_run_reads_state_without_mutating() {
        let admin = OperationAdmin::new();
        let plan = preflight_proxy_drain_with_admin(
            &admin,
            Arc::new(MutationPlanSeal),
            &drain_request(ProxyDrainAction::Begin, "operation-1"),
        )
        .await
        .expect("drain preflight");

        assert!(plan.changes());
        assert_eq!(plan.failure(), None);
        let before = plan.before().expect("state was read");
        assert_eq!(before.phase, ProxyDrainPhase::Accepting);
        assert_eq!(before.operation, ActiveOperation::None);
        // The topology is validated before the one configured endpoint is read, and nothing is
        // written.
        assert_eq!(
            admin.calls(),
            vec![("cluster_info", String::new()), ("drain_state", PROXY.to_owned())]
        );
        assert_eq!(admin.drain_phase(), "accepting");
    }

    #[tokio::test]
    async fn begin_then_cancel_restores_accepting_state() {
        let admin = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);

        let begun = drain(&admin, &seal, ProxyDrainAction::Begin, "operation-1").await;
        assert!(begun.applied && begun.changed);
        assert_eq!(begun.verification, MutationVerificationState::Verified);
        assert_eq!(begun.failure, None);
        let after = begun.after.expect("state was read again");
        assert_eq!(after.phase, ProxyDrainPhase::Draining);
        assert_eq!(after.operation, ActiveOperation::Requested);
        assert!(!after.admission_open && !after.routing_open);
        assert_eq!(
            admin.calls()[1..],
            [
                ("drain_state", PROXY.to_owned()),
                ("begin_drain", PROXY.to_owned()),
                ("drain_state", PROXY.to_owned()),
            ]
        );

        let cancelled = drain(&admin, &seal, ProxyDrainAction::Cancel, "operation-1").await;
        assert!(cancelled.applied && cancelled.changed);
        assert_eq!(cancelled.verification, MutationVerificationState::Verified);
        assert_eq!(
            cancelled.before.expect("state was read").operation,
            ActiveOperation::Requested
        );
        let after = cancelled.after.expect("state was read again");
        assert_eq!(after.phase, ProxyDrainPhase::Accepting);
        assert_eq!(after.operation, ActiveOperation::None);
        assert!(after.admission_open && after.routing_open);
        assert_eq!(admin.drain_phase(), "accepting");
    }

    #[tokio::test]
    async fn repeating_a_begin_or_a_cancel_sends_no_second_write() {
        let admin = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);

        let idle_cancel = drain(&admin, &seal, ProxyDrainAction::Cancel, "operation-1").await;
        assert!(idle_cancel.applied && !idle_cancel.changed);
        assert_eq!(idle_cancel.after, idle_cancel.before);
        assert_eq!(admin.writes(), 0);

        drain(&admin, &seal, ProxyDrainAction::Begin, "operation-1").await;
        let repeated = drain(&admin, &seal, ProxyDrainAction::Begin, "operation-1").await;
        assert!(repeated.applied && !repeated.changed);
        assert_eq!(repeated.failure, None);
        assert_eq!(repeated.verification, MutationVerificationState::NotPerformed);
        assert_eq!(admin.writes(), 1);
    }

    #[tokio::test]
    async fn a_drain_another_operation_holds_is_a_conflict_without_a_write() {
        let admin = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);
        drain(&admin, &seal, ProxyDrainAction::Begin, "other-operation").await;
        let writes = admin.writes();

        for action in [ProxyDrainAction::Begin, ProxyDrainAction::Cancel] {
            let report = drain(&admin, &seal, action, "operation-1").await;
            assert_eq!(report.failure, Some(MutationFailureCode::Conflict), "{action:?}");
            assert!(!report.applied && !report.changed && !report.retryable);
            assert_eq!(report.before.expect("state was read").operation, ActiveOperation::Other);
            assert_eq!(report.after, None);
        }
        assert_eq!(admin.writes(), writes);
        assert_eq!(admin.drain_phase(), "draining");
    }

    #[tokio::test]
    async fn a_begin_that_loses_a_race_reports_the_conflict_it_reads() {
        let admin = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);
        admin.inject(Fault::Race("begin_drain"));

        let report = drain(&admin, &seal, ProxyDrainAction::Begin, "operation-1").await;

        assert_eq!(report.failure, Some(MutationFailureCode::Conflict));
        assert!(!report.applied && !report.changed && !report.retryable);
        assert_eq!(
            report.after.expect("state was read again").operation,
            ActiveOperation::Other
        );
        assert_eq!(admin.writes(), 1);
    }

    #[tokio::test]
    async fn an_unreachable_proxy_is_a_retryable_failure_of_the_target() {
        let unread = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);
        unread.inject(Fault::Fail("drain_state"));
        let report = drain(&unread, &seal, ProxyDrainAction::Begin, "operation-1").await;
        assert_eq!(report.failure, Some(MutationFailureCode::Unavailable));
        assert!(report.retryable && !report.applied);
        assert_eq!(report.before, None);
        assert_eq!(unread.writes(), 0);

        let unwritten = OperationAdmin::new();
        unwritten.inject(Fault::Fail("begin_drain"));
        let report = drain(&unwritten, &seal, ProxyDrainAction::Begin, "operation-1").await;
        assert_eq!(report.failure, Some(MutationFailureCode::Unavailable));
        assert!(report.retryable && !report.applied && !report.changed);
        assert_eq!(
            report.after.expect("state was read again").phase,
            ProxyDrainPhase::Accepting
        );

        let unverified = OperationAdmin::new();
        let plan = preflight_proxy_drain_with_admin(
            &unverified,
            Arc::clone(&seal),
            &drain_request(ProxyDrainAction::Begin, "operation-1"),
        )
        .await
        .expect("drain preflight");
        unverified.inject(Fault::Fail("drain_state"));
        let report = execute_proxy_drain_with_admin(&unverified, &seal, &plan)
            .await
            .expect("drain execution");
        assert!(report.applied && report.changed);
        assert_eq!(report.verification, MutationVerificationState::Failed);
        assert_eq!(report.failure, Some(MutationFailureCode::VerificationFailed));
        assert_eq!(report.after, None);
    }

    #[tokio::test]
    async fn an_undecodable_drain_state_is_invalid_data_and_never_written_over() {
        let admin = OperationAdmin::new();
        // A Proxy that accepts work without admitting any contradicts itself.
        admin.drain.lock().expect("drain").admission_open = false;
        let report = drain(
            &admin,
            &Arc::new(MutationPlanSeal),
            ProxyDrainAction::Begin,
            "operation-1",
        )
        .await;
        assert_eq!(report.failure, Some(MutationFailureCode::InvalidData));
        assert!(!report.retryable);
        assert_eq!(admin.writes(), 0);
    }

    #[tokio::test]
    async fn a_drain_is_planned_only_after_the_selected_cluster_is_validated() {
        let seal = Arc::new(MutationPlanSeal);

        let unknown_cluster = OperationAdmin::new();
        let mut request = drain_request(ProxyDrainAction::Begin, "operation-1");
        request.cluster = "cluster-b".to_owned();
        assert!(
            preflight_proxy_drain_with_admin(&unknown_cluster, Arc::clone(&seal), &request)
                .await
                .is_err()
        );
        assert_eq!(unknown_cluster.calls(), vec![("cluster_info", String::new())]);

        let unreachable = OperationAdmin::new();
        unreachable.inject(Fault::Fail("cluster_info"));
        assert!(preflight_proxy_drain_with_admin(
            &unreachable,
            Arc::clone(&seal),
            &drain_request(ProxyDrainAction::Begin, "operation-1"),
        )
        .await
        .is_err());
        assert_eq!(unreachable.calls().len(), 1);

        // Malformed requests are refused before anything is asked.
        let silent = OperationAdmin::new();
        for (proxy_addr, operation_id) in [
            ("https://proxy.internal:8080/path", "operation-1"),
            ("proxy.internal", "operation-1"),
            (PROXY, ""),
            (PROXY, "operation 1"),
        ] {
            let mut request = drain_request(ProxyDrainAction::Begin, operation_id);
            request.proxy_addr = proxy_addr.to_owned();
            assert!(preflight_proxy_drain_with_admin(&silent, Arc::clone(&seal), &request)
                .await
                .is_err());
        }
        assert!(silent.calls().is_empty());
    }

    #[tokio::test]
    async fn a_plan_of_another_session_is_rejected_before_any_rpc() {
        let admin = OperationAdmin::new();
        let drain_plan = preflight_proxy_drain_with_admin(
            &admin,
            Arc::new(MutationPlanSeal),
            &drain_request(ProxyDrainAction::Begin, "operation-1"),
        )
        .await
        .expect("drain preflight");
        let filter_plan = preflight_broker_log_filter_with_admin(
            &admin,
            Arc::new(MutationPlanSeal),
            &filter_request(debug_processor(), "operation-1"),
        )
        .await
        .expect("log-filter preflight");
        let calls = admin.calls().len();
        let other_session = Arc::new(MutationPlanSeal);

        assert!(execute_proxy_drain_with_admin(&admin, &other_session, &drain_plan)
            .await
            .is_err());
        assert!(
            execute_broker_log_filter_with_admin(&admin, &other_session, &filter_plan)
                .await
                .is_err()
        );
        assert_eq!(admin.calls().len(), calls);
    }

    #[tokio::test]
    async fn set_then_restore_returns_to_the_previous_filter() {
        let admin = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);

        let set = filter(&admin, &seal, debug_processor(), "operation-1").await;
        assert!(set.applied && set.changed);
        assert_eq!(set.verification, MutationVerificationState::Verified);
        let before = set.before.expect("state was read");
        assert_eq!(before.level, Some(BrokerLogLevel::Info));
        assert_eq!(before.operation, ActiveOperation::None);
        assert_eq!(before.expires_at_millis, None);
        let after = set.after.expect("state was read again");
        assert_eq!(after.level, Some(BrokerLogLevel::Debug));
        assert_eq!(after.operation, ActiveOperation::Requested);
        assert_eq!(after.expires_at_millis, Some(120_000));
        // Only the master of the named Broker is asked.
        assert_eq!(
            admin.calls()[1..],
            [
                ("filter_status", MASTER.to_owned()),
                ("set_filter", MASTER.to_owned()),
                ("filter_status", MASTER.to_owned()),
            ]
        );

        let restored = filter(&admin, &seal, BrokerLogFilterAction::Restore, "operation-1").await;
        assert!(restored.applied && restored.changed);
        assert_eq!(restored.verification, MutationVerificationState::Verified);
        let after = restored.after.expect("state was read again");
        assert_eq!(after.operation, ActiveOperation::None);
        assert_eq!(after.expires_at_millis, None);
        assert_eq!(admin.effective_filter().as_deref(), Some(BASELINE));

        // The override is gone, so the logger is back at the level it had before the set.
        let replanned = preflight_broker_log_filter_with_admin(
            &admin,
            Arc::clone(&seal),
            &filter_request(debug_processor(), "operation-2"),
        )
        .await
        .expect("log-filter preflight");
        assert_eq!(
            replanned.before().expect("state was read").level,
            Some(BrokerLogLevel::Info)
        );
    }

    #[tokio::test]
    async fn restore_with_a_mismatched_operation_id_is_a_conflict() {
        let admin = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);
        filter(&admin, &seal, debug_processor(), "other-operation").await;
        let overridden = admin.effective_filter();
        let writes = admin.writes();

        // The Broker would end the override for any caller, so the refusal has to come before the
        // restore is sent.
        let report = filter(&admin, &seal, BrokerLogFilterAction::Restore, "operation-1").await;

        assert_eq!(report.failure, Some(MutationFailureCode::Conflict));
        assert!(!report.applied && !report.changed && !report.retryable);
        assert_eq!(report.before.expect("state was read").operation, ActiveOperation::Other);
        assert_eq!(admin.writes(), writes);
        assert_eq!(admin.effective_filter(), overridden);
    }

    #[tokio::test]
    async fn a_set_never_replaces_an_active_override() {
        let admin = OperationAdmin::new();
        let seal = Arc::new(MutationPlanSeal);
        filter(&admin, &seal, debug_processor(), "operation-1").await;
        let overridden = admin.effective_filter();
        let writes = admin.writes();

        let repeated = filter(&admin, &seal, debug_processor(), "operation-1").await;
        assert!(repeated.applied && !repeated.changed);
        assert_eq!(repeated.failure, None);

        let other_logger = BrokerLogFilterAction::Set(
            BrokerLogFilterOverride::try_new("rocketmq_broker::client", BrokerLogLevel::Debug, 120)
                .expect("bounded override"),
        );
        for (action, operation_id) in [(other_logger, "operation-1"), (debug_processor(), "operation-2")] {
            let report = filter(&admin, &seal, action, operation_id).await;
            assert_eq!(report.failure, Some(MutationFailureCode::Conflict), "{operation_id}");
            assert!(!report.applied && !report.changed);
        }
        assert_eq!(admin.writes(), writes);
        assert_eq!(admin.effective_filter(), overridden);
    }

    #[tokio::test]
    async fn a_restore_after_the_override_ended_changes_nothing() {
        let admin = OperationAdmin::new();
        let report = filter(
            &admin,
            &Arc::new(MutationPlanSeal),
            BrokerLogFilterAction::Restore,
            "operation-1",
        )
        .await;
        assert!(report.applied && !report.changed);
        assert_eq!(report.failure, None);
        assert_eq!(report.after, report.before);
        assert_eq!(admin.writes(), 0);
    }

    #[tokio::test]
    async fn a_broker_without_the_control_or_out_of_reach_is_never_written() {
        let seal = Arc::new(MutationPlanSeal);

        let unsupported = OperationAdmin::new();
        unsupported.filter.lock().expect("filter").supported = false;
        let report = filter(&unsupported, &seal, debug_processor(), "operation-1").await;
        assert_eq!(report.failure, Some(MutationFailureCode::Unavailable));
        assert!(!report.retryable && !report.applied);
        assert_eq!(unsupported.writes(), 0);

        let unreachable = OperationAdmin::new();
        unreachable.inject(Fault::Fail("filter_status"));
        let report = filter(&unreachable, &seal, debug_processor(), "operation-1").await;
        assert_eq!(report.failure, Some(MutationFailureCode::Unavailable));
        assert!(report.retryable);
        assert_eq!(unreachable.writes(), 0);

        let unwritten = OperationAdmin::new();
        unwritten.inject(Fault::Fail("set_filter"));
        let report = filter(&unwritten, &seal, debug_processor(), "operation-1").await;
        assert_eq!(report.failure, Some(MutationFailureCode::Unavailable));
        assert!(!report.applied && !report.changed);
        assert_eq!(unwritten.effective_filter().as_deref(), Some(BASELINE));
    }

    #[tokio::test]
    async fn a_log_filter_is_planned_only_for_a_master_of_the_selected_cluster() {
        let seal = Arc::new(MutationPlanSeal);

        for (cluster, broker_name) in [("cluster-b", "broker-0"), ("cluster-a", "broker-9")] {
            let admin = OperationAdmin::new();
            let mut request = filter_request(debug_processor(), "operation-1");
            request.cluster = cluster.to_owned();
            request.broker_name = broker_name.to_owned();
            assert!(
                preflight_broker_log_filter_with_admin(&admin, Arc::clone(&seal), &request)
                    .await
                    .is_err(),
                "{cluster} {broker_name}"
            );
            // Nothing but the topology was read.
            assert_eq!(admin.calls(), vec![("cluster_info", String::new())]);
        }

        let admin = OperationAdmin::new();
        filter(&admin, &seal, debug_processor(), "operation-1").await;
        assert!(admin.calls().iter().all(|(_, endpoint)| endpoint != SLAVE));
    }
}
