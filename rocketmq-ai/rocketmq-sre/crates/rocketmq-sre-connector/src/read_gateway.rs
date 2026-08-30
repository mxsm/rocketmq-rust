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

mod admin;
mod audit;
mod mcp;
mod policy;

use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::TenantId;
use tokio::sync::SemaphorePermit;

use self::admin::AdminReadAdapter;
use self::audit::ReadAudit;
use self::audit::ReadAuditOutcome;
use self::mcp::McpReadAdapter;
use self::policy::ReadPolicy;
use crate::ConnectorConfig;
use crate::ConnectorError;
use crate::EvidenceOperation;
use crate::mcp::McpGateway;
use crate::sources::CancelSignal;
use crate::sources::SourceOutput;
use crate::sources::bounded_future;
use crate::sources::sanitize_and_bound;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadAdapterKind {
    Mcp,
    Admin,
}

impl ReadAdapterKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Mcp => "mcp",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ReadAuditTarget {
    adapter: ReadAdapterKind,
    resource_class: &'static str,
}

impl ReadAuditTarget {
    pub(crate) const fn new(adapter: ReadAdapterKind, resource_class: &'static str) -> Self {
        Self {
            adapter,
            resource_class,
        }
    }
}

pub(crate) struct ReadContext<'a> {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub external_cluster: &'a str,
    pub subject: &'a str,
    pub correlation_id: CorrelationId,
    pub time_range_start: DateTime<Utc>,
    pub time_range_end: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub cancel: &'a CancelSignal,
}

pub(crate) struct ReadSession<'policy, 'context> {
    context: &'context ReadContext<'context>,
    _permit: SemaphorePermit<'policy>,
}

impl ReadSession<'_, '_> {
    pub(crate) const fn context(&self) -> &ReadContext<'_> {
        self.context
    }
}

pub(crate) enum CanonicalRead<'a> {
    Mcp(&'a EvidenceOperation),
    McpSystemResource(&'a str),
    Admin(&'a str),
    AdminProducerConnections { max_rows: usize },
    AdminConsumerConnections { consumer_group: &'a str, max_rows: usize },
}

impl CanonicalRead<'_> {
    const fn adapter_kind(&self) -> ReadAdapterKind {
        match self {
            Self::Mcp(_) | Self::McpSystemResource(_) => ReadAdapterKind::Mcp,
            Self::Admin(_) | Self::AdminProducerConnections { .. } | Self::AdminConsumerConnections { .. } => {
                ReadAdapterKind::Admin
            }
        }
    }

    const fn resource_class(&self) -> &'static str {
        match self {
            Self::Mcp(_) => "mcp_operation",
            Self::McpSystemResource(_) => "mcp_system_resource",
            Self::Admin(_) => "admin_resource",
            Self::AdminProducerConnections { .. } => "admin_producer_connections",
            Self::AdminConsumerConnections { .. } => "admin_consumer_connections",
        }
    }
}

pub(crate) trait ReadAdapter: Send + Sync {
    fn kind(&self) -> ReadAdapterKind;

    async fn read(
        &self,
        context: &ReadContext<'_>,
        request: &CanonicalRead<'_>,
    ) -> Result<SourceOutput, ConnectorError>;
}

pub(crate) struct ReadGateway<M, A> {
    policy: ReadPolicy,
    mcp: M,
    admin: A,
    audit: ReadAudit,
}

pub(crate) type ConnectorReadGateway<G> = ReadGateway<McpReadAdapter<G>, AdminReadAdapter>;

impl<M, A> ReadGateway<M, A>
where
    M: ReadAdapter,
    A: ReadAdapter,
{
    fn with_adapters(policy: ReadPolicy, mcp: M, admin: A) -> Self {
        debug_assert_eq!(mcp.kind(), ReadAdapterKind::Mcp);
        debug_assert_eq!(admin.kind(), ReadAdapterKind::Admin);
        Self {
            policy,
            mcp,
            admin,
            audit: ReadAudit::default(),
        }
    }

    pub(crate) async fn admit<'policy, 'context>(
        &'policy self,
        context: &'context ReadContext<'context>,
        audit_target: Option<ReadAuditTarget>,
    ) -> Result<ReadSession<'policy, 'context>, ConnectorError> {
        let started_at = Instant::now();
        if let Err(error) = self.policy.authorize(context) {
            self.audit_denial(audit_target, error.code, started_at, context.correlation_id)
                .await;
            return Err(error);
        }
        let permit = match self.policy.enter(context).await {
            Ok(permit) => permit,
            Err(error) => {
                self.audit_denial(audit_target, error.code, started_at, context.correlation_id)
                    .await;
                return Err(error);
            }
        };
        Ok(ReadSession {
            context,
            _permit: permit,
        })
    }

    pub(crate) async fn mcp_query(
        &self,
        session: &ReadSession<'_, '_>,
        operation: &EvidenceOperation,
    ) -> Result<SourceOutput, ConnectorError> {
        self.read(session, CanonicalRead::Mcp(operation)).await
    }

    pub(crate) async fn mcp_system_resource(
        &self,
        session: &ReadSession<'_, '_>,
        uri: &str,
    ) -> Result<SourceOutput, ConnectorError> {
        self.read(session, CanonicalRead::McpSystemResource(uri)).await
    }

    pub(crate) async fn admin_query(
        &self,
        session: &ReadSession<'_, '_>,
        resource: &str,
    ) -> Result<SourceOutput, ConnectorError> {
        self.read(session, CanonicalRead::Admin(resource)).await
    }

    pub(crate) async fn admin_producer_connections(
        &self,
        session: &ReadSession<'_, '_>,
        max_rows: usize,
    ) -> Result<SourceOutput, ConnectorError> {
        self.read(session, CanonicalRead::AdminProducerConnections { max_rows })
            .await
    }

    pub(crate) async fn admin_consumer_connections(
        &self,
        session: &ReadSession<'_, '_>,
        consumer_group: &str,
        max_rows: usize,
    ) -> Result<SourceOutput, ConnectorError> {
        self.read(
            session,
            CanonicalRead::AdminConsumerConnections {
                consumer_group,
                max_rows,
            },
        )
        .await
    }

    async fn read(
        &self,
        session: &ReadSession<'_, '_>,
        request: CanonicalRead<'_>,
    ) -> Result<SourceOutput, ConnectorError> {
        let context = session.context();
        let started_at = Instant::now();
        let adapter_kind = request.adapter_kind();
        let resource_class = request.resource_class();
        let adapter_result = match adapter_kind {
            ReadAdapterKind::Mcp => {
                bounded_future(context.deadline, context.cancel, self.mcp.read(context, &request)).await
            }
            ReadAdapterKind::Admin => {
                bounded_future(context.deadline, context.cancel, self.admin.read(context, &request)).await
            }
        };
        let result = match adapter_result {
            Ok(output) => self.finish_output(context, output),
            Err(error) => Err(error.with_correlation_id(context.correlation_id)),
        };
        let outcome = result
            .as_ref()
            .map(|_| ReadAuditOutcome::Allowed)
            .unwrap_or_else(|error| ReadAuditOutcome::from_error(error.code));
        self.audit
            .record(
                adapter_kind,
                resource_class,
                outcome,
                started_at.elapsed(),
                context.correlation_id,
            )
            .await;
        result
    }

    async fn audit_denial(
        &self,
        target: Option<ReadAuditTarget>,
        code: crate::ConnectorErrorCode,
        started_at: Instant,
        correlation_id: CorrelationId,
    ) {
        if let Some(target) = target {
            self.audit
                .record(
                    target.adapter,
                    target.resource_class,
                    ReadAuditOutcome::from_error(code),
                    started_at.elapsed(),
                    correlation_id,
                )
                .await;
        }
    }

    fn finish_output(
        &self,
        context: &ReadContext<'_>,
        mut output: SourceOutput,
    ) -> Result<SourceOutput, ConnectorError> {
        self.policy.validate_completion(context)?;
        let (content, bounded) = sanitize_and_bound(
            output.content,
            self.policy.max_rows,
            self.policy.max_bytes,
            self.policy.pseudonymization_key(),
        )
        .map_err(|error| error.with_correlation_id(context.correlation_id))?;
        output.content = content;
        if bounded {
            output.partial = true;
            output.warnings.push("read_gateway_output_bounded".to_owned());
        }
        Ok(output)
    }

    #[cfg(test)]
    async fn audit_events(&self) -> Vec<audit::ReadAuditEvent> {
        self.audit.events().await
    }
}

impl<G> ReadGateway<McpReadAdapter<G>, AdminReadAdapter>
where
    G: McpGateway,
{
    pub(crate) fn new(config: &ConnectorConfig, gateway: Arc<G>) -> Self {
        Self::with_adapters(
            ReadPolicy::from_config(config),
            McpReadAdapter::new(gateway),
            AdminReadAdapter::new(config.admin_source.clone()),
        )
    }

    pub(crate) fn admin_configured(&self) -> bool {
        self.admin.configured()
    }

    pub(crate) async fn initialize(&self, context: ChildServiceContext) -> Result<(), ConnectorError> {
        self.admin.initialize(context.component("admin-query")).await
    }

    pub(crate) async fn shutdown(&self) {
        self.admin.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use chrono::Duration as ChronoDuration;
    use serde_json::json;

    use super::*;
    use crate::ConnectorErrorCode;

    #[derive(Clone, Copy)]
    enum FakeBehavior {
        Available,
        Partial,
        Sensitive,
        Oversized,
        Failed,
        Pending,
    }

    struct FakeAdapter {
        kind: ReadAdapterKind,
        behavior: FakeBehavior,
        calls: AtomicUsize,
    }

    impl FakeAdapter {
        const fn new(kind: ReadAdapterKind, behavior: FakeBehavior) -> Self {
            Self {
                kind,
                behavior,
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl ReadAdapter for FakeAdapter {
        fn kind(&self) -> ReadAdapterKind {
            self.kind
        }

        async fn read(
            &self,
            _context: &ReadContext<'_>,
            _request: &CanonicalRead<'_>,
        ) -> Result<SourceOutput, ConnectorError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.behavior {
                FakeBehavior::Available => Ok(SourceOutput::available(json!({"rows": [1, 2]}), Utc::now())),
                FakeBehavior::Partial => {
                    let mut output = SourceOutput::available(json!({"rows": [1]}), Utc::now());
                    output.partial = true;
                    output.warnings.push("source_partial".to_owned());
                    Ok(output)
                }
                FakeBehavior::Sensitive => Ok(SourceOutput::available(
                    json!({
                        "access_token": "must-not-leave-gateway",
                        "rows": [1, 2, 3, 4, 5, 6, 7, 8, 9]
                    }),
                    Utc::now(),
                )),
                FakeBehavior::Oversized => Ok(SourceOutput::available(
                    json!({"diagnostic": "x".repeat(5000)}),
                    Utc::now(),
                )),
                FakeBehavior::Failed => Err(ConnectorError::source("fake adapter unavailable")),
                FakeBehavior::Pending => pending().await,
            }
        }
    }

    fn gateway(
        tenant_id: TenantId,
        cluster_id: ClusterId,
        max_per_minute: usize,
        mcp_behavior: FakeBehavior,
        admin_behavior: FakeBehavior,
    ) -> ReadGateway<FakeAdapter, FakeAdapter> {
        gateway_with_limits(tenant_id, cluster_id, 2, max_per_minute, mcp_behavior, admin_behavior)
    }

    fn gateway_with_limits(
        tenant_id: TenantId,
        cluster_id: ClusterId,
        max_concurrency: usize,
        max_per_minute: usize,
        mcp_behavior: FakeBehavior,
        admin_behavior: FakeBehavior,
    ) -> ReadGateway<FakeAdapter, FakeAdapter> {
        ReadGateway::with_adapters(
            ReadPolicy::for_test(tenant_id, cluster_id, max_concurrency, max_per_minute),
            FakeAdapter::new(ReadAdapterKind::Mcp, mcp_behavior),
            FakeAdapter::new(ReadAdapterKind::Admin, admin_behavior),
        )
    }

    fn context<'a>(
        tenant_id: TenantId,
        cluster_id: ClusterId,
        subject: &'a str,
        deadline: DateTime<Utc>,
        cancel: &'a CancelSignal,
    ) -> ReadContext<'a> {
        let now = Utc::now();
        ReadContext {
            tenant_id,
            cluster_id,
            external_cluster: "local",
            subject,
            correlation_id: CorrelationId::new(),
            time_range_start: now - ChronoDuration::seconds(1),
            time_range_end: now,
            deadline,
            cancel,
        }
    }

    fn future_deadline() -> DateTime<Utc> {
        Utc::now() + ChronoDuration::seconds(1)
    }

    async fn mcp_read<'a>(
        gateway: &ReadGateway<FakeAdapter, FakeAdapter>,
        context: &'a ReadContext<'a>,
    ) -> Result<SourceOutput, ConnectorError> {
        let session = gateway
            .admit(
                context,
                Some(ReadAuditTarget::new(ReadAdapterKind::Mcp, "logical_query")),
            )
            .await?;
        gateway.mcp_query(&session, &EvidenceOperation::ClusterOverview).await
    }

    async fn admin_read<'a>(
        gateway: &ReadGateway<FakeAdapter, FakeAdapter>,
        context: &'a ReadContext<'a>,
    ) -> Result<SourceOutput, ConnectorError> {
        let session = gateway
            .admit(
                context,
                Some(ReadAuditTarget::new(ReadAdapterKind::Admin, "logical_query")),
            )
            .await?;
        gateway.admin_query(&session, "admin/brokers").await
    }

    #[tokio::test]
    async fn authorized_read_is_bounded_redacted_and_audited() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway(
            tenant_id,
            cluster_id,
            8,
            FakeBehavior::Sensitive,
            FakeBehavior::Available,
        );
        let cancel = CancelSignal::default();
        let context = context(tenant_id, cluster_id, "operator", future_deadline(), &cancel);

        let output = mcp_read(&gateway, &context).await.expect("authorized read");

        assert!(output.content.get("access_token").is_none());
        assert_eq!(output.content["rows"].as_array().map(Vec::len), Some(8));
        assert!(output.partial);
        let events = gateway.audit_events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome, ReadAuditOutcome::Allowed);
        assert_eq!(events[0].correlation_id, context.correlation_id);
        let audit_debug = format!("{events:?}");
        for sensitive in [
            "operator",
            "must-not-leave-gateway",
            "access_token",
            "message body",
            "private key",
        ] {
            assert!(!audit_debug.contains(sensitive));
        }
    }

    #[tokio::test]
    async fn mcp_and_admin_share_output_and_audit_policy() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway(
            tenant_id,
            cluster_id,
            8,
            FakeBehavior::Sensitive,
            FakeBehavior::Sensitive,
        );
        let cancel = CancelSignal::default();
        let context = context(tenant_id, cluster_id, "operator", future_deadline(), &cancel);

        let mcp = mcp_read(&gateway, &context).await.expect("MCP read");
        let admin = admin_read(&gateway, &context).await.expect("Admin read");

        for output in [mcp, admin] {
            assert!(output.content.get("access_token").is_none());
            assert_eq!(output.content["rows"].as_array().map(Vec::len), Some(8));
            assert!(output.partial);
        }
        let events = gateway.audit_events().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].adapter, ReadAdapterKind::Mcp);
        assert_eq!(events[1].adapter, ReadAdapterKind::Admin);
        assert!(events.iter().all(|event| event.outcome == ReadAuditOutcome::Allowed));
    }

    #[tokio::test]
    async fn tenant_cluster_and_subject_checks_fail_before_adapter_dispatch() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway(
            tenant_id,
            cluster_id,
            8,
            FakeBehavior::Available,
            FakeBehavior::Available,
        );
        let cancel = CancelSignal::default();
        for denied in [
            context(TenantId::new(), cluster_id, "operator", future_deadline(), &cancel),
            context(tenant_id, ClusterId::new(), "operator", future_deadline(), &cancel),
            context(tenant_id, cluster_id, "", future_deadline(), &cancel),
        ] {
            let error = mcp_read(&gateway, &denied).await.expect_err("scope must be denied");
            assert!(matches!(
                error.code,
                ConnectorErrorCode::TenantMismatch
                    | ConnectorErrorCode::ClusterNotAllowed
                    | ConnectorErrorCode::UnauthorizedScope
            ));
        }
        assert_eq!(gateway.mcp.calls.load(Ordering::Relaxed), 0);
        assert!(
            gateway
                .audit_events()
                .await
                .iter()
                .all(|event| event.outcome == ReadAuditOutcome::Denied)
        );
    }

    #[tokio::test]
    async fn time_range_and_deadline_bounds_fail_before_adapter_dispatch() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway(
            tenant_id,
            cluster_id,
            8,
            FakeBehavior::Available,
            FakeBehavior::Available,
        );
        let cancel = CancelSignal::default();
        let mut wide_range = context(tenant_id, cluster_id, "operator", future_deadline(), &cancel);
        wide_range.time_range_start = wide_range.time_range_end - ChronoDuration::seconds(61);
        let long_deadline = context(
            tenant_id,
            cluster_id,
            "operator",
            Utc::now() + ChronoDuration::seconds(6),
            &cancel,
        );

        for denied in [&wide_range, &long_deadline] {
            let error = mcp_read(&gateway, denied)
                .await
                .expect_err("bounded context must be denied");
            assert_eq!(error.code, ConnectorErrorCode::InvalidEvidenceQuery);
        }
        assert_eq!(gateway.mcp.calls.load(Ordering::Relaxed), 0);
        assert!(
            gateway
                .audit_events()
                .await
                .iter()
                .all(|event| event.outcome == ReadAuditOutcome::Denied)
        );
    }

    #[tokio::test]
    async fn mcp_and_admin_share_one_rate_budget() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway(
            tenant_id,
            cluster_id,
            1,
            FakeBehavior::Available,
            FakeBehavior::Available,
        );
        let cancel = CancelSignal::default();
        let context = context(tenant_id, cluster_id, "operator", future_deadline(), &cancel);

        mcp_read(&gateway, &context).await.expect("first read");
        let error = admin_read(&gateway, &context).await.expect_err("shared rate limit");

        assert_eq!(error.code, ConnectorErrorCode::RateLimited);
        assert_eq!(gateway.admin.calls.load(Ordering::Relaxed), 0);
        assert_eq!(gateway.audit_events().await[1].outcome, ReadAuditOutcome::RateLimited);
    }

    #[tokio::test]
    async fn mcp_and_admin_share_one_concurrency_budget() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway_with_limits(
            tenant_id,
            cluster_id,
            1,
            8,
            FakeBehavior::Available,
            FakeBehavior::Available,
        );
        let first_cancel = CancelSignal::default();
        let first_context = context(tenant_id, cluster_id, "operator-a", future_deadline(), &first_cancel);
        let held = gateway
            .admit(
                &first_context,
                Some(ReadAuditTarget::new(ReadAdapterKind::Mcp, "logical_query")),
            )
            .await
            .expect("first admission");
        let second_cancel = CancelSignal::default();
        let second_context = context(tenant_id, cluster_id, "operator-b", future_deadline(), &second_cancel);

        let error = admin_read(&gateway, &second_context)
            .await
            .expect_err("shared concurrency limit");
        assert_eq!(error.code, ConnectorErrorCode::RateLimited);
        assert_eq!(gateway.admin.calls.load(Ordering::Relaxed), 0);

        drop(held);
        admin_read(&gateway, &second_context)
            .await
            .expect("admission recovers after permit release");
    }

    #[tokio::test]
    async fn cancellation_and_timeout_are_fail_closed_and_audited() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let cancelled_gateway = gateway(
            tenant_id,
            cluster_id,
            8,
            FakeBehavior::Available,
            FakeBehavior::Available,
        );
        let cancel = CancelSignal::default();
        cancel.cancel();
        let cancelled_context = context(tenant_id, cluster_id, "operator", future_deadline(), &cancel);
        let error = mcp_read(&cancelled_gateway, &cancelled_context)
            .await
            .expect_err("cancelled read");
        assert_eq!(error.code, ConnectorErrorCode::QueryCancelled);
        assert_eq!(
            cancelled_gateway.audit_events().await[0].outcome,
            ReadAuditOutcome::Cancelled
        );

        let timeout_gateway = gateway(tenant_id, cluster_id, 8, FakeBehavior::Pending, FakeBehavior::Available);
        let active = CancelSignal::default();
        let timeout_context = context(
            tenant_id,
            cluster_id,
            "operator",
            Utc::now() + ChronoDuration::milliseconds(20),
            &active,
        );
        let error = mcp_read(&timeout_gateway, &timeout_context)
            .await
            .expect_err("timed out read");
        assert_eq!(error.code, ConnectorErrorCode::DeadlineExceeded);
        assert_eq!(
            timeout_gateway.audit_events().await[0].outcome,
            ReadAuditOutcome::TimedOut
        );
    }

    #[tokio::test]
    async fn source_failure_is_typed_and_audited_without_sensitive_context() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let gateway = gateway(tenant_id, cluster_id, 8, FakeBehavior::Failed, FakeBehavior::Available);
        let cancel = CancelSignal::default();
        let context = context(tenant_id, cluster_id, "operator", future_deadline(), &cancel);

        let error = mcp_read(&gateway, &context).await.expect_err("source failure");

        assert_eq!(error.code, ConnectorErrorCode::SourceUnavailable);
        let events = gateway.audit_events().await;
        let event = &events[0];
        assert_eq!(event.outcome, ReadAuditOutcome::SourceFailed);
        assert_eq!(event.resource_class, "mcp_operation");
    }

    #[tokio::test]
    async fn partial_output_is_preserved_and_oversized_output_fails_closed() {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        let partial_gateway = gateway(tenant_id, cluster_id, 8, FakeBehavior::Partial, FakeBehavior::Available);
        let cancel = CancelSignal::default();
        let context = context(tenant_id, cluster_id, "operator", future_deadline(), &cancel);

        let output = mcp_read(&partial_gateway, &context).await.expect("partial output");
        assert!(output.partial);
        assert_eq!(output.warnings, vec!["source_partial"]);

        let oversized_gateway = gateway(
            tenant_id,
            cluster_id,
            8,
            FakeBehavior::Oversized,
            FakeBehavior::Available,
        );
        let error = mcp_read(&oversized_gateway, &context)
            .await
            .expect_err("oversized output");
        assert_eq!(error.code, ConnectorErrorCode::OutputTooLarge);
        assert_eq!(
            oversized_gateway.audit_events().await[0].outcome,
            ReadAuditOutcome::SourceFailed
        );
    }
}
