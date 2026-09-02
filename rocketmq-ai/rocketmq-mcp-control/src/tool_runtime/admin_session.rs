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

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_admin_core::core::supervised_mutation as admin;
use rocketmq_admin_core::core::supervised_mutation::SupervisedMutationAdmin;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminBuilder;
use rocketmq_admin_core::mutation_client_adapter::MutationAdminSession;

use super::group_before;
use super::group_dry_run;
use super::group_executed;
use super::map_group_to_admin;
use super::map_topic_to_admin;
use super::topic_before;
use super::topic_dry_run;
use super::topic_executed;
use super::MutationToolRequest;
use super::MutationToolResponse;
use super::MutationToolSession;
use super::MutationToolSessionFactory;
use super::RuntimeFuture;
use crate::config::MutationClusterConfig;
use crate::error::ControlError;
use crate::model::ClusterName;
use crate::tools;

pub(crate) trait SupervisedMutationBackend: Send {
    type TopicPlan: Send;
    type GroupPlan: Send;
    type OffsetPlan: Send;
    type BrokerPlan: Send;
    type RequestModePlan: Send;

    fn preflight_topic<'a>(
        &'a mut self,
        request: &'a admin::TopicMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> RuntimeFuture<'a, Result<Self::TopicPlan, ControlError>>;

    fn topic_targets(plan: &Self::TopicPlan) -> Vec<admin::MetadataPreflightTarget<admin::TopicReplacement>>;

    fn topic_failures(plan: &Self::TopicPlan) -> &[admin::MutationTargetFailure];

    fn execute_topic<'a>(
        &'a mut self,
        plan: &'a Self::TopicPlan,
    ) -> RuntimeFuture<'a, Result<admin::MetadataMutationOutcome, ControlError>>;

    fn preflight_group<'a>(
        &'a mut self,
        request: &'a admin::SubscriptionGroupMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> RuntimeFuture<'a, Result<Self::GroupPlan, ControlError>>;

    fn group_targets(
        plan: &Self::GroupPlan,
    ) -> Vec<admin::MetadataPreflightTarget<admin::SubscriptionGroupReplacement>>;

    fn group_failures(plan: &Self::GroupPlan) -> &[admin::MutationTargetFailure];

    fn execute_group<'a>(
        &'a mut self,
        plan: &'a Self::GroupPlan,
    ) -> RuntimeFuture<'a, Result<admin::MetadataMutationOutcome, ControlError>>;

    fn preview_offset<'a>(
        &'a mut self,
        request: &'a admin::OffsetResetPreviewRequest,
    ) -> RuntimeFuture<'a, Result<Self::OffsetPlan, ControlError>>;

    fn offset_rows(plan: &Self::OffsetPlan) -> Vec<admin::OffsetResetPreviewRow>;

    fn offset_failures(plan: &Self::OffsetPlan) -> &[admin::MutationTargetFailure];

    fn execute_offset<'a>(
        &'a mut self,
        plan: &'a Self::OffsetPlan,
    ) -> RuntimeFuture<'a, Result<admin::OffsetResetOutcome, ControlError>>;

    fn preflight_broker<'a>(
        &'a mut self,
        cluster: &'a str,
        broker_name: &'a str,
    ) -> RuntimeFuture<'a, Result<Self::BrokerPlan, ControlError>>;

    fn broker_targets(plan: &Self::BrokerPlan) -> Vec<admin::BrokerMutationConfigTarget>;

    fn broker_failures(plan: &Self::BrokerPlan) -> &[admin::MutationTargetFailure];

    fn execute_broker<'a>(
        &'a mut self,
        plan: &'a Self::BrokerPlan,
        patch: admin::BrokerMutationConfigPatch,
    ) -> RuntimeFuture<'a, Result<admin::BrokerMutationConfigOutcome, ControlError>>;

    fn preflight_request_mode<'a>(
        &'a mut self,
        request: &'a admin::RequestModePreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::RequestModePlan, ControlError>>;

    fn request_mode_targets(plan: &Self::RequestModePlan) -> Vec<(String, Option<admin::RequestModeValue>)>;

    fn request_mode_failures(plan: &Self::RequestModePlan) -> &[admin::MutationTargetFailure];

    fn execute_request_mode<'a>(
        &'a mut self,
        plan: &'a Self::RequestModePlan,
        timeout_millis: u64,
    ) -> RuntimeFuture<'a, Result<admin::RequestModeMutationOutcome, ControlError>>;

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>>;
}

impl SupervisedMutationBackend for MutationAdminSession {
    type TopicPlan = admin::TopicMutationPlan;
    type GroupPlan = admin::SubscriptionGroupMutationPlan;
    type OffsetPlan = admin::OffsetResetPlan;
    type BrokerPlan = admin::BrokerMutationConfigPlan;
    type RequestModePlan = admin::RequestModeMutationPlan;

    fn preflight_topic<'a>(
        &'a mut self,
        request: &'a admin::TopicMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> RuntimeFuture<'a, Result<Self::TopicPlan, ControlError>> {
        Box::pin(async move {
            self.preflight_topic_targets(request, broker_names)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn topic_targets(plan: &Self::TopicPlan) -> Vec<admin::MetadataPreflightTarget<admin::TopicReplacement>> {
        plan.preflight_targets()
    }

    fn topic_failures(plan: &Self::TopicPlan) -> &[admin::MutationTargetFailure] {
        plan.failures()
    }

    fn execute_topic<'a>(
        &'a mut self,
        plan: &'a Self::TopicPlan,
    ) -> RuntimeFuture<'a, Result<admin::MetadataMutationOutcome, ControlError>> {
        Box::pin(async move {
            SupervisedMutationAdmin::execute_topic(self, plan)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn preflight_group<'a>(
        &'a mut self,
        request: &'a admin::SubscriptionGroupMutationPreflightRequest,
        broker_names: &'a [String],
    ) -> RuntimeFuture<'a, Result<Self::GroupPlan, ControlError>> {
        Box::pin(async move {
            self.preflight_subscription_group_targets(request, broker_names)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn group_targets(
        plan: &Self::GroupPlan,
    ) -> Vec<admin::MetadataPreflightTarget<admin::SubscriptionGroupReplacement>> {
        plan.preflight_targets()
    }

    fn group_failures(plan: &Self::GroupPlan) -> &[admin::MutationTargetFailure] {
        plan.failures()
    }

    fn execute_group<'a>(
        &'a mut self,
        plan: &'a Self::GroupPlan,
    ) -> RuntimeFuture<'a, Result<admin::MetadataMutationOutcome, ControlError>> {
        Box::pin(async move {
            self.execute_subscription_group(plan)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn preview_offset<'a>(
        &'a mut self,
        request: &'a admin::OffsetResetPreviewRequest,
    ) -> RuntimeFuture<'a, Result<Self::OffsetPlan, ControlError>> {
        Box::pin(async move {
            self.preview_offset_reset(request)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn offset_rows(plan: &Self::OffsetPlan) -> Vec<admin::OffsetResetPreviewRow> {
        plan.rows()
    }

    fn offset_failures(plan: &Self::OffsetPlan) -> &[admin::MutationTargetFailure] {
        plan.failures()
    }

    fn execute_offset<'a>(
        &'a mut self,
        plan: &'a Self::OffsetPlan,
    ) -> RuntimeFuture<'a, Result<admin::OffsetResetOutcome, ControlError>> {
        Box::pin(async move {
            self.execute_offset_reset(plan)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn preflight_broker<'a>(
        &'a mut self,
        cluster: &'a str,
        broker_name: &'a str,
    ) -> RuntimeFuture<'a, Result<Self::BrokerPlan, ControlError>> {
        Box::pin(async move {
            self.preflight_broker_config_target(cluster, broker_name)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn broker_targets(plan: &Self::BrokerPlan) -> Vec<admin::BrokerMutationConfigTarget> {
        plan.targets()
    }

    fn broker_failures(plan: &Self::BrokerPlan) -> &[admin::MutationTargetFailure] {
        plan.failures()
    }

    fn execute_broker<'a>(
        &'a mut self,
        plan: &'a Self::BrokerPlan,
        patch: admin::BrokerMutationConfigPatch,
    ) -> RuntimeFuture<'a, Result<admin::BrokerMutationConfigOutcome, ControlError>> {
        Box::pin(async move {
            self.execute_broker_config_patch_verified(plan, patch)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn preflight_request_mode<'a>(
        &'a mut self,
        request: &'a admin::RequestModePreflightRequest,
    ) -> RuntimeFuture<'a, Result<Self::RequestModePlan, ControlError>> {
        Box::pin(async move {
            SupervisedMutationAdmin::preflight_request_mode(self, request)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn request_mode_targets(plan: &Self::RequestModePlan) -> Vec<(String, Option<admin::RequestModeValue>)> {
        plan.targets()
    }

    fn request_mode_failures(plan: &Self::RequestModePlan) -> &[admin::MutationTargetFailure] {
        plan.failures()
    }

    fn execute_request_mode<'a>(
        &'a mut self,
        plan: &'a Self::RequestModePlan,
        timeout_millis: u64,
    ) -> RuntimeFuture<'a, Result<admin::RequestModeMutationOutcome, ControlError>> {
        Box::pin(async move {
            SupervisedMutationAdmin::execute_request_mode_with_timeout(self, plan, timeout_millis)
                .await
                .map_err(|_| ControlError::execution_failed())
        })
    }

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>> {
        Box::pin(async move {
            MutationAdminSession::shutdown(self).await;
            Ok(())
        })
    }
}

pub(crate) struct AdminMutationToolSession<B> {
    backend: B,
}

impl<B> AdminMutationToolSession<B> {
    pub(crate) const fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B> MutationToolSession for AdminMutationToolSession<B>
where
    B: SupervisedMutationBackend,
{
    fn run<'a>(
        &'a mut self,
        request: MutationToolRequest,
    ) -> RuntimeFuture<'a, Result<MutationToolResponse, ControlError>> {
        Box::pin(async move {
            match request {
                MutationToolRequest::Topic(args) => run_topic(&mut self.backend, args)
                    .await
                    .map(MutationToolResponse::Topic),
                MutationToolRequest::ConsumerGroup(args) => run_group(&mut self.backend, args)
                    .await
                    .map(MutationToolResponse::ConsumerGroup),
                MutationToolRequest::ConsumerOffset(args) => super::remaining::run_offset(&mut self.backend, args)
                    .await
                    .map(MutationToolResponse::ConsumerOffset),
                MutationToolRequest::BrokerConfig(args) => super::remaining::run_broker(&mut self.backend, args)
                    .await
                    .map(MutationToolResponse::BrokerConfig),
                MutationToolRequest::ConsumerRequestMode(args) => {
                    super::remaining::run_request_mode(&mut self.backend, args)
                        .await
                        .map(MutationToolResponse::ConsumerRequestMode)
                }
            }
        })
    }

    fn shutdown(&mut self) -> RuntimeFuture<'_, Result<(), ControlError>> {
        self.backend.shutdown()
    }
}

async fn run_topic<B: SupervisedMutationBackend>(
    backend: &mut B,
    args: tools::UpsertTopicArgs,
) -> Result<tools::TopicMutationToolResponse, ControlError> {
    let request = admin::TopicMutationPreflightRequest {
        cluster: args.cluster.clone(),
        topic: args.topic.clone(),
        replacement: map_topic_to_admin(&args.replacement),
    };
    let plan = backend.preflight_topic(&request, &args.broker_names).await?;
    let before = topic_before(&args, B::topic_targets(&plan));
    if args.dry_run {
        return Ok(topic_dry_run(&args, before, B::topic_failures(&plan)));
    }
    let outcome = backend.execute_topic(&plan).await?;
    let observed = backend
        .preflight_topic(&request, &args.broker_names)
        .await
        .ok()
        .map(|plan| B::topic_targets(&plan));
    Ok(topic_executed(&args, before, outcome, observed))
}

async fn run_group<B: SupervisedMutationBackend>(
    backend: &mut B,
    args: tools::UpsertConsumerGroupArgs,
) -> Result<tools::ConsumerGroupMutationToolResponse, ControlError> {
    let request = admin::SubscriptionGroupMutationPreflightRequest {
        cluster: args.cluster.clone(),
        consumer_group: args.consumer_group.clone(),
        replacement: map_group_to_admin(&args.replacement),
    };
    let plan = backend.preflight_group(&request, &args.broker_names).await?;
    let before = group_before(&args, B::group_targets(&plan));
    if args.dry_run {
        return Ok(group_dry_run(&args, before, B::group_failures(&plan)));
    }
    let outcome = backend.execute_group(&plan).await?;
    let observed = backend
        .preflight_group(&request, &args.broker_names)
        .await
        .ok()
        .map(|plan| B::group_targets(&plan));
    Ok(group_executed(&args, before, outcome, observed))
}

pub(crate) struct AdminMutationToolFactory {
    runtime: Arc<rocketmq_admin_core::mutation_client_adapter::ClientRuntime>,
    clusters: BTreeMap<ClusterName, MutationClusterConfig>,
    sequence: AtomicU64,
}

impl AdminMutationToolFactory {
    pub(crate) fn new(
        service_context: rocketmq_runtime::ChildServiceContext,
        clusters: &[MutationClusterConfig],
    ) -> Result<Self, ControlError> {
        let runtime = rocketmq_admin_core::mutation_client_adapter::create_mutation_client_runtime(
            service_context.component("mutation-client"),
        )
        .map_err(|_| ControlError::invalid_config())?;
        Ok(Self {
            runtime,
            clusters: clusters
                .iter()
                .cloned()
                .map(|cluster| (cluster.name().clone(), cluster))
                .collect(),
            sequence: AtomicU64::new(0),
        })
    }
}

impl MutationToolSessionFactory for AdminMutationToolFactory {
    fn open<'a>(
        &'a self,
        cluster: &'a ClusterName,
    ) -> RuntimeFuture<'a, Result<Box<dyn MutationToolSession>, ControlError>> {
        Box::pin(async move {
            let config = self
                .clusters
                .get(cluster)
                .ok_or_else(ControlError::operation_unavailable)?;
            let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
            let identity = format!("mcp-control-{sequence}");
            let mut builder = MutationAdminBuilder::new(self.runtime.clone())
                .namesrv_addr(config.namesrv_addr())
                .use_tls(config.use_tls())
                .admin_group(identity.clone())
                .instance_name(identity);
            let (access_env, secret_env, token_env) = config.credential_envs();
            if let (Some(access_env), Some(secret_env)) = (access_env, secret_env) {
                let access = std::env::var(access_env).map_err(|_| ControlError::invalid_config())?;
                let secret = std::env::var(secret_env).map_err(|_| ControlError::invalid_config())?;
                let token = token_env
                    .map(std::env::var)
                    .transpose()
                    .map_err(|_| ControlError::invalid_config())?;
                let credentials = rocketmq_admin_core::core::security::AdminCredentials::try_new(access, secret, token)
                    .map_err(|_| ControlError::invalid_config())?;
                builder = builder.credentials(credentials);
            }
            let admin = builder
                .build_and_start()
                .await
                .map_err(|_| ControlError::execution_failed())?;
            Ok(Box::new(AdminMutationToolSession::new(admin)) as Box<dyn MutationToolSession>)
        })
    }
}

#[cfg(test)]
#[path = "admin_session/tests.rs"]
mod tests;
