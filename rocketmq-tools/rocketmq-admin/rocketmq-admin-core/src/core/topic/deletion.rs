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

#[cfg(any(feature = "client-adapter", test))]
use std::future::Future;

use cheetah_string::CheetahString;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_error::Result;
use rocketmq_model::common::topic::TopicValidator;
use serde::Deserialize;
use serde::Serialize;

/// Request for deleting one Topic from all masters in a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicRequest {
    topic: CheetahString,
    cluster_name: CheetahString,
    namesrv_addr: Option<String>,
}

impl DeleteTopicRequest {
    /// Normalizes the Topic and cluster names before selecting mutation targets.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument error for an invalid Topic or absent cluster.
    pub fn try_new(topic: impl Into<String>, cluster_name: Option<String>) -> Result<Self> {
        let topic = topic.into();
        let topic = topic.trim();
        if !TopicValidator::validate_topic(topic).valid() {
            return Err(invalid_request());
        }
        let cluster_name = cluster_name
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty())
            .ok_or_else(invalid_request)?;
        Ok(Self {
            topic: topic.into(),
            cluster_name: cluster_name.into(),
            namesrv_addr: None,
        })
    }

    pub fn with_optional_namesrv_addr(mut self, namesrv_addr: Option<String>) -> Self {
        self.namesrv_addr = namesrv_addr
            .map(|addr| addr.trim().to_owned())
            .filter(|addr| !addr.is_empty());
        self
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn cluster_name(&self) -> &CheetahString {
        &self.cluster_name
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    #[cfg(any(feature = "client-adapter", test))]
    pub(crate) fn validated(&self) -> Result<Self> {
        Self::try_new(self.topic.to_string(), Some(self.cluster_name.to_string()))
            .map(|request| request.with_optional_namesrv_addr(self.namesrv_addr.clone()))
    }
}

fn invalid_request() -> Error {
    Error::new(&rocketmq_error::CORE_ARGUMENT_INVALID)
        .with_context(ErrorContext::new().with_secret_presence(fields::MESSAGE_PRESENT))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTopicResult {
    pub topic: CheetahString,
    pub cluster_name: CheetahString,
    pub broker_addrs: Vec<CheetahString>,
    pub failures: Vec<TopicOperationFailure>,
    pub name_server_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicOperationFailure {
    pub broker_addr: CheetahString,
    pub error_code: String,
    pub error: String,
}

impl DeleteTopicResult {
    pub fn is_complete_success(&self) -> bool {
        self.failures.is_empty() && self.name_server_deleted
    }

    pub fn is_partial_failure(&self) -> bool {
        !self.broker_addrs.is_empty() && (!self.failures.is_empty() || !self.name_server_deleted)
    }

    /// Projects completion for an adapter that needs a success or error status.
    /// The full result remains available for rendering individual target outcomes.
    ///
    /// # Errors
    ///
    /// Returns permission denied when the first failed target reports that cause;
    /// otherwise returns a Broker operation error when deletion is incomplete.
    pub fn ensure_complete(&self) -> Result<()> {
        if self.is_complete_success() {
            return Ok(());
        }
        if self.failures.first().is_some_and(|failure| {
            matches!(
                failure.error_code.as_str(),
                "BROKER_PERMISSION_DENIED" | "auth.permission.denied"
            )
        }) {
            return Err(Error::new(&rocketmq_error::AUTH_PERMISSION_DENIED));
        }
        Err(Error::new(&rocketmq_error::BROKER_OPERATION_FAILED).with_context(
            ErrorContext::new()
                .with_text(fields::OPERATION_DIAGNOSTIC, "DELETE_TOPIC_IN_BROKER_LIST")
                .with_i64(fields::BROKER_CODE, -1),
        ))
    }
}

/// The SDK adapter owns metadata decoding and safe failure projection. This
/// use case owns target normalization and when route deletion is permitted.
#[cfg(any(feature = "client-adapter", test))]
pub(crate) trait TopicDeletionBackend {
    fn master_targets(&mut self, cluster: &CheetahString) -> impl Future<Output = Result<Vec<CheetahString>>> + Send;
    fn delete_broker(
        &mut self,
        topic: &CheetahString,
        broker: &CheetahString,
    ) -> impl Future<Output = std::result::Result<(), TopicOperationFailure>> + Send;
    fn delete_route(
        &mut self,
        topic: &CheetahString,
        cluster: &CheetahString,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[cfg(any(feature = "client-adapter", test))]
pub(crate) async fn execute_deletion(
    backend: &mut impl TopicDeletionBackend,
    request: &DeleteTopicRequest,
) -> Result<DeleteTopicResult> {
    // Deserialized requests must pass the same validation as CLI/TUI builders.
    let request = request.validated()?;
    let mut targets = backend.master_targets(request.cluster_name()).await?;
    targets.sort();
    targets.dedup();
    if targets.is_empty() {
        return Err(Error::new(&rocketmq_error::ROUTE_CLUSTER_NOT_FOUND));
    }
    let mut result = DeleteTopicResult {
        topic: request.topic.clone(),
        cluster_name: request.cluster_name.clone(),
        broker_addrs: Vec::new(),
        failures: Vec::new(),
        name_server_deleted: false,
    };
    for broker in targets {
        match backend.delete_broker(request.topic(), &broker).await {
            Ok(()) => result.broker_addrs.push(broker),
            Err(failure) => result.failures.push(failure),
        }
    }
    // Preserve the NameServer mapping whenever a master still owns the Topic.
    if result.failures.is_empty() {
        backend.delete_route(request.topic(), request.cluster_name()).await?;
        result.name_server_deleted = true;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Backend {
        targets: Vec<CheetahString>,
        failure: Option<&'static str>,
        calls: Vec<String>,
    }

    impl TopicDeletionBackend for Backend {
        async fn master_targets(&mut self, _: &CheetahString) -> Result<Vec<CheetahString>> {
            self.calls.push("targets".into());
            Ok(self.targets.clone())
        }

        async fn delete_broker(
            &mut self,
            _: &CheetahString,
            broker: &CheetahString,
        ) -> std::result::Result<(), TopicOperationFailure> {
            self.calls.push(broker.to_string());
            if self.failure == Some(broker.as_str()) {
                return Err(TopicOperationFailure {
                    broker_addr: broker.clone(),
                    error_code: "auth.permission.denied".into(),
                    error: "Permission was denied".into(),
                });
            }
            Ok(())
        }

        async fn delete_route(&mut self, _: &CheetahString, _: &CheetahString) -> Result<()> {
            self.calls.push("route".into());
            Ok(())
        }
    }

    #[tokio::test]
    async fn deletion_preserves_partial_outcomes_and_retains_the_route() {
        let mut backend = Backend {
            targets: vec!["b".into(), "a".into(), "c".into(), "a".into()],
            failure: Some("b"),
            calls: Vec::new(),
        };
        let request = DeleteTopicRequest::try_new(" Topic ", Some(" Cluster ".into())).unwrap();
        let result = execute_deletion(&mut backend, &request).await.unwrap();
        assert_eq!(backend.calls, ["targets", "a", "b", "c"]);
        assert_eq!(result.broker_addrs, [CheetahString::from("a"), "c".into()]);
        assert!(result.is_partial_failure());
        assert_eq!(
            result.ensure_complete().unwrap_err().descriptor(),
            &rocketmq_error::AUTH_PERMISSION_DENIED
        );
        assert!(!result.name_server_deleted);
    }

    #[tokio::test]
    async fn deletion_removes_route_only_after_all_distinct_masters_succeed() {
        let mut backend = Backend {
            targets: vec!["b".into(), "a".into()],
            failure: None,
            calls: Vec::new(),
        };
        let request = DeleteTopicRequest::try_new("Topic", Some("Cluster".into())).unwrap();
        let result = execute_deletion(&mut backend, &request).await.unwrap();
        assert_eq!(backend.calls, ["targets", "a", "b", "route"]);
        result.ensure_complete().unwrap();
    }

    #[tokio::test]
    async fn deletion_rejects_invalid_deserialized_input_and_empty_target_sets() {
        let mut backend = Backend {
            targets: Vec::new(),
            failure: None,
            calls: Vec::new(),
        };
        let request: DeleteTopicRequest =
            serde_json::from_str(r#"{"topic":"illegal topic","cluster_name":"cluster","namesrv_addr":null}"#).unwrap();
        assert!(execute_deletion(&mut backend, &request).await.is_err());
        assert!(backend.calls.is_empty());
        let request = DeleteTopicRequest::try_new("Topic", Some("Cluster".into())).unwrap();
        assert_eq!(
            execute_deletion(&mut backend, &request).await.unwrap_err().descriptor(),
            &rocketmq_error::ROUTE_CLUSTER_NOT_FOUND
        );
        assert_eq!(backend.calls, ["targets"]);
    }
}
