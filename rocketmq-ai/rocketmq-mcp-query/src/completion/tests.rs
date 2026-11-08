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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rmcp::model::ArgumentInfo;
use rmcp::model::CompletionContext;
use rocketmq_admin_core::core::broker::BrokerRuntimeTargetStatus;

use super::*;
use crate::adapter::admin_session::AdminSession;
use crate::adapter::admin_session::AdminSessionFactory;
use crate::adapter::admin_session::ResolvedCluster;
use crate::adapter::admin_session::SessionConsumerLag;
use crate::adapter::admin_session::SessionTopicRoute;
use crate::adapter::query_facade::QueryFacade;
use crate::config::AuditConfig;
use crate::config::ClusterConfig;
use crate::config::McpConfig;
use crate::config::SecurityConfig;
use crate::guard::audit::AuditStatus;
use crate::guard::context::Principal;
use crate::model::contract::QueryPayload;
use crate::tools::cluster_tools::BrokerSummary;
use crate::tools::consumer_tools::ConsumerGroupSummary;
use crate::tools::executor::ToolExecutionError;
use crate::tools::executor::ToolFailure;

/// More names than one inventory snapshot keeps.
const OVERSIZED: usize = 10_001;

const TOPIC_TEMPLATE: &str = "rocketmq://clusters/{cluster}/topics/{topic}";

/// Every listing Tool a completion can be answered from, on every cluster.
const EVERY_LISTING_TOOL: &str = "allowed_clusters = [\"*\"]\nallow_tools = [\"rocketmq_list_clusters\", \
                                  \"rocketmq_list_topics\", \"rocketmq_list_consumer_groups\", \
                                  \"rocketmq_list_brokers\"]";

#[derive(Debug, Default)]
struct Counters {
    starts: AtomicUsize,
    topic_inventory: AtomicUsize,
    consumer_group_inventory: AtomicUsize,
    broker_rows: AtomicUsize,
    /// Makes the Topic inventory unavailable.
    fail_topics: AtomicBool,
}

#[derive(Clone)]
struct Factory {
    counters: Arc<Counters>,
    topics: Arc<Vec<String>>,
}

impl AdminSessionFactory for Factory {
    type Session = Session;

    async fn start(&self, _cluster: ResolvedCluster) -> Result<Self::Session, ToolFailure> {
        self.counters.starts.fetch_add(1, Ordering::SeqCst);
        Ok(Session {
            counters: self.counters.clone(),
            topics: self.topics.clone(),
        })
    }
}

struct Session {
    counters: Arc<Counters>,
    topics: Arc<Vec<String>>,
}

impl AdminSession for Session {
    async fn broker_rows(&mut self) -> Result<QueryPayload<Vec<BrokerSummary>>, ToolFailure> {
        self.counters.broker_rows.fetch_add(1, Ordering::SeqCst);
        let row = |broker_name: &str, broker_id: u64| BrokerSummary {
            cluster: "DefaultCluster".to_string(),
            broker_name: broker_name.to_string(),
            broker_id,
            broker_addr: "10.11.12.13:10911".to_string(),
            version: "V5_3_0".to_string(),
            in_tps: "0".to_string(),
            out_tps: "0".to_string(),
            timer_progress: "0".to_string(),
            page_cache_lock_time_millis: "0".to_string(),
            hour: "0".to_string(),
            space: "0".to_string(),
            broker_active: true,
        };
        Ok(QueryPayload::complete(vec![
            row("broker-b", 0),
            row("broker-a", 0),
            row("broker-a", 1),
        ]))
    }

    async fn topic_inventory(&mut self) -> Result<Vec<String>, ToolFailure> {
        self.counters.topic_inventory.fetch_add(1, Ordering::SeqCst);
        if self.counters.fail_topics.load(Ordering::SeqCst) {
            return Err(ToolFailure::Operational(ToolExecutionError::SourceUnavailable(None)));
        }
        Ok(self.topics.as_ref().clone())
    }

    async fn topic_route(&mut self, _topic: &str) -> Result<SessionTopicRoute, ToolFailure> {
        Ok(SessionTopicRoute {
            brokers: Vec::new(),
            queues: Vec::new(),
        })
    }

    async fn consumer_groups(&mut self) -> Result<QueryPayload<Vec<ConsumerGroupSummary>>, ToolFailure> {
        Ok(QueryPayload::complete(Vec::new()))
    }

    async fn consumer_group_inventory(&mut self) -> Result<QueryPayload<Vec<String>>, ToolFailure> {
        self.counters.consumer_group_inventory.fetch_add(1, Ordering::SeqCst);
        Ok(QueryPayload::complete(vec![
            "payments".to_string(),
            "orders".to_string(),
        ]))
    }

    async fn consumer_lag(
        &mut self,
        _topic: &str,
        _consumer_group: &str,
    ) -> Result<QueryPayload<SessionConsumerLag>, ToolFailure> {
        Ok(QueryPayload::complete(SessionConsumerLag {
            queues: Vec::new(),
            total_lag: 0,
            consume_tps: 0.0,
            inflight_total: 0,
        }))
    }

    async fn probe_broker_runtime_target(
        &mut self,
        _broker_name: &str,
    ) -> Result<BrokerRuntimeTargetStatus, ToolFailure> {
        Ok(BrokerRuntimeTargetStatus::NotFound)
    }

    async fn shutdown(self) -> Result<(), ToolFailure> {
        Ok(())
    }
}

/// The example configuration, extended by a second cluster named `other`.
fn config() -> McpConfig {
    let mut config = McpConfig::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("conf")
            .join("mcp.example.toml"),
    )
    .unwrap();
    let mut other = config.clusters[0].clone();
    other.name = "other".to_string();
    other.default = None;
    config.clusters.push(other);
    config
}

fn facade(topics: Vec<String>) -> (QueryFacade<Factory>, Arc<Counters>) {
    let counters = Arc::new(Counters::default());
    let factory = Factory {
        counters: counters.clone(),
        topics: Arc::new(topics),
    };
    (QueryFacade::with_factory(config(), factory), counters)
}

fn numbered(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|index| format!("{prefix}{index:05}")).collect()
}

/// A Guard over the two clusters whose only role, `reader`, is defined by `role`.
fn guard_with_role(role: &str, rate_limit_per_minute: u32) -> (Guard, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let permissions = directory.path().join("permissions.toml");
    std::fs::write(&permissions, format!("[roles.reader]\n{role}\n")).unwrap();
    let clusters: Vec<ClusterConfig> = config().clusters;
    let guard = Guard::new(
        SecurityConfig {
            profile: "reader".to_string(),
            allow_change_planning: false,
            sanitize_output: true,
            rate_limit_per_minute,
            permissions_file: permissions.to_string_lossy().into_owned(),
            max_concurrent_requests_per_cluster: 1,
            pseudonym_key_env: None,
            http_rate_limit_per_minute: None,
        },
        AuditConfig {
            enabled: true,
            sink: "memory".to_string(),
            path: String::new(),
            queue_capacity: 64,
            max_record_bytes: 16 * 1024,
            queue_max_bytes: 1024 * 1024,
            max_file_bytes: 0,
        },
        &clusters,
    )
    .unwrap();
    (guard, directory)
}

fn caller() -> RequestContext {
    RequestContext {
        principal: Principal {
            id: "caller".to_string(),
            tenant: None,
            roles: BTreeSet::from(["reader".to_string()]),
            scopes: BTreeSet::from(["rocketmq:read".to_string()]),
            allowed_clusters: None,
        },
        client: None,
    }
}

fn with_cluster(request: CompleteRequestParams, cluster: Option<&str>) -> CompleteRequestParams {
    match cluster {
        Some(cluster) => request.with_context(CompletionContext::with_arguments(HashMap::from([(
            "cluster".to_string(),
            cluster.to_string(),
        )]))),
        None => request,
    }
}

fn prompt_request(prompt: &str, argument: &str, value: &str, cluster: Option<&str>) -> CompleteRequestParams {
    with_cluster(
        CompleteRequestParams::new(Reference::for_prompt(prompt), ArgumentInfo::new(argument, value)),
        cluster,
    )
}

fn template_request(template: &str, variable: &str, value: &str, cluster: Option<&str>) -> CompleteRequestParams {
    with_cluster(
        CompleteRequestParams::new(Reference::for_resource(template), ArgumentInfo::new(variable, value)),
        cluster,
    )
}

/// A Topic completion against the Topic Resource template.
fn topic_request(value: &str, cluster: Option<&str>) -> CompleteRequestParams {
    template_request(TOPIC_TEMPLATE, "topic", value, cluster)
}

#[tokio::test]
async fn completion_requires_the_backing_list_tool_permission() {
    let (facade, counters) = facade(numbered("orders-", 3));

    // The role lists Topics only: every other class has no candidates and starts no session.
    let topics_only = "allowed_clusters = [\"*\"]\nallow_tools = [\"rocketmq_list_topics\"]";
    let (guard, _permissions) = guard_with_role(topics_only, 60);
    let topics = complete(&facade, &guard, &caller(), &topic_request("", Some("local-dev"))).await;
    assert_eq!(topics.completion.values.len(), 3);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    for request in [
        prompt_request("diagnose_consumer_lag", "consumer_group", "", Some("local-dev")),
        prompt_request("diagnose_broker_health", "broker_name", "", Some("local-dev")),
        prompt_request("diagnose_broker_health", "cluster", "", None),
    ] {
        let denied = complete(&facade, &guard, &caller(), &request).await;
        assert_eq!(denied, CompleteResult::default(), "{request:?}");
    }
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);
    let denied = guard
        .audit_log()
        .records()
        .into_iter()
        .filter(|record| record.status == AuditStatus::Failure)
        .map(|record| record.tool)
        .collect::<Vec<_>>();
    assert_eq!(
        denied,
        ["completion:consumer_group", "completion:broker", "completion:cluster"]
    );

    // The role reaches one cluster: the other has no candidates of any class.
    let one_cluster = EVERY_LISTING_TOOL.replace("[\"*\"]", "[\"local-dev\"]");
    let (guard, _permissions) = guard_with_role(&one_cluster, 60);
    let clusters = complete(
        &facade,
        &guard,
        &caller(),
        &prompt_request("diagnose_consumer_lag", "cluster", "", None),
    )
    .await;
    assert_eq!(clusters.completion.values, ["local-dev"]);
    let before = counters.starts.load(Ordering::SeqCst);
    for request in [
        topic_request("", Some("other")),
        prompt_request("diagnose_consumer_lag", "consumer_group", "", Some("other")),
        prompt_request("diagnose_broker_health", "broker_name", "", Some("other")),
        topic_request("", Some("unconfigured")),
    ] {
        let denied = complete(&facade, &guard, &caller(), &request).await;
        assert_eq!(denied, CompleteResult::default(), "{request:?}");
    }
    assert_eq!(counters.starts.load(Ordering::SeqCst), before);

    // The scope of the listing Tool is required as well.
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);
    let mut unscoped = caller();
    unscoped.principal.scopes = BTreeSet::from(["rocketmq:diagnose".to_string()]);
    let denied = complete(&facade, &guard, &unscoped, &topic_request("", Some("local-dev"))).await;
    assert_eq!(denied, CompleteResult::default());
    assert_eq!(counters.starts.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn completion_without_a_cluster_returns_no_topic_candidates() {
    let (facade, counters) = facade(numbered("orders-", 3));
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);

    // `local-dev` is the default cluster, and it is still not assumed.
    for cluster in [None, Some(""), Some("   ")] {
        for request in [
            topic_request("ord", cluster),
            prompt_request("diagnose_consumer_lag", "consumer_group", "", cluster),
            prompt_request("diagnose_broker_health", "broker_name", "", cluster),
        ] {
            let result = complete(&facade, &guard, &caller(), &request).await;
            assert!(result.completion.values.is_empty(), "{request:?}");
            assert_eq!(result.completion.total, Some(0));
            assert_eq!(result.completion.has_more, Some(false));
        }
    }
    assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
    // The requests were admitted and recorded, without a cluster.
    let records = guard.audit_log().records();
    assert_eq!(records.len(), 9);
    assert!(records
        .iter()
        .all(|record| record.cluster.is_none() && record.status == AuditStatus::Success));

    // With the cluster chosen, the same request has candidates.
    let result = complete(&facade, &guard, &caller(), &topic_request("ord", Some("local-dev"))).await;
    assert_eq!(result.completion.values.len(), 3);
}

#[tokio::test]
async fn completion_is_prefix_filtered_and_bounded_to_one_hundred() {
    let mut topics = numbered("Orders-", 250);
    topics.extend(["payments".to_string(), "order".to_string(), "xorders-1".to_string()]);
    let (facade, _counters) = facade(topics);
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);
    let complete_topic = |value: &'static str| {
        let (facade, guard) = (&facade, &guard);
        async move {
            complete(facade, guard, &caller(), &topic_request(value, Some("local-dev")))
                .await
                .completion
        }
    };

    // The prefix is matched at the start of a name, ignoring ASCII case.
    let bounded = complete_topic("orders-").await;
    assert_eq!(bounded.values.len(), 100);
    assert_eq!(bounded.values[0], "Orders-00000");
    assert_eq!(bounded.values[99], "Orders-00099");
    assert_eq!(bounded.total, Some(250));
    assert_eq!(bounded.has_more, Some(true));

    let narrowed = complete_topic("ORDERS-0024").await;
    assert_eq!(narrowed.values.len(), 10);
    assert_eq!(narrowed.total, Some(10));
    assert_eq!(narrowed.has_more, Some(false));

    let exact = complete_topic("order").await;
    assert_eq!(exact.total, Some(251), "`order` and the 250 `Orders-` names");
    assert_eq!(exact.values[0], "Orders-00000", "names keep their sorted order");

    assert_eq!(
        complete_topic("rders").await.total,
        Some(0),
        "a prefix is not a substring"
    );
    assert_eq!(complete_topic("zzz").await.total, Some(0));
    // A value longer than any Topic name, or one with a control character, has no candidates.
    let too_long = Box::leak("o".repeat(identifier::TOPIC_MAX_BYTES + 1).into_boxed_str());
    assert_eq!(complete_topic(too_long).await.total, Some(0));
    assert_eq!(complete_topic("orders\n").await.total, Some(0));
}

#[tokio::test]
async fn completion_reuses_the_inventory_snapshot() {
    let (facade, counters) = facade(numbered("orders-", 3));
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);

    for value in ["", "o", "orders-00001"] {
        let result = complete(&facade, &guard, &caller(), &topic_request(value, Some("local-dev"))).await;
        assert!(!result.completion.values.is_empty(), "value={value}");
    }
    assert_eq!(counters.topic_inventory.load(Ordering::SeqCst), 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 1);

    // The Consumer Group inventory and the Broker list are kept the same way.
    for _ in 0..2 {
        let groups = complete(
            &facade,
            &guard,
            &caller(),
            &prompt_request("diagnose_consumer_lag", "consumer_group", "", Some("local-dev")),
        )
        .await;
        assert_eq!(groups.completion.values, ["orders", "payments"]);
        let brokers = complete(
            &facade,
            &guard,
            &caller(),
            &prompt_request("diagnose_broker_health", "broker_name", "broker-", Some("local-dev")),
        )
        .await;
        assert_eq!(brokers.completion.values, ["broker-a", "broker-b"]);
    }
    assert_eq!(counters.consumer_group_inventory.load(Ordering::SeqCst), 1);
    assert_eq!(counters.broker_rows.load(Ordering::SeqCst), 1);
    assert_eq!(counters.starts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn completion_is_audited_and_rate_limited() {
    let (facade, _counters) = facade(numbered("orders-", 3));
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 2);
    let secret_prefix = "orders-secret-prefix";

    for _ in 0..2 {
        let result = complete(&facade, &guard, &caller(), &topic_request("", Some("local-dev"))).await;
        assert_eq!(result.completion.values.len(), 3);
    }
    // The third request in the window is refused and looks like one without candidates.
    let limited = complete(
        &facade,
        &guard,
        &caller(),
        &topic_request(secret_prefix, Some("local-dev")),
    )
    .await;
    assert_eq!(limited, CompleteResult::default());

    let records = guard.audit_log().records();
    assert_eq!(records.len(), 3);
    for record in &records {
        assert_eq!(record.tool, "completion:topic");
        assert_eq!(record.cluster.as_deref(), Some("local-dev"));
        assert_eq!(record.operator, "caller");
    }
    assert_eq!(records[0].status, AuditStatus::Success);
    assert_eq!(records[2].status, AuditStatus::Failure);
    assert!(records[2].error.is_some());
    // Different values are told apart by their hash; the value itself is not recorded.
    assert_eq!(records[0].arguments_hash, records[1].arguments_hash);
    assert_ne!(records[0].arguments_hash, records[2].arguments_hash);
    assert!(!serde_json::to_string(&records).unwrap().contains(secret_prefix));

    // Completing does not spend the budget of the listing Tool itself, nor of another principal.
    let list_topics = ToolId::ListTopics.descriptor();
    let arguments = JsonObject::from_iter([("cluster".to_string(), Value::from("local-dev"))]);
    assert!(guard
        .begin_tool_call(&caller(), list_topics.name, list_topics.risk_level, &arguments)
        .is_ok());
    let mut other = caller();
    other.principal.id = "other-caller".to_string();
    let result = complete(&facade, &guard, &other, &topic_request("", Some("local-dev"))).await;
    assert_eq!(result.completion.values.len(), 3);
}

#[tokio::test]
async fn completion_offers_only_names_the_argument_accepts() {
    let (facade, _counters) = facade(vec![
        "orders".to_string(),
        "%RETRY%orders".to_string(),
        "orders|priority".to_string(),
        // None of these is a Topic name an argument accepts.
        "10.11.12.13:9876".to_string(),
        "orders password=hunter2".to_string(),
        "orders%2Fhidden".to_string(),
        " orders ".to_string(),
    ]);
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);

    let result = complete(&facade, &guard, &caller(), &topic_request("", Some("local-dev"))).await;
    assert_eq!(result.completion.values, ["%RETRY%orders", "orders", "orders|priority"]);
    assert_eq!(result.completion.total, Some(3));
}

#[tokio::test]
async fn completion_of_an_unavailable_source_is_empty_and_audited() {
    let (facade, counters) = facade(numbered("orders-", 3));
    counters.fail_topics.store(true, Ordering::SeqCst);
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);

    let result = complete(&facade, &guard, &caller(), &topic_request("", Some("local-dev"))).await;
    assert_eq!(result, CompleteResult::default());
    let records = guard.audit_log().records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tool, "completion:topic");
    assert_eq!(records[0].status, AuditStatus::Failure);
    assert!(records[0].error.is_some());
}

#[tokio::test]
async fn completion_of_an_unknown_reference_or_free_argument_is_empty() {
    let (facade, counters) = facade(numbered("orders-", 3));
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);
    for request in [
        prompt_request("unknown_prompt", "cluster", "", None),
        // A Prompt that does not declare the argument.
        prompt_request("broker_health_check", "topic", "", Some("local-dev")),
        // Arguments and variables that take free values.
        prompt_request("broker_health_check", "check_level", "", Some("local-dev")),
        prompt_request("diagnose_message_delivery", "message_id", "", Some("local-dev")),
        template_request(
            "rocketmq://clusters/{cluster}/topics{?filter,limit,cursor}",
            "filter",
            "",
            Some("local-dev"),
        ),
        // A template this server does not publish, and a concrete URI in place of a template.
        template_request(
            "rocketmq://clusters/{cluster}/secrets/{topic}",
            "topic",
            "",
            Some("local-dev"),
        ),
        template_request(
            "rocketmq://clusters/local-dev/topics/orders",
            "topic",
            "",
            Some("local-dev"),
        ),
        template_request(TOPIC_TEMPLATE, "group", "", Some("local-dev")),
    ] {
        let result = complete(&facade, &guard, &caller(), &request).await;
        assert_eq!(result, CompleteResult::default(), "{request:?}");
    }
    assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
    assert!(guard.audit_log().records().is_empty(), "nothing was read or refused");
}

#[test]
fn every_prompt_argument_and_template_variable_is_classified() {
    // A new argument or variable has to be added to one of these lists, which is the decision
    // whether it gets candidates.
    const FREE_PROMPT_ARGUMENTS: [&str; 2] = ["check_level", "message_id"];
    const FREE_TEMPLATE_VARIABLES: [&str; 3] = ["filter", "limit", "cursor"];

    let prompts = prompts::registry::prompt_templates().unwrap();
    assert_eq!(prompts.len(), 5);
    let mut completable = BTreeSet::new();
    for prompt in &prompts {
        for argument in &prompt.front_matter.arguments {
            let reference = Reference::for_prompt(prompt.front_matter.name.clone());
            match classify(&reference, &argument.name) {
                Some(class) => {
                    completable.insert((argument.name.clone(), class.backing_tool().descriptor().name));
                }
                None => assert!(
                    FREE_PROMPT_ARGUMENTS.contains(&argument.name.as_str()),
                    "{}.{} is not classified",
                    prompt.front_matter.name,
                    argument.name
                ),
            }
        }
    }
    assert_eq!(
        completable.into_iter().collect::<Vec<_>>(),
        [
            ("broker_name".to_string(), "rocketmq_list_brokers"),
            ("cluster".to_string(), "rocketmq_list_clusters"),
            ("consumer_group".to_string(), "rocketmq_list_consumer_groups"),
            ("topic".to_string(), "rocketmq_list_topics"),
        ]
    );

    let registry = resources::registry::ResourceRegistry::new().unwrap();
    let templates = registry
        .list_resource_templates(&config(), None, b"test", |_, _| true)
        .unwrap()
        .resource_templates;
    assert_eq!(templates.len(), 15);
    let mut completable = BTreeSet::new();
    for template in &templates {
        let variables = resources::registry::template_variables(&template.uri_template).unwrap();
        assert_eq!(variables[0], "cluster", "{}", template.uri_template);
        for variable in variables {
            let reference = Reference::for_resource(template.uri_template.clone());
            match classify(&reference, &variable) {
                Some(class) => {
                    completable.insert((variable, class.backing_tool().descriptor().name));
                }
                None => assert!(
                    FREE_TEMPLATE_VARIABLES.contains(&variable.as_str()),
                    "{} variable {variable} is not classified",
                    template.uri_template
                ),
            }
        }
    }
    assert_eq!(
        completable.into_iter().collect::<Vec<_>>(),
        [
            ("broker".to_string(), "rocketmq_list_brokers"),
            ("cluster".to_string(), "rocketmq_list_clusters"),
            ("group".to_string(), "rocketmq_list_consumer_groups"),
            ("topic".to_string(), "rocketmq_list_topics"),
        ]
    );
    assert_eq!(
        resources::registry::template_variables("rocketmq://clusters/{cluster}/consumer-groups/{group}/lag{?topic}")
            .unwrap(),
        ["cluster", "group", "topic"]
    );
}

#[tokio::test]
async fn completion_names_of_an_oversized_inventory_need_a_prefix() {
    let (facade, counters) = facade(numbered("Orders-", OVERSIZED));

    // The whole inventory cannot be kept, and nothing narrows it.
    let unfiltered = facade
        .completion_names("local-dev", CompletionClass::Topic, "")
        .await
        .unwrap();
    assert!(unfiltered.is_empty());

    // A prefix narrows the selection to names that contain it, which fits. The names themselves
    // were read once: narrowing them reads nothing again.
    let narrowed = facade
        .completion_names("local-dev", CompletionClass::Topic, "orders-0999")
        .await
        .unwrap();
    assert_eq!(narrowed.len(), 10);
    assert!(narrowed.iter().all(|name| name.starts_with("Orders-0999")));
    assert_eq!(counters.topic_inventory.load(Ordering::SeqCst), 1);

    // Through the handler, the narrowed read is what a caller typing a prefix gets.
    let (guard, _permissions) = guard_with_role(EVERY_LISTING_TOOL, 60);
    let result = complete(
        &facade,
        &guard,
        &caller(),
        &topic_request("orders-0999", Some("local-dev")),
    )
    .await;
    assert_eq!(result.completion.total, Some(10));
}

#[tokio::test]
async fn completion_names_reject_an_unconfigured_cluster_without_a_session() {
    let (facade, counters) = facade(numbered("orders-", 3));
    for class in [
        CompletionClass::Topic,
        CompletionClass::ConsumerGroup,
        CompletionClass::Broker,
    ] {
        let error = facade.completion_names("unconfigured", class, "").await.unwrap_err();
        assert_eq!(error.code(), "invalid_arguments");
    }
    assert!(facade
        .completion_names("local-dev", CompletionClass::Cluster, "")
        .await
        .is_err());
    assert_eq!(counters.starts.load(Ordering::SeqCst), 0);
}
