use std::collections::BTreeMap;

use anyhow::bail;
use anyhow::Context;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use serde::Serialize;

use crate::admin_facade::TuiAdminFacade;
use crate::state::CommandFormState;
use crate::view_model::CommandResultViewModel;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandCategory {
    Topic,
    NameServer,
    Auth,
    Broker,
    Cluster,
    Controller,
    Connection,
    Consumer,
    Offset,
    Queue,
    Ha,
    Stats,
    Producer,
    Lite,
    Message,
    Export,
    StaticTopic,
}

impl CommandCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "Topic",
            Self::NameServer => "NameServer",
            Self::Auth => "Auth",
            Self::Broker => "Broker",
            Self::Cluster => "Cluster",
            Self::Controller => "Controller",
            Self::Connection => "Connection",
            Self::Consumer => "Consumer",
            Self::Offset => "Offset",
            Self::Queue => "Queue",
            Self::Ha => "HA",
            Self::Stats => "Stats",
            Self::Producer => "Producer",
            Self::Lite => "Lite",
            Self::Message => "Message",
            Self::Export => "Export",
            Self::StaticTopic => "Static Topic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskLevel {
    Safe,
    Mutating,
    Dangerous,
}

impl RiskLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Mutating => "mutating",
            Self::Dangerous => "dangerous",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultViewKind {
    Table,
    KeyValue,
    Json,
    Text,
    OperationSummary,
}

impl ResultViewKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::KeyValue => "key-value",
            Self::Json => "json",
            Self::Text => "text",
            Self::OperationSummary => "operation-summary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandExecutor {
    Facade,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgKind {
    String {
        placeholder: &'static str,
    },
    OptionalString {
        placeholder: &'static str,
    },
    Number {
        default: Option<i64>,
        min: Option<i64>,
    },
    Bool {
        default: bool,
    },
    Enum {
        values: &'static [&'static str],
        default: &'static str,
    },
    KeyValueMap,
    TimestampMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgSpec {
    pub name: &'static str,
    pub label: &'static str,
    pub help: &'static str,
    pub required: bool,
    pub kind: ArgKind,
}

impl ArgSpec {
    pub fn default_value(&self) -> String {
        match &self.kind {
            ArgKind::String { .. } | ArgKind::OptionalString { .. } | ArgKind::KeyValueMap => String::new(),
            ArgKind::Number { default, .. } => default.map(|value| value.to_string()).unwrap_or_default(),
            ArgKind::Bool { default } => default.to_string(),
            ArgKind::Enum { default, .. } => (*default).to_string(),
            ArgKind::TimestampMillis => String::new(),
        }
    }

    pub fn placeholder(&self) -> &'static str {
        match &self.kind {
            ArgKind::String { placeholder } | ArgKind::OptionalString { placeholder } => placeholder,
            ArgKind::Number { .. } => "number",
            ArgKind::Bool { .. } => "true/false",
            ArgKind::Enum { values, .. } => values.first().copied().unwrap_or("value"),
            ArgKind::KeyValueMap => "key=value, one per line",
            ArgKind::TimestampMillis => "timestamp millis",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub id: &'static str,
    pub category: CommandCategory,
    pub title: &'static str,
    pub description: &'static str,
    pub risk_level: RiskLevel,
    pub args: Vec<ArgSpec>,
    pub executor: CommandExecutor,
    pub result_view_kind: ResultViewKind,
    pub confirmation_field: Option<&'static str>,
}

impl CommandSpec {
    pub fn matches_query(&self, query: &str) -> bool {
        let query = query.trim().to_ascii_lowercase();
        if query.is_empty() {
            return true;
        }
        self.id.to_ascii_lowercase().contains(&query)
            || self.title.to_ascii_lowercase().contains(&query)
            || self.description.to_ascii_lowercase().contains(&query)
            || self.category.as_str().to_ascii_lowercase().contains(&query)
    }

    pub fn expected_confirmation(&self, form: &CommandFormState) -> Option<String> {
        match self.risk_level {
            RiskLevel::Safe => None,
            RiskLevel::Mutating => Some("confirm".to_string()),
            RiskLevel::Dangerous => self
                .confirmation_field
                .and_then(|field| form.raw_value(field))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .or_else(|| Some("confirm".to_string())),
        }
    }
}

pub fn command_catalog() -> Vec<CommandSpec> {
    let mut commands = Vec::new();
    topic_commands(&mut commands);
    nameserver_commands(&mut commands);
    auth_commands(&mut commands);
    broker_commands(&mut commands);
    cluster_commands(&mut commands);
    controller_commands(&mut commands);
    connection_commands(&mut commands);
    consumer_commands(&mut commands);
    offset_commands(&mut commands);
    queue_commands(&mut commands);
    ha_commands(&mut commands);
    stats_commands(&mut commands);
    producer_commands(&mut commands);
    lite_commands(&mut commands);
    message_commands(&mut commands);
    export_commands(&mut commands);
    static_topic_commands(&mut commands);
    commands
}

pub async fn execute_command_with_progress<F>(
    facade: &TuiAdminFacade,
    spec: &CommandSpec,
    form: &CommandFormState,
    mut progress: F,
) -> anyhow::Result<CommandResultViewModel>
where
    F: FnMut(String),
{
    let result = match spec.id {
        "topic.list" => {
            let result = facade.query_topic_list(form.optional_string("cluster_name")).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "topic.cluster" => {
            let result = facade.query_topic_clusters(form.required_string("topic")?).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "topic.route" => {
            let result = facade.query_topic_route(form.required_string("topic")?).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "topic.status" => {
            let result = facade
                .query_topic_status(form.required_string("topic")?, form.optional_string("cluster_name"))
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "topic.update" => {
            let request = facade.update_topic_request(
                form.required_string("topic")?,
                topic_target(form)?,
                form.number_u32("read_queue_nums")?,
                form.number_u32("write_queue_nums")?,
                form.optional_u32("perm")?,
                Some(form.bool_value("order")?),
                Some(form.bool_value("unit")?),
                Some(form.bool_value("has_unit_sub")?),
            )?;
            let result = facade.create_or_update_topic(request).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "topic.update_perm" => {
            let request = facade.update_topic_perm_request(
                form.required_string("topic")?,
                topic_target(form)?,
                form.number_i32("perm")?,
            )?;
            let result = facade.update_topic_perm(request).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "topic.delete" => {
            let topic = form.required_string("topic")?;
            let result = facade
                .delete_topic(topic.clone(), form.optional_string("cluster_name"))
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![topic]).with_debug_tail(&result)
        }
        "topic.order_conf" => {
            let result = facade
                .apply_order_conf(
                    form.required_string("topic")?,
                    form.enum_string("method")?,
                    form.optional_string("order_conf"),
                )
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "topic.allocate_mq" => {
            let result = facade
                .query_allocated_mq(form.required_string("topic")?, form.required_string("ip_list")?)
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "namesrv.config.query" => {
            let result = facade.query_namesrv_config().await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "namesrv.config.update" => {
            let result = facade
                .update_namesrv_config(form.required_string("key")?, form.required_string("value")?)
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "namesrv.kv.update" => {
            let result = facade
                .update_kv_config(
                    form.required_string("namespace")?,
                    form.required_string("key")?,
                    form.required_string("value")?,
                )
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "namesrv.kv.delete" => {
            let key = form.required_string("key")?;
            let result = facade
                .delete_kv_config(form.required_string("namespace")?, key.clone())
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![key]).with_debug_tail(&result)
        }
        "namesrv.write_perm.add" => {
            let result = facade.add_write_perm(form.required_string("broker_name")?).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "namesrv.write_perm.wipe" => {
            let result = facade.wipe_write_perm(form.required_string("broker_name")?).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "auth.user.get" => {
            let result = facade
                .query_auth_user(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("username")?,
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "auth.user.list" => {
            let result = facade
                .list_auth_users(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.optional_string("filter"),
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "auth.user.create" => {
            let result = facade
                .create_auth_user(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("username")?,
                    form.required_string("password")?,
                    form.optional_string("user_type"),
                )
                .await?;
            CommandResultViewModel::auth_operation(spec.title, &result)
        }
        "auth.user.update" => {
            let result = facade
                .update_auth_user(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("username")?,
                    form.optional_string("password"),
                    form.optional_string("user_type"),
                    form.optional_string("user_status"),
                )
                .await?;
            CommandResultViewModel::auth_operation(spec.title, &result)
        }
        "auth.user.delete" => {
            let username = form.required_string("username")?;
            let result = facade
                .delete_auth_user(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    username.clone(),
                )
                .await?;
            CommandResultViewModel::auth_operation(spec.title, &result).with_debug_tail(&username)
        }
        "auth.user.copy" => {
            let result = facade
                .copy_auth_users(
                    form.required_string("from_broker")?,
                    form.required_string("to_broker")?,
                    form.optional_string("usernames"),
                )
                .await?;
            CommandResultViewModel::auth_copy_users(spec.title, &result)
        }
        "auth.acl.get" => {
            let result = facade
                .query_auth_acl(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("subject")?,
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "auth.acl.list" => {
            let result = facade
                .list_auth_acl(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.optional_string("subject_filter"),
                    form.optional_string("resource_filter"),
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "auth.acl.create" => {
            let result = facade
                .create_auth_acl(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("subject")?,
                    form.required_string("resources")?,
                    form.required_string("actions")?,
                    form.enum_string("decision")?,
                    form.optional_string("source_ip"),
                )
                .await?;
            CommandResultViewModel::auth_operation(spec.title, &result)
        }
        "auth.acl.update" => {
            let result = facade
                .update_auth_acl(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("subject")?,
                    form.required_string("resources")?,
                    form.required_string("actions")?,
                    form.enum_string("decision")?,
                    form.optional_string("source_ip"),
                )
                .await?;
            CommandResultViewModel::auth_operation(spec.title, &result)
        }
        "auth.acl.delete" => {
            let subject = form.required_string("subject")?;
            let result = facade
                .delete_auth_acl(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    subject.clone(),
                    form.optional_string("resource"),
                )
                .await?;
            CommandResultViewModel::auth_operation(spec.title, &result).with_debug_tail(&subject)
        }
        "auth.acl.copy" => {
            let result = facade
                .copy_auth_acl(
                    form.required_string("from_broker")?,
                    form.required_string("to_broker")?,
                    form.optional_string("subjects"),
                )
                .await?;
            CommandResultViewModel::auth_copy_acl(spec.title, &result)
        }
        "broker.config.query" => {
            let result = facade
                .query_broker_config(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.optional_string("key_pattern"),
                )
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "broker.config.update_plan" => {
            let request = broker_config_update_request(facade, form)?;
            let result = facade.build_broker_config_update_plan(request).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "broker.config.update_apply" => {
            let request = broker_config_update_request(facade, form)?;
            let result = facade.apply_broker_config_update(request).await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "broker.runtime_stats" => {
            let result = facade
                .query_broker_runtime_stats(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "broker.consume_stats" => {
            let result = facade
                .query_broker_consume_stats(
                    form.required_string("broker_addr")?,
                    form.number_u64("timeout_millis")?,
                    form.number_i64("diff_level")?,
                    form.bool_value("is_order")?,
                )
                .await?;
            CommandResultViewModel::broker_consume_stats(spec.title, &result)
        }
        "broker.epoch" => {
            let result = facade
                .query_broker_epoch(
                    form.optional_string("broker_name"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "broker.cold_data_flow_ctr_info" => {
            let result = facade
                .query_cold_data_flow_ctr_info(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "broker.clean_expired_cq" => {
            let result = facade
                .clean_expired_consume_queue(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.optional_string("topic"),
                    form.bool_value("dry_run")?,
                )
                .await?;
            CommandResultViewModel::clean_expired_consume_queue(spec.title, &result)
        }
        "broker.delete_expired_commit_log" => {
            let target = operation_target_label(
                form.optional_string("broker_addr"),
                form.optional_string("cluster_name"),
                "all brokers",
            );
            let result = facade
                .delete_expired_commit_log(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::broker_boolean_operation(spec.title, target, &result)
        }
        "broker.clean_unused_topic" => {
            let target = operation_target_label(
                form.optional_string("broker_addr"),
                form.optional_string("cluster_name"),
                "all brokers",
            );
            let result = facade
                .clean_unused_topic(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::broker_boolean_operation(spec.title, target, &result)
        }
        "broker.reset_master_flush_offset" => {
            let broker_addr = form.required_string("broker_addr")?;
            facade
                .reset_master_flush_offset(Some(broker_addr.clone()), Some(form.number_i64("offset")?))
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![broker_addr])
        }
        "broker.cold_data_flow_ctr_update" => {
            let result = facade
                .update_cold_data_flow_ctr_group_config(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("consumer_group")?,
                    form.required_string("threshold")?,
                )
                .await?;
            CommandResultViewModel::broker_operation(spec.title, &result)
        }
        "broker.cold_data_flow_ctr_remove" => {
            let result = facade
                .remove_cold_data_flow_ctr_group_config(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("consumer_group")?,
                )
                .await?;
            CommandResultViewModel::broker_operation(spec.title, &result)
        }
        "broker.commit_log_read_ahead" => {
            let result = facade
                .set_commit_log_read_ahead(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.optional_string("mode"),
                    form.bool_value("enable")?,
                    form.bool_value("disable")?,
                    form.optional_string("read_ahead_size"),
                    form.optional_string("read_ahead_size_key"),
                    form.bool_value("show_only")?,
                )
                .await?;
            CommandResultViewModel::commit_log_read_ahead(spec.title, &result)
        }
        "broker.switch_timer_engine" => {
            let result = facade
                .switch_timer_engine(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.enum_string("engine_type")?,
                )
                .await?;
            CommandResultViewModel::broker_operation(spec.title, &result)
        }
        "cluster.list" => {
            let result = facade
                .query_cluster_list(form.bool_value("more_stats")?, form.optional_string("cluster_name"))
                .await?;
            CommandResultViewModel::cluster_list(spec.title, &result)
        }
        "cluster.broker_names" => {
            let result = facade
                .query_cluster_broker_names(form.optional_string("cluster_name"))
                .await?;
            CommandResultViewModel::key_value_sorted(
                spec.title,
                result
                    .broker_names_by_cluster
                    .iter()
                    .map(|(cluster, brokers)| (cluster.clone(), brokers.join(", ")))
                    .collect(),
            )
        }
        "cluster.send_message_rt" => {
            let result = facade
                .check_cluster_send_message_rt(
                    form.number_u64("amount")?,
                    form.number_u64("size")?,
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::cluster_send_message_rt(spec.title, &result)
        }
        "controller.config.query" => {
            let result = facade
                .query_controller_config(form.required_string("controller_address")?)
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "controller.config.update" => {
            let controller_address = form.required_string("controller_address")?;
            facade
                .update_controller_config(
                    controller_address.clone(),
                    form.required_string("key")?,
                    form.required_string("value")?,
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![controller_address])
        }
        "controller.metadata.query" => {
            let result = facade
                .query_controller_metadata(form.required_string("controller_address")?)
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "controller.metadata.clean" => {
            let broker_name = form.required_string("broker_name")?;
            facade
                .clean_controller_metadata(
                    form.required_string("controller_address")?,
                    broker_name.clone(),
                    form.optional_string("broker_controller_ids"),
                    form.optional_string("cluster_name"),
                    form.bool_value("clean_living_broker")?,
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![broker_name])
        }
        "connection.consumer" => {
            let result = facade
                .query_consumer_connection(
                    form.required_string("consumer_group")?,
                    form.optional_string("broker_addr"),
                )
                .await?;
            CommandResultViewModel::consumer_connection(spec.title, &result)
        }
        "connection.producer" => {
            let result = facade
                .query_producer_connection(form.required_string("producer_group")?, form.required_string("topic")?)
                .await?;
            CommandResultViewModel::producer_connection(spec.title, &result)
        }
        "consumer.config" => {
            let result = facade
                .query_consumer_config(form.required_string("group_name")?)
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "consumer.running_info" => {
            let result = facade
                .query_consumer_running_info(
                    form.required_string("group_name")?,
                    form.optional_string("client_id"),
                    form.optional_string("broker_addr"),
                    form.bool_value("jstack")?,
                )
                .await?;
            CommandResultViewModel::consumer_running_info(spec.title, &result)
        }
        "consumer.progress" => {
            let result = facade
                .query_consumer_progress(
                    form.optional_string("consumer_group"),
                    form.optional_string("topic_name"),
                    form.bool_value("show_client_ip")?,
                    form.optional_string("cluster"),
                )
                .await?;
            CommandResultViewModel::consumer_progress(spec.title, &result)
        }
        "consumer.delete_subscription_group" => {
            let group = form.required_string("group_name")?;
            let result = facade
                .delete_subscription_group(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    group.clone(),
                    form.bool_value("remove_offset")?,
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![group]).with_debug_tail(&result)
        }
        "consumer.set_consume_mode" => {
            let result = facade
                .set_consume_mode(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("topic_name")?,
                    form.required_string("group_name")?,
                    message_request_mode(form.enum_string("mode")?)?,
                    form.optional_i32("pop_share_queue_num")?,
                )
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "consumer.update_subscription_group" => {
            let result = facade
                .update_subscription_group(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("group_name")?,
                    form.bool_value("consume_enable")?,
                    form.bool_value("consume_from_min_enable")?,
                    form.bool_value("consume_broadcast_enable")?,
                    form.bool_value("consume_message_orderly")?,
                    form.number_i32("retry_queue_nums")?,
                    form.number_i32("retry_max_times")?,
                    form.number_u64("broker_id")?,
                    form.number_u64("which_broker_when_consume_slowly")?,
                    form.bool_value("notify_consumer_ids_changed_enable")?,
                    form.number_i32("group_sys_flag")?,
                    form.number_i32("consume_timeout_minute")?,
                )
                .await?;
            CommandResultViewModel::consumer_operation(spec.title, &result)
        }
        "consumer.update_subscription_group_list" => {
            let result = facade
                .update_subscription_group_list(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                    form.required_string("group_names")?,
                    form.bool_value("consume_enable")?,
                    form.bool_value("consume_from_min_enable")?,
                    form.bool_value("consume_broadcast_enable")?,
                    form.bool_value("consume_message_orderly")?,
                    form.number_i32("retry_queue_nums")?,
                    form.number_i32("retry_max_times")?,
                    form.number_u64("broker_id")?,
                    form.number_u64("which_broker_when_consume_slowly")?,
                    form.bool_value("notify_consumer_ids_changed_enable")?,
                    form.number_i32("group_sys_flag")?,
                    form.number_i32("consume_timeout_minute")?,
                )
                .await?;
            CommandResultViewModel::consumer_operation(spec.title, &result)
        }
        "offset.clone_group" => {
            facade
                .clone_group_offset(
                    form.required_string("src_group")?,
                    form.required_string("dest_group")?,
                    form.required_string("topic")?,
                    form.bool_value("offline")?,
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![form.required_string("dest_group")?])
        }
        "offset.consumer_status" => {
            let result = facade
                .query_consumer_status(
                    form.required_string("group")?,
                    form.required_string("topic")?,
                    form.optional_string("origin_client_id"),
                )
                .await?;
            CommandResultViewModel::Table(crate::view_model::TableViewModel {
                title: spec.title.to_string(),
                headers: vec![
                    "Client ID".to_string(),
                    "Broker Name".to_string(),
                    "Queue ID".to_string(),
                    "Offset".to_string(),
                ],
                rows: result
                    .rows
                    .iter()
                    .map(|row| {
                        vec![
                            row.client_id.to_string(),
                            row.broker_name.to_string(),
                            row.queue_id.to_string(),
                            row.offset.to_string(),
                        ]
                    })
                    .collect(),
            })
        }
        "offset.skip_accumulated" => {
            let group = form.required_string("group")?;
            let result = facade
                .skip_accumulated_message(
                    group.clone(),
                    form.required_string("topic")?,
                    form.optional_string("cluster"),
                    Some(form.bool_value("force")?),
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![group]).with_debug_tail(&result)
        }
        "offset.reset_by_time" => {
            let group = form.required_string("group")?;
            let result = facade
                .reset_offset_by_time(
                    group.clone(),
                    form.required_string("topic")?,
                    form.timestamp_millis("timestamp")?,
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![group]).with_debug_tail(&result)
        }
        "offset.reset_by_time_old" => {
            let group = form.required_string("group")?;
            let result = facade
                .reset_offset_by_time_old(
                    group.clone(),
                    form.required_string("topic")?,
                    form.timestamp_millis("timestamp")?,
                    Some(form.bool_value("force")?),
                    form.optional_string("cluster"),
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![group]).with_debug_tail(&result)
        }
        "queue.consume_queue" => {
            let result = facade
                .query_consume_queue(
                    form.required_string("topic")?,
                    form.number_i32("queue_id")?,
                    form.number_u64("index")?,
                    form.number_i32("count")?,
                    form.optional_string("broker_addr"),
                    form.optional_string("consumer_group"),
                )
                .await?;
            CommandResultViewModel::consume_queue(spec.title, &result)
        }
        "queue.rocksdb_cq_progress" => {
            let result = facade
                .check_rocksdb_cq_write_progress(
                    form.required_string("cluster_name")?,
                    form.optional_string("topic"),
                    form.optional_i64("check_from")?,
                )
                .await?;
            CommandResultViewModel::rocksdb_cq_progress(spec.title, &result)
        }
        "ha.status" => {
            let result = facade
                .query_ha_status(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::from_debug(spec.title, &result)
        }
        "ha.sync_state_set" => {
            let result = facade
                .query_sync_state_set(
                    form.required_string("controller_address")?,
                    form.optional_string("broker_name"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "stats.all" => {
            let result = facade
                .query_stats_all(form.bool_value("active_topic")?, form.optional_string("topic"))
                .await?;
            CommandResultViewModel::stats_all(spec.title, &result)
        }
        "producer.info" => {
            let result = facade.query_producer_info(form.required_string("broker_addr")?).await?;
            CommandResultViewModel::producer_info(spec.title, &result)
        }
        "producer.send_message" => {
            let result = facade
                .send_message(
                    form.required_string("topic")?,
                    form.required_string("body")?,
                    form.optional_string("keys"),
                    form.optional_string("tags"),
                    form.optional_string("broker_name"),
                    form.optional_i32("queue_id")?,
                    form.bool_value("msg_trace_enable")?,
                )
                .await?;
            CommandResultViewModel::send_message(spec.title, &result)
        }
        "producer.send_message_status" => {
            let result = facade
                .send_message_status(
                    form.required_string("broker_name")?,
                    usize::try_from(form.number_u64("message_size")?)?,
                    form.number_u32("count")?,
                )
                .await?;
            CommandResultViewModel::send_message_status(spec.title, &result)
        }
        "producer.check_message_send_rt" => {
            let result = facade
                .check_message_send_rt(
                    form.required_string("topic")?,
                    form.number_u64("amount")?,
                    usize::try_from(form.number_u64("size")?)?,
                )
                .await?;
            CommandResultViewModel::check_message_send_rt(spec.title, &result)
        }
        "lite.broker_info" => {
            let result = facade
                .query_broker_lite_info(
                    form.optional_string("broker_addr"),
                    form.optional_string("cluster_name"),
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "lite.parent_topic_info" => {
            let result = facade
                .query_parent_topic_info(form.required_string("parent_topic")?)
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "lite.topic_info" => {
            let result = facade
                .query_lite_topic_info(
                    form.required_string("parent_topic")?,
                    form.required_string("lite_topic")?,
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "lite.group_info" => {
            let result = facade
                .query_lite_group_info(
                    form.required_string("parent_topic")?,
                    form.required_string("group")?,
                    form.optional_string("lite_topic"),
                    form.optional_i32("top_k")?,
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "lite.client_info" => {
            let result = facade
                .query_lite_client_info(
                    form.required_string("parent_topic")?,
                    form.required_string("group")?,
                    form.required_string("client_id")?,
                )
                .await?;
            CommandResultViewModel::from_serializable(spec.title, &result)
        }
        "lite.trigger_dispatch" => {
            let result = facade
                .trigger_lite_dispatch(
                    form.required_string("parent_topic")?,
                    form.required_string("group")?,
                    form.optional_string("client_id"),
                    form.optional_string("broker_name"),
                )
                .await?;
            CommandResultViewModel::lite_trigger_dispatch(spec.title, &result)
        }
        "message.decode_id" => {
            let result = facade.decode_message_id(form.required_string("message_ids")?)?;
            CommandResultViewModel::decoded_message_ids(spec.title, &result)
        }
        "message.query_by_id" => {
            let result = facade
                .query_message_by_id(
                    form.required_string("message_ids")?,
                    form.optional_string("topic"),
                    form.number_u64("timeout_millis")?,
                )
                .await?;
            CommandResultViewModel::message_query_by_id(spec.title, &result)
        }
        "message.query_by_key" => {
            let result = facade
                .query_message_by_key(
                    form.required_string("topic")?,
                    form.required_string("msg_key")?,
                    form.optional_i64("begin_timestamp")?,
                    form.optional_i64("end_timestamp")?,
                    form.number_i32("max_num")?,
                    form.optional_string("cluster"),
                    Some(form.enum_string("key_type")?),
                    form.optional_string("last_key"),
                )
                .await?;
            CommandResultViewModel::message_query_by_key(spec.title, &result)
        }
        "message.query_by_unique_key" => {
            let result = facade
                .query_message_by_unique_key(
                    form.required_string("msg_id")?,
                    form.optional_string("consumer_group"),
                    form.optional_string("client_id"),
                    form.required_string("topic")?,
                    form.bool_value("show_all")?,
                    form.optional_string("cluster"),
                    form.optional_i64("start_time")?,
                    form.optional_i64("end_time")?,
                )
                .await?;
            CommandResultViewModel::message_query_by_unique_key(spec.title, &result)
        }
        "message.query_by_offset" => {
            let result = facade
                .query_message_by_offset(
                    form.required_string("topic")?,
                    form.required_string("broker_name")?,
                    form.number_i32("queue_id")?,
                    form.number_i64("offset")?,
                    form.optional_string("route_topic"),
                )
                .await?;
            CommandResultViewModel::message_query_by_offset(spec.title, &result)
        }
        "message.query_trace_by_id" => {
            let result = facade
                .query_message_trace_by_id(
                    form.required_string("msg_id")?,
                    form.optional_string("trace_topic"),
                    form.optional_i64("begin_timestamp")?,
                    form.optional_i64("end_timestamp")?,
                    form.number_i32("max_num")?,
                )
                .await?;
            CommandResultViewModel::message_trace(spec.title, &result)
        }
        "message.dump_compaction_log" => {
            let result = facade.dump_compaction_log(Some(form.required_string("file")?))?;
            CommandResultViewModel::dump_compaction_log(spec.title, &result)
        }
        "message.print" => {
            let result = facade
                .print_messages_with_progress(
                    form.required_string("topic")?,
                    form.required_string("sub_expression")?,
                    optional_u64_arg(form, "begin_timestamp")?,
                    optional_u64_arg(form, "end_timestamp")?,
                    form.optional_string("lmq_parent_topic"),
                    number_usize_arg(form, "max_events")?,
                    &mut progress,
                )
                .await?;
            CommandResultViewModel::message_pull_events(spec.title, &result)
        }
        "message.print_by_queue" => {
            let result = facade
                .print_messages_by_queue_with_progress(
                    form.required_string("topic")?,
                    form.required_string("broker_name")?,
                    form.number_i32("queue_id")?,
                    form.required_string("sub_expression")?,
                    optional_u64_arg(form, "begin_timestamp")?,
                    optional_u64_arg(form, "end_timestamp")?,
                    form.bool_value("print_messages")?,
                    form.bool_value("calculate_by_tag")?,
                    number_usize_arg(form, "max_events")?,
                    &mut progress,
                )
                .await?;
            CommandResultViewModel::message_pull_events(spec.title, &result)
        }
        "message.consume" => {
            let result = facade
                .consume_messages_with_progress(
                    form.required_string("topic")?,
                    form.optional_string("broker_name"),
                    form.optional_i32("queue_id")?,
                    form.optional_i64("offset")?,
                    form.optional_string("consumer_group"),
                    form.optional_i64("begin_timestamp")?,
                    form.optional_i64("end_timestamp")?,
                    form.number_i64("message_number")?,
                    number_usize_arg(form, "max_events")?,
                    &mut progress,
                )
                .await?;
            CommandResultViewModel::message_pull_events(spec.title, &result)
        }
        "export.configs" => {
            let result = facade.export_configs(form.required_string("cluster_name")?).await?;
            export_output_or_view(
                facade,
                spec.title,
                form,
                &result,
                CommandResultViewModel::export_configs(spec.title, &result),
            )?
        }
        "export.metadata" => {
            let result = facade
                .export_metadata(
                    form.optional_string("cluster_name"),
                    form.optional_string("broker_addr"),
                    form.bool_value("topic_only")?,
                    form.bool_value("subscription_group_only")?,
                    form.bool_value("special_topic")?,
                )
                .await?;
            export_output_or_view(
                facade,
                spec.title,
                form,
                &result,
                CommandResultViewModel::export_metadata(spec.title, &result),
            )?
        }
        "export.metadata_rocksdb" => {
            let result = facade.export_metadata_rocksdb(
                form.required_string("path")?,
                form.enum_string("config_type")?,
                form.bool_value("json_enable")?,
            )?;
            export_output_or_view(
                facade,
                spec.title,
                form,
                &result,
                CommandResultViewModel::export_metadata_rocksdb(spec.title, &result),
            )?
        }
        "export.pop_record" => {
            let result = facade
                .export_pop_records(
                    form.optional_string("cluster_name"),
                    form.optional_string("broker_addr"),
                    form.bool_value("dry_run")?,
                    optional_u64_arg(form, "timeout_millis")?,
                )
                .await?;
            export_output_or_view(
                facade,
                spec.title,
                form,
                &result,
                CommandResultViewModel::export_pop_records(spec.title, &result),
            )?
        }
        "static_topic.update" => {
            let topic = form.required_string("topic")?;
            let result = facade
                .update_static_topic(
                    topic.clone(),
                    form.required_string("broker_names")?,
                    form.required_string("queue_num")?,
                    form.optional_string("cluster_names"),
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![topic]).with_debug_tail(&result)
        }
        "static_topic.remap" => {
            let topic = form.required_string("topic")?;
            let result = facade
                .remapping_static_topic(
                    topic.clone(),
                    form.optional_string("broker_names"),
                    form.optional_string("cluster_names"),
                    Some(form.bool_value("force_replace")?),
                )
                .await?;
            CommandResultViewModel::operation_success(spec.title, vec![topic]).with_debug_tail(&result)
        }
        unknown => bail!("unknown command id: {unknown}"),
    };

    Ok(result)
}

fn topic_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "topic.list",
            CommandCategory::Topic,
            "List Topics",
            "List topics, optionally scoped to a cluster.",
            RiskLevel::Safe,
            vec![optional_string(
                "cluster_name",
                "Cluster",
                "Optional cluster name.",
                "DefaultCluster",
            )],
            ResultViewKind::Text,
            None,
        ),
        spec(
            "topic.cluster",
            CommandCategory::Topic,
            "Topic Clusters",
            "Show clusters that contain a topic.",
            RiskLevel::Safe,
            vec![required_string("topic", "Topic", "Topic name.", "TopicA")],
            ResultViewKind::Text,
            None,
        ),
        spec(
            "topic.route",
            CommandCategory::Topic,
            "Topic Route",
            "Show route data for a topic.",
            RiskLevel::Safe,
            vec![required_string("topic", "Topic", "Topic name.", "TopicA")],
            ResultViewKind::Text,
            None,
        ),
        spec(
            "topic.status",
            CommandCategory::Topic,
            "Topic Status",
            "Show offsets and runtime status for a topic.",
            RiskLevel::Safe,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                optional_string("cluster_name", "Cluster", "Optional cluster name.", "DefaultCluster"),
            ],
            ResultViewKind::Text,
            None,
        ),
        spec(
            "topic.update",
            CommandCategory::Topic,
            "Create or Update Topic",
            "Create or update topic configuration on a broker or cluster.",
            RiskLevel::Mutating,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                enum_arg(
                    "target_type",
                    "Target Type",
                    "Whether target is a broker or cluster.",
                    &["broker", "cluster"],
                    "broker",
                ),
                required_string("target", "Target", "Broker address or cluster name.", "127.0.0.1:10911"),
                number(
                    "read_queue_nums",
                    "Read Queues",
                    "Read queue count.",
                    true,
                    Some(8),
                    Some(1),
                ),
                number(
                    "write_queue_nums",
                    "Write Queues",
                    "Write queue count.",
                    true,
                    Some(8),
                    Some(1),
                ),
                number(
                    "perm",
                    "Perm",
                    "Permission: 2 write, 4 read, 6 read/write.",
                    false,
                    Some(6),
                    Some(0),
                ),
                bool_arg("order", "Order", "Enable ordered topic.", false),
                bool_arg("unit", "Unit", "Enable unit topic flag.", false),
                bool_arg("has_unit_sub", "Has Unit Sub", "Enable hasUnitSub flag.", false),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "topic.update_perm",
            CommandCategory::Topic,
            "Update Topic Permission",
            "Update topic permission on a broker or cluster.",
            RiskLevel::Mutating,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                enum_arg(
                    "target_type",
                    "Target Type",
                    "Whether target is a broker or cluster.",
                    &["broker", "cluster"],
                    "broker",
                ),
                required_string("target", "Target", "Broker address or cluster name.", "127.0.0.1:10911"),
                number(
                    "perm",
                    "Perm",
                    "Permission: 2 write, 4 read, 6 read/write.",
                    true,
                    Some(6),
                    Some(0),
                ),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "topic.delete",
            CommandCategory::Topic,
            "Delete Topic",
            "Delete a topic from a cluster.",
            RiskLevel::Dangerous,
            vec![
                required_string("topic", "Topic", "Topic name to delete.", "TopicA"),
                required_string("cluster_name", "Cluster", "Cluster name.", "DefaultCluster"),
            ],
            ResultViewKind::OperationSummary,
            Some("topic"),
        ),
        spec(
            "topic.order_conf",
            CommandCategory::Topic,
            "Apply Order Config",
            "Get, put, or delete ordered topic configuration.",
            RiskLevel::Mutating,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                enum_arg(
                    "method",
                    "Method",
                    "Order config method.",
                    &["get", "put", "delete"],
                    "get",
                ),
                optional_string(
                    "order_conf",
                    "Order Conf",
                    "Required for put, for example broker-a:4.",
                    "broker-a:4",
                ),
            ],
            ResultViewKind::Text,
            None,
        ),
        spec(
            "topic.allocate_mq",
            CommandCategory::Topic,
            "Allocate Message Queues",
            "Preview message queue allocation for IP list.",
            RiskLevel::Safe,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                required_string(
                    "ip_list",
                    "IP List",
                    "Consumer IP list accepted by core service.",
                    "192.168.1.1",
                ),
            ],
            ResultViewKind::Text,
            None,
        ),
    ]);
}

fn nameserver_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "namesrv.config.query",
            CommandCategory::NameServer,
            "Query NameServer Config",
            "Query NameServer configuration.",
            RiskLevel::Safe,
            vec![],
            ResultViewKind::KeyValue,
            None,
        ),
        spec(
            "namesrv.config.update",
            CommandCategory::NameServer,
            "Update NameServer Config",
            "Update one NameServer configuration key.",
            RiskLevel::Mutating,
            vec![
                required_string("key", "Key", "Configuration key.", "deleteWhen"),
                required_string("value", "Value", "Configuration value.", "04"),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "namesrv.kv.update",
            CommandCategory::NameServer,
            "Update KV Config",
            "Update NameServer KV config.",
            RiskLevel::Mutating,
            vec![
                required_string("namespace", "Namespace", "KV namespace.", "namespace"),
                required_string("key", "Key", "KV key.", "key"),
                required_string("value", "Value", "KV value.", "value"),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "namesrv.kv.delete",
            CommandCategory::NameServer,
            "Delete KV Config",
            "Delete NameServer KV config.",
            RiskLevel::Dangerous,
            vec![
                required_string("namespace", "Namespace", "KV namespace.", "namespace"),
                required_string("key", "Key", "KV key to delete.", "key"),
            ],
            ResultViewKind::OperationSummary,
            Some("key"),
        ),
        spec(
            "namesrv.write_perm.add",
            CommandCategory::NameServer,
            "Add Write Permission",
            "Add write permission for a broker.",
            RiskLevel::Mutating,
            vec![required_string(
                "broker_name",
                "Broker Name",
                "Broker name.",
                "broker-a",
            )],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "namesrv.write_perm.wipe",
            CommandCategory::NameServer,
            "Wipe Write Permission",
            "Wipe write permission for a broker.",
            RiskLevel::Dangerous,
            vec![required_string(
                "broker_name",
                "Broker Name",
                "Broker name.",
                "broker-a",
            )],
            ResultViewKind::OperationSummary,
            Some("broker_name"),
        ),
    ]);
}

fn auth_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "auth.user.get",
            CommandCategory::Auth,
            "Get User",
            "Query ACL user detail from a broker or cluster.",
            RiskLevel::Safe,
            broker_target_args_with(vec![required_string("username", "Username", "ACL username.", "user-a")]),
            ResultViewKind::Json,
            None,
        ),
        spec(
            "auth.user.list",
            CommandCategory::Auth,
            "List Users",
            "List ACL users from a broker or cluster.",
            RiskLevel::Safe,
            broker_target_args_with(vec![optional_string(
                "filter",
                "Filter",
                "Optional username filter.",
                "user",
            )]),
            ResultViewKind::Json,
            None,
        ),
        spec(
            "auth.user.create",
            CommandCategory::Auth,
            "Create User",
            "Create an ACL user on a broker or cluster.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                required_string("username", "Username", "ACL username.", "user-a"),
                required_string("password", "Password", "ACL password.", "secret"),
                optional_string("user_type", "User Type", "Optional user type.", "NORMAL"),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "auth.user.update",
            CommandCategory::Auth,
            "Update User",
            "Update an ACL user password, type, or status.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                required_string("username", "Username", "ACL username.", "user-a"),
                optional_string("password", "Password", "Optional new password.", "secret"),
                optional_string("user_type", "User Type", "Optional user type.", "SUPER"),
                optional_string("user_status", "User Status", "Optional user status.", "ENABLE"),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "auth.user.delete",
            CommandCategory::Auth,
            "Delete User",
            "Delete an ACL user from a broker or cluster.",
            RiskLevel::Dangerous,
            broker_target_args_with(vec![required_string(
                "username",
                "Username",
                "ACL username to delete.",
                "user-a",
            )]),
            ResultViewKind::OperationSummary,
            Some("username"),
        ),
        spec(
            "auth.user.copy",
            CommandCategory::Auth,
            "Copy Users",
            "Copy ACL users from one broker to another.",
            RiskLevel::Mutating,
            vec![
                required_string(
                    "from_broker",
                    "From Broker",
                    "Source broker address.",
                    "127.0.0.1:10911",
                ),
                required_string(
                    "to_broker",
                    "To Broker",
                    "Destination broker address.",
                    "127.0.0.2:10911",
                ),
                optional_string(
                    "usernames",
                    "Usernames",
                    "Optional comma-separated usernames.",
                    "user-a,user-b",
                ),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "auth.acl.get",
            CommandCategory::Auth,
            "Get ACL",
            "Query ACL rule detail from a broker or cluster.",
            RiskLevel::Safe,
            broker_target_args_with(vec![required_string(
                "subject",
                "Subject",
                "ACL subject, for example User:user-a.",
                "User:user-a",
            )]),
            ResultViewKind::Json,
            None,
        ),
        spec(
            "auth.acl.list",
            CommandCategory::Auth,
            "List ACL",
            "List ACL rules from a broker or cluster.",
            RiskLevel::Safe,
            broker_target_args_with(vec![
                optional_string(
                    "subject_filter",
                    "Subject Filter",
                    "Optional ACL subject filter.",
                    "User:user-a",
                ),
                optional_string(
                    "resource_filter",
                    "Resource Filter",
                    "Optional resource filter.",
                    "Topic:TopicA",
                ),
            ]),
            ResultViewKind::Json,
            None,
        ),
        spec(
            "auth.acl.create",
            CommandCategory::Auth,
            "Create ACL",
            "Create an ACL policy entry on a broker or cluster.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                required_string("subject", "Subject", "ACL subject.", "User:user-a"),
                required_string("resources", "Resources", "Comma-separated resources.", "Topic:TopicA"),
                required_string("actions", "Actions", "Comma-separated actions.", "PUB,SUB"),
                enum_arg("decision", "Decision", "ACL decision.", &["ALLOW", "DENY"], "ALLOW"),
                optional_string(
                    "source_ip",
                    "Source IP",
                    "Optional comma-separated source IPs.",
                    "127.0.0.1",
                ),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "auth.acl.update",
            CommandCategory::Auth,
            "Update ACL",
            "Update an ACL policy entry on a broker or cluster.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                required_string("subject", "Subject", "ACL subject.", "User:user-a"),
                required_string("resources", "Resources", "Comma-separated resources.", "Topic:TopicA"),
                required_string("actions", "Actions", "Comma-separated actions.", "PUB,SUB"),
                enum_arg("decision", "Decision", "ACL decision.", &["ALLOW", "DENY"], "ALLOW"),
                optional_string(
                    "source_ip",
                    "Source IP",
                    "Optional comma-separated source IPs.",
                    "127.0.0.1",
                ),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "auth.acl.delete",
            CommandCategory::Auth,
            "Delete ACL",
            "Delete an ACL subject or one subject/resource rule.",
            RiskLevel::Dangerous,
            broker_target_args_with(vec![
                required_string("subject", "Subject", "ACL subject to delete.", "User:user-a"),
                optional_string("resource", "Resource", "Optional resource to delete.", "Topic:TopicA"),
            ]),
            ResultViewKind::OperationSummary,
            Some("subject"),
        ),
        spec(
            "auth.acl.copy",
            CommandCategory::Auth,
            "Copy ACL",
            "Copy ACL subjects from one broker to another.",
            RiskLevel::Mutating,
            vec![
                required_string(
                    "from_broker",
                    "From Broker",
                    "Source broker address.",
                    "127.0.0.1:10911",
                ),
                required_string(
                    "to_broker",
                    "To Broker",
                    "Destination broker address.",
                    "127.0.0.2:10911",
                ),
                optional_string(
                    "subjects",
                    "Subjects",
                    "Optional comma-separated subjects.",
                    "User:user-a",
                ),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
    ]);
}

fn broker_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "broker.config.query",
            CommandCategory::Broker,
            "Query Broker Config",
            "Query broker configuration by broker address or cluster.",
            RiskLevel::Safe,
            broker_target_args_with(vec![optional_string(
                "key_pattern",
                "Key Pattern",
                "Optional regex filter.",
                "^flush.*",
            )]),
            ResultViewKind::Text,
            None,
        ),
        spec(
            "broker.config.update_plan",
            CommandCategory::Broker,
            "Plan Broker Config Update",
            "Build a broker configuration update plan without applying it.",
            RiskLevel::Safe,
            broker_target_args_with(vec![
                key_value_map("entries", "Entries", "One key=value pair per line."),
                bool_arg(
                    "rollback_enabled",
                    "Rollback Enabled",
                    "Build rollback data when possible.",
                    true,
                ),
            ]),
            ResultViewKind::Text,
            None,
        ),
        spec(
            "broker.config.update_apply",
            CommandCategory::Broker,
            "Apply Broker Config Update",
            "Apply broker configuration updates.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                key_value_map("entries", "Entries", "One key=value pair per line."),
                bool_arg(
                    "rollback_enabled",
                    "Rollback Enabled",
                    "Build rollback data when possible.",
                    true,
                ),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "broker.runtime_stats",
            CommandCategory::Broker,
            "Broker Runtime Stats",
            "Query broker runtime statistics by broker address or cluster.",
            RiskLevel::Safe,
            broker_target_args(),
            ResultViewKind::Text,
            None,
        ),
        spec(
            "broker.consume_stats",
            CommandCategory::Broker,
            "Broker Consume Stats",
            "Query consume stats from one broker.",
            RiskLevel::Safe,
            vec![
                required_string("broker_addr", "Broker Addr", "Broker address.", "127.0.0.1:10911"),
                number(
                    "timeout_millis",
                    "Timeout Millis",
                    "Request timeout in milliseconds.",
                    true,
                    Some(3000),
                    Some(1),
                ),
                number(
                    "diff_level",
                    "Diff Level",
                    "Minimum diff threshold.",
                    true,
                    Some(0),
                    Some(0),
                ),
                bool_arg("is_order", "Is Order", "Whether this is ordered consumption.", false),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "broker.epoch",
            CommandCategory::Broker,
            "Broker Epoch",
            "Query broker epoch cache by broker name or cluster.",
            RiskLevel::Safe,
            vec![
                optional_string("broker_name", "Broker Name", "Broker name target.", "broker-a"),
                optional_string("cluster_name", "Cluster", "Cluster name target.", "DefaultCluster"),
            ],
            ResultViewKind::Json,
            None,
        ),
        spec(
            "broker.cold_data_flow_ctr_info",
            CommandCategory::Broker,
            "Cold Data Flow Control",
            "Query cold data flow control information by broker address or cluster.",
            RiskLevel::Safe,
            broker_target_args(),
            ResultViewKind::Json,
            None,
        ),
        spec(
            "broker.clean_expired_cq",
            CommandCategory::Broker,
            "Clean Expired CQ",
            "Clean expired consume queue files by broker, cluster, topic, or global scope.",
            RiskLevel::Dangerous,
            broker_target_args_with(vec![
                optional_string("topic", "Topic", "Optional topic scope.", "TopicA"),
                bool_arg("dry_run", "Dry Run", "Only scan matching targets.", true),
            ]),
            ResultViewKind::OperationSummary,
            Some("broker_addr"),
        ),
        spec(
            "broker.delete_expired_commit_log",
            CommandCategory::Broker,
            "Delete Expired CommitLog",
            "Delete expired CommitLog files by broker or cluster.",
            RiskLevel::Dangerous,
            broker_target_args(),
            ResultViewKind::OperationSummary,
            Some("broker_addr"),
        ),
        spec(
            "broker.clean_unused_topic",
            CommandCategory::Broker,
            "Clean Unused Topic",
            "Clean unused topics by broker or cluster.",
            RiskLevel::Dangerous,
            broker_target_args(),
            ResultViewKind::OperationSummary,
            Some("broker_addr"),
        ),
        spec(
            "broker.reset_master_flush_offset",
            CommandCategory::Broker,
            "Reset Master Flush Offset",
            "Reset master flush offset for a slave broker.",
            RiskLevel::Dangerous,
            vec![
                required_string("broker_addr", "Broker Addr", "Slave broker address.", "127.0.0.1:10912"),
                number("offset", "Offset", "Master flush offset.", true, Some(0), Some(0)),
            ],
            ResultViewKind::OperationSummary,
            Some("broker_addr"),
        ),
        spec(
            "broker.cold_data_flow_ctr_update",
            CommandCategory::Broker,
            "Update Cold Data Flow Control",
            "Add or update a cold data flow control group threshold.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                required_string("consumer_group", "Consumer Group", "Consumer group.", "GroupA"),
                required_string("threshold", "Threshold", "Cold data flow threshold.", "1024"),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "broker.cold_data_flow_ctr_remove",
            CommandCategory::Broker,
            "Remove Cold Data Flow Control",
            "Remove a cold data flow control group threshold.",
            RiskLevel::Dangerous,
            broker_target_args_with(vec![required_string(
                "consumer_group",
                "Consumer Group",
                "Consumer group to remove.",
                "GroupA",
            )]),
            ResultViewKind::OperationSummary,
            Some("consumer_group"),
        ),
        spec(
            "broker.commit_log_read_ahead",
            CommandCategory::Broker,
            "Set CommitLog ReadAhead",
            "Inspect or update commitlog read-ahead mode and size.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                optional_string("mode", "Mode", "Optional mode config value: 0 normal, 1 random.", "0"),
                bool_arg("enable", "Enable", "Force normal read-ahead mode.", false),
                bool_arg("disable", "Disable", "Force random read-ahead mode.", false),
                optional_string("read_ahead_size", "ReadAhead Size", "Optional read-ahead size.", "4096"),
                optional_string(
                    "read_ahead_size_key",
                    "Size Key",
                    "Optional read-ahead size config key.",
                    "mappedFileSizeCommitLog",
                ),
                bool_arg(
                    "show_only",
                    "Show Only",
                    "Inspect current config without updates.",
                    false,
                ),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "broker.switch_timer_engine",
            CommandCategory::Broker,
            "Switch Timer Engine",
            "Switch timer message engine type.",
            RiskLevel::Dangerous,
            broker_target_args_with(vec![enum_arg(
                "engine_type",
                "Engine Type",
                "Timer engine type: R for RocksDB timeline, F for file time wheel.",
                &["R", "F"],
                "R",
            )]),
            ResultViewKind::OperationSummary,
            Some("broker_addr"),
        ),
    ]);
}

fn cluster_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "cluster.list",
            CommandCategory::Cluster,
            "Cluster List",
            "List brokers and cluster statistics.",
            RiskLevel::Safe,
            vec![
                bool_arg("more_stats", "More Stats", "Show extended statistics.", false),
                optional_string("cluster_name", "Cluster", "Optional cluster name.", "DefaultCluster"),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "cluster.broker_names",
            CommandCategory::Cluster,
            "Cluster Broker Names",
            "List broker names by cluster.",
            RiskLevel::Safe,
            vec![optional_string(
                "cluster_name",
                "Cluster",
                "Optional cluster name.",
                "DefaultCluster",
            )],
            ResultViewKind::KeyValue,
            None,
        ),
        spec(
            "cluster.send_message_rt",
            CommandCategory::Cluster,
            "Send Message RT",
            "Run send-message RT diagnostics.",
            RiskLevel::Safe,
            vec![
                number("amount", "Amount", "Messages to send.", true, Some(2), Some(1)),
                number("size", "Size", "Message body size.", true, Some(128), Some(0)),
                optional_string("cluster_name", "Cluster", "Optional cluster name.", "DefaultCluster"),
            ],
            ResultViewKind::Table,
            None,
        ),
    ]);
}

fn controller_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "controller.config.query",
            CommandCategory::Controller,
            "Query Controller Config",
            "Query controller configuration from one or more controller addresses.",
            RiskLevel::Safe,
            vec![required_string(
                "controller_address",
                "Controller",
                "Controller address list.",
                "127.0.0.1:9878",
            )],
            ResultViewKind::Json,
            None,
        ),
        spec(
            "controller.config.update",
            CommandCategory::Controller,
            "Update Controller Config",
            "Update one controller configuration key on one or more controller addresses.",
            RiskLevel::Mutating,
            vec![
                required_string(
                    "controller_address",
                    "Controller",
                    "Controller address list separated by semicolon.",
                    "127.0.0.1:9878",
                ),
                required_string("key", "Key", "Controller config key.", "enableElectUncleanMaster"),
                required_string("value", "Value", "Controller config value.", "true"),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "controller.metadata.query",
            CommandCategory::Controller,
            "Query Controller Metadata",
            "Query controller metadata from a controller address.",
            RiskLevel::Safe,
            vec![required_string(
                "controller_address",
                "Controller",
                "Controller address.",
                "127.0.0.1:9878",
            )],
            ResultViewKind::Json,
            None,
        ),
        spec(
            "controller.metadata.clean",
            CommandCategory::Controller,
            "Clean Controller Metadata",
            "Clean broker metadata from a controller.",
            RiskLevel::Dangerous,
            vec![
                required_string(
                    "controller_address",
                    "Controller",
                    "Controller address.",
                    "127.0.0.1:9878",
                ),
                required_string("broker_name", "Broker Name", "Broker name to clean.", "broker-a"),
                optional_string(
                    "broker_controller_ids",
                    "Broker Controller IDs",
                    "Optional semicolon-separated controller ids.",
                    "1;2",
                ),
                optional_string("cluster_name", "Cluster", "Cluster name.", "DefaultCluster"),
                bool_arg(
                    "clean_living_broker",
                    "Clean Living Broker",
                    "Allow cleaning living broker metadata.",
                    false,
                ),
            ],
            ResultViewKind::OperationSummary,
            Some("broker_name"),
        ),
    ]);
}

fn connection_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "connection.consumer",
            CommandCategory::Connection,
            "Consumer Connection",
            "Inspect consumer connection details.",
            RiskLevel::Safe,
            vec![
                required_string("consumer_group", "Consumer Group", "Consumer group.", "GroupA"),
                optional_string(
                    "broker_addr",
                    "Broker Addr",
                    "Optional broker address.",
                    "127.0.0.1:10911",
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "connection.producer",
            CommandCategory::Connection,
            "Producer Connection",
            "Inspect producer connection details.",
            RiskLevel::Safe,
            vec![
                required_string("producer_group", "Producer Group", "Producer group.", "ProducerGroupA"),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
            ],
            ResultViewKind::Table,
            None,
        ),
    ]);
}

fn consumer_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "consumer.config",
            CommandCategory::Consumer,
            "Consumer Config",
            "Query consumer group config.",
            RiskLevel::Safe,
            vec![required_string("group_name", "Group", "Consumer group.", "GroupA")],
            ResultViewKind::Text,
            None,
        ),
        spec(
            "consumer.running_info",
            CommandCategory::Consumer,
            "Consumer Running Info",
            "Query consumer running info.",
            RiskLevel::Safe,
            vec![
                required_string("group_name", "Group", "Consumer group.", "GroupA"),
                optional_string("client_id", "Client ID", "Optional client id.", "client-a"),
                optional_string(
                    "broker_addr",
                    "Broker Addr",
                    "Optional broker address.",
                    "127.0.0.1:10911",
                ),
                bool_arg("jstack", "JStack", "Include jstack data.", false),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "consumer.progress",
            CommandCategory::Consumer,
            "Consumer Progress",
            "Query consumer progress by group/topic or all groups.",
            RiskLevel::Safe,
            vec![
                optional_string("consumer_group", "Consumer Group", "Optional consumer group.", "GroupA"),
                optional_string("topic_name", "Topic", "Optional topic name.", "TopicA"),
                bool_arg("show_client_ip", "Show Client IP", "Show client IP in rows.", false),
                optional_string("cluster", "Cluster", "Optional cluster name.", "DefaultCluster"),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "consumer.delete_subscription_group",
            CommandCategory::Consumer,
            "Delete Subscription Group",
            "Delete a subscription group by broker or cluster.",
            RiskLevel::Dangerous,
            broker_target_args_with(vec![
                required_string("group_name", "Group", "Group to delete.", "GroupA"),
                bool_arg("remove_offset", "Remove Offset", "Remove stored offsets.", false),
            ]),
            ResultViewKind::OperationSummary,
            Some("group_name"),
        ),
        spec(
            "consumer.set_consume_mode",
            CommandCategory::Consumer,
            "Set Consume Mode",
            "Set consume mode for a topic/group.",
            RiskLevel::Mutating,
            broker_target_args_with(vec![
                required_string("topic_name", "Topic", "Topic name.", "TopicA"),
                required_string("group_name", "Group", "Consumer group.", "GroupA"),
                enum_arg("mode", "Mode", "Message request mode.", &["pull", "pop"], "pull"),
                number(
                    "pop_share_queue_num",
                    "Pop Share Queues",
                    "Optional POP share queue count.",
                    false,
                    None,
                    Some(0),
                ),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "consumer.update_subscription_group",
            CommandCategory::Consumer,
            "Update Subscription Group",
            "Create or update one subscription group config by broker or cluster.",
            RiskLevel::Mutating,
            subscription_group_args("group_name", "Group", "Consumer group.", "GroupA"),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "consumer.update_subscription_group_list",
            CommandCategory::Consumer,
            "Update Subscription Group List",
            "Create or update several subscription group configs with the same settings.",
            RiskLevel::Mutating,
            subscription_group_args(
                "group_names",
                "Groups",
                "Comma-separated consumer groups.",
                "GroupA,GroupB",
            ),
            ResultViewKind::OperationSummary,
            None,
        ),
    ]);
}

fn offset_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "offset.clone_group",
            CommandCategory::Offset,
            "Clone Group Offset",
            "Clone offsets from one group to another.",
            RiskLevel::Mutating,
            vec![
                required_string("src_group", "Source Group", "Source consumer group.", "SourceGroup"),
                required_string(
                    "dest_group",
                    "Destination Group",
                    "Destination consumer group.",
                    "DestGroup",
                ),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                bool_arg("offline", "Offline", "Clone offline offsets.", false),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "offset.consumer_status",
            CommandCategory::Offset,
            "Consumer Status",
            "Query consumer status offsets.",
            RiskLevel::Safe,
            vec![
                required_string("group", "Group", "Consumer group.", "GroupA"),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                optional_string(
                    "origin_client_id",
                    "Origin Client ID",
                    "Optional origin client id.",
                    "client-a",
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "offset.skip_accumulated",
            CommandCategory::Offset,
            "Skip Accumulated Messages",
            "Reset offsets to latest for accumulated messages.",
            RiskLevel::Dangerous,
            vec![
                required_string("group", "Group", "Consumer group.", "GroupA"),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                optional_string("cluster", "Cluster", "Optional cluster name.", "DefaultCluster"),
                bool_arg("force", "Force", "Force reset.", true),
            ],
            ResultViewKind::OperationSummary,
            Some("group"),
        ),
        spec(
            "offset.reset_by_time",
            CommandCategory::Offset,
            "Reset Offset By Time",
            "Reset offsets by timestamp in milliseconds.",
            RiskLevel::Dangerous,
            vec![
                required_string("group", "Group", "Consumer group.", "GroupA"),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                timestamp("timestamp", "Timestamp", "Timestamp in milliseconds."),
            ],
            ResultViewKind::OperationSummary,
            Some("group"),
        ),
        spec(
            "offset.reset_by_time_old",
            CommandCategory::Offset,
            "Reset Offset By Time Old",
            "Reset offsets using the legacy path.",
            RiskLevel::Dangerous,
            vec![
                required_string("group", "Group", "Consumer group.", "GroupA"),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                timestamp("timestamp", "Timestamp", "Timestamp in milliseconds."),
                bool_arg("force", "Force", "Force reset.", true),
                optional_string("cluster", "Cluster", "Optional cluster name.", "DefaultCluster"),
            ],
            ResultViewKind::OperationSummary,
            Some("group"),
        ),
    ]);
}

fn queue_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "queue.consume_queue",
            CommandCategory::Queue,
            "Query Consume Queue",
            "Query consume queue entries.",
            RiskLevel::Safe,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                number("queue_id", "Queue ID", "Queue id.", true, Some(0), Some(0)),
                number("index", "Index", "Queue index.", true, Some(0), Some(0)),
                number("count", "Count", "Entry count.", true, Some(32), Some(1)),
                optional_string(
                    "broker_addr",
                    "Broker Addr",
                    "Optional broker address.",
                    "127.0.0.1:10911",
                ),
                optional_string("consumer_group", "Consumer Group", "Optional consumer group.", "GroupA"),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "queue.rocksdb_cq_progress",
            CommandCategory::Queue,
            "RocksDB CQ Write Progress",
            "Check RocksDB consume queue write progress.",
            RiskLevel::Safe,
            vec![
                required_string("cluster_name", "Cluster", "Cluster name.", "DefaultCluster"),
                optional_string("topic", "Topic", "Optional topic.", "TopicA"),
                number(
                    "check_from",
                    "Check From",
                    "Optional start timestamp.",
                    false,
                    None,
                    Some(0),
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
    ]);
}

fn ha_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "ha.status",
            CommandCategory::Ha,
            "HA Status",
            "Query HA status by broker address or cluster.",
            RiskLevel::Safe,
            broker_target_args(),
            ResultViewKind::Text,
            None,
        ),
        spec(
            "ha.sync_state_set",
            CommandCategory::Ha,
            "Sync State Set",
            "Query in-sync state set from a controller.",
            RiskLevel::Safe,
            vec![
                required_string(
                    "controller_address",
                    "Controller",
                    "Controller address.",
                    "127.0.0.1:9878",
                ),
                optional_string("broker_name", "Broker Name", "Broker name target.", "broker-a"),
                optional_string("cluster_name", "Cluster", "Cluster target.", "DefaultCluster"),
            ],
            ResultViewKind::Json,
            None,
        ),
    ]);
}

fn stats_commands(commands: &mut Vec<CommandSpec>) {
    commands.push(spec(
        "stats.all",
        CommandCategory::Stats,
        "Stats All",
        "Query all topic and group statistics.",
        RiskLevel::Safe,
        vec![
            bool_arg("active_topic", "Active Topic", "Only include active topics.", false),
            optional_string("topic", "Topic", "Optional topic filter.", "TopicA"),
        ],
        ResultViewKind::Table,
        None,
    ));
}

fn producer_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "producer.info",
            CommandCategory::Producer,
            "Producer Info",
            "Query producer information from a broker.",
            RiskLevel::Safe,
            vec![required_string(
                "broker_addr",
                "Broker Addr",
                "Broker address.",
                "127.0.0.1:10911",
            )],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "producer.send_message",
            CommandCategory::Producer,
            "Send Message",
            "Send one message to a topic or a specific broker queue.",
            RiskLevel::Mutating,
            vec![
                required_string("topic", "Topic", "Target topic.", "TopicA"),
                required_string("body", "Body", "Message body text.", "hello RocketMQ"),
                optional_string("keys", "Keys", "Optional message keys.", "KeyA"),
                optional_string("tags", "Tags", "Optional message tags.", "TagA"),
                optional_string(
                    "broker_name",
                    "Broker Name",
                    "Optional broker queue target.",
                    "broker-a",
                ),
                number(
                    "queue_id",
                    "Queue ID",
                    "Optional queue id; requires broker name.",
                    false,
                    None,
                    Some(0),
                ),
                bool_arg("msg_trace_enable", "Trace", "Enable message trace flag.", false),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "producer.send_message_status",
            CommandCategory::Producer,
            "Send Message Status",
            "Send diagnostic messages to a broker-named topic and collect per-send RT.",
            RiskLevel::Mutating,
            vec![
                required_string(
                    "broker_name",
                    "Broker Name",
                    "Broker name used as diagnostic topic.",
                    "broker-a",
                ),
                number(
                    "message_size",
                    "Message Size",
                    "Diagnostic message body size.",
                    true,
                    Some(128),
                    Some(1),
                ),
                number(
                    "count",
                    "Count",
                    "Number of diagnostic messages.",
                    true,
                    Some(1),
                    Some(1),
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "producer.check_message_send_rt",
            CommandCategory::Producer,
            "Check Message Send RT",
            "Send diagnostic messages to a topic and collect queue-level RT samples.",
            RiskLevel::Mutating,
            vec![
                required_string("topic", "Topic", "Diagnostic target topic.", "TopicA"),
                number("amount", "Amount", "Messages to send.", true, Some(2), Some(2)),
                number("size", "Size", "Message body size.", true, Some(128), Some(1)),
            ],
            ResultViewKind::Table,
            None,
        ),
    ]);
}

fn lite_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "lite.broker_info",
            CommandCategory::Lite,
            "Broker Lite Info",
            "Query broker lite information by broker address or cluster.",
            RiskLevel::Safe,
            broker_target_args(),
            ResultViewKind::Json,
            None,
        ),
        spec(
            "lite.parent_topic_info",
            CommandCategory::Lite,
            "Parent Topic Info",
            "Query lite parent topic information.",
            RiskLevel::Safe,
            vec![required_string(
                "parent_topic",
                "Parent Topic",
                "Parent topic name.",
                "ParentTopicA",
            )],
            ResultViewKind::Json,
            None,
        ),
        spec(
            "lite.topic_info",
            CommandCategory::Lite,
            "Lite Topic Info",
            "Query lite topic information.",
            RiskLevel::Safe,
            vec![
                required_string("parent_topic", "Parent Topic", "Parent topic name.", "ParentTopicA"),
                required_string("lite_topic", "Lite Topic", "Lite topic name.", "LiteTopicA"),
            ],
            ResultViewKind::Json,
            None,
        ),
        spec(
            "lite.group_info",
            CommandCategory::Lite,
            "Lite Group Info",
            "Query lite group information.",
            RiskLevel::Safe,
            vec![
                required_string("parent_topic", "Parent Topic", "Parent topic name.", "ParentTopicA"),
                required_string("group", "Group", "Consumer group.", "GroupA"),
                optional_string("lite_topic", "Lite Topic", "Optional lite topic filter.", "LiteTopicA"),
                number("top_k", "Top K", "Optional top K count.", false, None, Some(1)),
            ],
            ResultViewKind::Json,
            None,
        ),
        spec(
            "lite.client_info",
            CommandCategory::Lite,
            "Lite Client Info",
            "Query lite client information.",
            RiskLevel::Safe,
            vec![
                required_string("parent_topic", "Parent Topic", "Parent topic name.", "ParentTopicA"),
                required_string("group", "Group", "Consumer group.", "GroupA"),
                required_string("client_id", "Client ID", "Client id.", "client-a"),
            ],
            ResultViewKind::Json,
            None,
        ),
        spec(
            "lite.trigger_dispatch",
            CommandCategory::Lite,
            "Trigger Lite Dispatch",
            "Trigger lite dispatch for a group and optional client or broker.",
            RiskLevel::Mutating,
            vec![
                required_string("parent_topic", "Parent Topic", "Parent topic name.", "ParentTopicA"),
                required_string("group", "Group", "Consumer group.", "GroupA"),
                optional_string("client_id", "Client ID", "Optional client id.", "client-a"),
                optional_string("broker_name", "Broker Name", "Optional broker name.", "broker-a"),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
    ]);
}

fn message_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "message.decode_id",
            CommandCategory::Message,
            "Decode Message ID",
            "Decode one or more message IDs locally.",
            RiskLevel::Safe,
            vec![required_string(
                "message_ids",
                "Message IDs",
                "Message IDs separated by comma, semicolon, or whitespace.",
                "7F0000010007D8260BF075769D36C348",
            )],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.query_by_id",
            CommandCategory::Message,
            "Query Message By ID",
            "Query one or more messages by broker message ID.",
            RiskLevel::Safe,
            vec![
                required_string(
                    "message_ids",
                    "Message IDs",
                    "Message IDs separated by comma, semicolon, or whitespace.",
                    "7F0000010007D8260BF075769D36C348",
                ),
                optional_string("topic", "Topic", "Optional topic hint.", "TopicA"),
                number(
                    "timeout_millis",
                    "Timeout",
                    "Per-message query timeout in milliseconds.",
                    true,
                    Some(3000),
                    Some(1),
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.query_by_key",
            CommandCategory::Message,
            "Query Message By Key",
            "Query messages by topic and key or tag.",
            RiskLevel::Safe,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                required_string("msg_key", "Message Key", "Message key or tag.", "order-1"),
                number(
                    "begin_timestamp",
                    "Begin Timestamp",
                    "Optional begin timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
                number(
                    "end_timestamp",
                    "End Timestamp",
                    "Optional end timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
                number(
                    "max_num",
                    "Max Num",
                    "Maximum messages to return.",
                    true,
                    Some(32),
                    Some(1),
                ),
                optional_string("cluster", "Cluster", "Optional cluster name.", "DefaultCluster"),
                enum_arg("key_type", "Key Type", "K for key, T for tag.", &["K", "T"], "K"),
                optional_string("last_key", "Last Key", "Optional pagination key.", "last-key"),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.query_by_unique_key",
            CommandCategory::Message,
            "Query Message By Unique Key",
            "Query messages by unique message key and optional direct-consume target.",
            RiskLevel::Safe,
            vec![
                required_string(
                    "msg_id",
                    "Message ID",
                    "Unique message ID.",
                    "7F0000010007D8260BF075769D36C348",
                ),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                optional_string("consumer_group", "Consumer Group", "Optional consumer group.", "GroupA"),
                optional_string("client_id", "Client ID", "Optional consumer client id.", "client-a"),
                bool_arg("show_all", "Show All", "Show all matched messages.", false),
                optional_string("cluster", "Cluster", "Optional cluster name.", "DefaultCluster"),
                number(
                    "start_time",
                    "Start Time",
                    "Optional begin timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
                number(
                    "end_time",
                    "End Time",
                    "Optional end timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.query_by_offset",
            CommandCategory::Message,
            "Query Message By Offset",
            "Query one message by topic, broker, queue, and offset.",
            RiskLevel::Safe,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                required_string("broker_name", "Broker Name", "Broker name.", "broker-a"),
                number("queue_id", "Queue ID", "Queue id.", true, Some(0), Some(0)),
                number("offset", "Offset", "Queue offset.", true, Some(0), Some(0)),
                optional_string("route_topic", "Route Topic", "Optional route topic.", "TopicA"),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.query_trace_by_id",
            CommandCategory::Message,
            "Query Message Trace",
            "Query message trace by message ID.",
            RiskLevel::Safe,
            vec![
                required_string(
                    "msg_id",
                    "Message ID",
                    "Message ID.",
                    "7F0000010007D8260BF075769D36C348",
                ),
                optional_string(
                    "trace_topic",
                    "Trace Topic",
                    "Optional trace topic; default uses RocketMQ trace topic.",
                    "RMQ_SYS_TRACE_TOPIC",
                ),
                number(
                    "begin_timestamp",
                    "Begin Timestamp",
                    "Optional begin timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
                number(
                    "end_timestamp",
                    "End Timestamp",
                    "Optional end timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
                number(
                    "max_num",
                    "Max Num",
                    "Maximum trace rows to return.",
                    true,
                    Some(32),
                    Some(1),
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.dump_compaction_log",
            CommandCategory::Message,
            "Dump Compaction Log",
            "Decode messages from a local compaction log file.",
            RiskLevel::Safe,
            vec![required_string(
                "file",
                "File",
                "Local compaction log file path.",
                "./compact.log",
            )],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.print",
            CommandCategory::Message,
            "Print Messages",
            "Pull and display messages for a topic with a local event cap.",
            RiskLevel::Safe,
            message_stream_args(vec![optional_string(
                "lmq_parent_topic",
                "LMQ Parent Topic",
                "Optional parent topic for LMQ route lookup.",
                "ParentTopicA",
            )]),
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.print_by_queue",
            CommandCategory::Message,
            "Print Messages By Queue",
            "Pull and display messages from one topic queue with a local event cap.",
            RiskLevel::Safe,
            message_stream_args(vec![
                required_string("broker_name", "Broker Name", "Broker name.", "broker-a"),
                number("queue_id", "Queue ID", "Queue id.", true, Some(0), Some(0)),
                bool_arg(
                    "print_messages",
                    "Print Messages",
                    "Include message batches in the result.",
                    true,
                ),
                bool_arg("calculate_by_tag", "Calculate By Tag", "Return tag counters.", false),
            ]),
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.consume",
            CommandCategory::Message,
            "Consume Messages",
            "Pull messages by route or queue using Java consumeMessage-style targeting.",
            RiskLevel::Safe,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                optional_string("broker_name", "Broker Name", "Optional broker name.", "broker-a"),
                number("queue_id", "Queue ID", "Optional queue id.", false, None, Some(0)),
                number("offset", "Offset", "Optional queue offset.", false, None, Some(0)),
                optional_string("consumer_group", "Consumer Group", "Optional consumer group.", "GroupA"),
                number(
                    "begin_timestamp",
                    "Begin Timestamp",
                    "Optional begin timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
                number(
                    "end_timestamp",
                    "End Timestamp",
                    "Optional end timestamp in milliseconds.",
                    false,
                    None,
                    Some(0),
                ),
                number(
                    "message_number",
                    "Message Number",
                    "Maximum messages to consume.",
                    true,
                    Some(32),
                    Some(1),
                ),
                number(
                    "max_events",
                    "Max Events",
                    "Maximum pull events retained by the TUI.",
                    true,
                    Some(128),
                    Some(1),
                ),
            ],
            ResultViewKind::Table,
            None,
        ),
    ]);
}

fn export_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "export.configs",
            CommandCategory::Export,
            "Export Configs",
            "Export broker config values for a cluster into a structured result.",
            RiskLevel::Safe,
            with_export_output_args(vec![required_string(
                "cluster_name",
                "Cluster",
                "Cluster name.",
                "DefaultCluster",
            )]),
            ResultViewKind::Table,
            None,
        ),
        spec(
            "export.metadata",
            CommandCategory::Export,
            "Export Metadata",
            "Export topic and subscription metadata from a broker or cluster.",
            RiskLevel::Safe,
            with_export_output_args(vec![
                optional_string("cluster_name", "Cluster", "Cluster name target.", "DefaultCluster"),
                optional_string(
                    "broker_addr",
                    "Broker Addr",
                    "Broker address target.",
                    "127.0.0.1:10911",
                ),
                bool_arg("topic_only", "Topic Only", "Export topic configs only.", false),
                bool_arg(
                    "subscription_group_only",
                    "Subscription Only",
                    "Export subscription groups only.",
                    false,
                ),
                bool_arg("special_topic", "Special Topic", "Include special topics.", false),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "export.metadata_rocksdb",
            CommandCategory::Export,
            "Export Metadata In RocksDB",
            "Read metadata entries from a local RocksDB config directory.",
            RiskLevel::Safe,
            with_export_output_args(vec![
                required_string("path", "Path", "Local config directory path.", "./store/config"),
                enum_arg(
                    "config_type",
                    "Config Type",
                    "RocksDB config type.",
                    &["topics", "subscriptionGroups"],
                    "topics",
                ),
                bool_arg(
                    "json_enable",
                    "JSON",
                    "Render values as JSON data where supported.",
                    true,
                ),
            ]),
            ResultViewKind::Table,
            None,
        ),
        spec(
            "export.pop_record",
            CommandCategory::Export,
            "Export POP Records",
            "Trigger broker-side POP record export or dry-run target resolution.",
            RiskLevel::Mutating,
            with_export_output_args(vec![
                optional_string("cluster_name", "Cluster", "Cluster name target.", "DefaultCluster"),
                optional_string(
                    "broker_addr",
                    "Broker Addr",
                    "Broker address target.",
                    "127.0.0.1:10911",
                ),
                bool_arg("dry_run", "Dry Run", "Resolve targets without exporting.", true),
                number(
                    "timeout_millis",
                    "Timeout",
                    "Optional broker request timeout in milliseconds.",
                    false,
                    None,
                    Some(1),
                ),
            ]),
            ResultViewKind::OperationSummary,
            None,
        ),
    ]);
}

fn with_export_output_args(mut args: Vec<ArgSpec>) -> Vec<ArgSpec> {
    args.push(optional_string(
        "output_path",
        "Output Path",
        "Optional JSON file path for writing the export result.",
        "./rocketmq-export.json",
    ));
    args.push(optional_bool_arg(
        "overwrite",
        "Overwrite",
        "Replace the output file when it already exists.",
        false,
    ));
    args
}

fn static_topic_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "static_topic.update",
            CommandCategory::StaticTopic,
            "Update Static Topic",
            "Create or update static topic queue mapping across brokers and clusters.",
            RiskLevel::Mutating,
            vec![
                required_string("topic", "Topic", "Static topic name.", "StaticTopicA"),
                required_string(
                    "broker_names",
                    "Broker Names",
                    "Comma-separated broker names.",
                    "broker-a,broker-b",
                ),
                number("queue_num", "Queue Num", "Total queue count.", true, Some(4), Some(1)),
                optional_string(
                    "cluster_names",
                    "Cluster Names",
                    "Optional comma-separated clusters.",
                    "DefaultCluster",
                ),
            ],
            ResultViewKind::OperationSummary,
            None,
        ),
        spec(
            "static_topic.remap",
            CommandCategory::StaticTopic,
            "Remap Static Topic",
            "Remap static topic queues across brokers and clusters.",
            RiskLevel::Dangerous,
            vec![
                required_string("topic", "Topic", "Static topic name.", "StaticTopicA"),
                optional_string(
                    "broker_names",
                    "Broker Names",
                    "Optional comma-separated broker names.",
                    "broker-a,broker-b",
                ),
                optional_string(
                    "cluster_names",
                    "Cluster Names",
                    "Optional comma-separated clusters.",
                    "DefaultCluster",
                ),
                bool_arg(
                    "force_replace",
                    "Force Replace",
                    "Force replacing existing mapping.",
                    false,
                ),
            ],
            ResultViewKind::OperationSummary,
            Some("topic"),
        ),
    ]);
}

fn spec(
    id: &'static str,
    category: CommandCategory,
    title: &'static str,
    description: &'static str,
    risk_level: RiskLevel,
    args: Vec<ArgSpec>,
    result_view_kind: ResultViewKind,
    confirmation_field: Option<&'static str>,
) -> CommandSpec {
    CommandSpec {
        id,
        category,
        title,
        description,
        risk_level,
        args,
        executor: CommandExecutor::Facade,
        result_view_kind,
        confirmation_field,
    }
}

fn required_string(name: &'static str, label: &'static str, help: &'static str, placeholder: &'static str) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required: true,
        kind: ArgKind::String { placeholder },
    }
}

fn optional_string(name: &'static str, label: &'static str, help: &'static str, placeholder: &'static str) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required: false,
        kind: ArgKind::OptionalString { placeholder },
    }
}

fn number(
    name: &'static str,
    label: &'static str,
    help: &'static str,
    required: bool,
    default: Option<i64>,
    min: Option<i64>,
) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required,
        kind: ArgKind::Number { default, min },
    }
}

fn bool_arg(name: &'static str, label: &'static str, help: &'static str, default: bool) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required: true,
        kind: ArgKind::Bool { default },
    }
}

fn optional_bool_arg(name: &'static str, label: &'static str, help: &'static str, default: bool) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required: false,
        kind: ArgKind::Bool { default },
    }
}

fn enum_arg(
    name: &'static str,
    label: &'static str,
    help: &'static str,
    values: &'static [&'static str],
    default: &'static str,
) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required: true,
        kind: ArgKind::Enum { values, default },
    }
}

fn key_value_map(name: &'static str, label: &'static str, help: &'static str) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required: true,
        kind: ArgKind::KeyValueMap,
    }
}

fn timestamp(name: &'static str, label: &'static str, help: &'static str) -> ArgSpec {
    ArgSpec {
        name,
        label,
        help,
        required: true,
        kind: ArgKind::TimestampMillis,
    }
}

fn broker_target_args() -> Vec<ArgSpec> {
    vec![
        optional_string(
            "broker_addr",
            "Broker Addr",
            "Broker address target.",
            "127.0.0.1:10911",
        ),
        optional_string("cluster_name", "Cluster", "Cluster name target.", "DefaultCluster"),
    ]
}

fn broker_target_args_with(mut extra: Vec<ArgSpec>) -> Vec<ArgSpec> {
    let mut args = broker_target_args();
    args.append(&mut extra);
    args
}

fn subscription_group_args(
    group_field: &'static str,
    group_label: &'static str,
    group_help: &'static str,
    group_placeholder: &'static str,
) -> Vec<ArgSpec> {
    broker_target_args_with(vec![
        required_string(group_field, group_label, group_help, group_placeholder),
        bool_arg("consume_enable", "Consume Enable", "Enable consumption.", true),
        bool_arg(
            "consume_from_min_enable",
            "Consume From Min",
            "Enable consume from min offset.",
            true,
        ),
        bool_arg(
            "consume_broadcast_enable",
            "Broadcast Enable",
            "Enable broadcast consumption.",
            true,
        ),
        bool_arg(
            "consume_message_orderly",
            "Orderly",
            "Enable ordered consumption.",
            false,
        ),
        number(
            "retry_queue_nums",
            "Retry Queues",
            "Retry queue count.",
            true,
            Some(1),
            Some(0),
        ),
        number(
            "retry_max_times",
            "Retry Max",
            "Maximum retry count.",
            true,
            Some(16),
            Some(0),
        ),
        number("broker_id", "Broker ID", "Broker id.", true, Some(0), Some(0)),
        number(
            "which_broker_when_consume_slowly",
            "Slow Broker ID",
            "Broker id used when consumption is slow.",
            true,
            Some(1),
            Some(0),
        ),
        bool_arg(
            "notify_consumer_ids_changed_enable",
            "Notify Consumers",
            "Notify clients after consumer ids change.",
            true,
        ),
        number(
            "group_sys_flag",
            "Group Sys Flag",
            "Group system flag.",
            true,
            Some(0),
            Some(0),
        ),
        number(
            "consume_timeout_minute",
            "Timeout Minutes",
            "Consume timeout in minutes.",
            true,
            Some(15),
            Some(1),
        ),
    ])
}

fn message_stream_args(mut extra: Vec<ArgSpec>) -> Vec<ArgSpec> {
    let mut args = vec![
        required_string("topic", "Topic", "Topic name.", "TopicA"),
        required_string("sub_expression", "Sub Expression", "Tag expression.", "*"),
        number(
            "begin_timestamp",
            "Begin Timestamp",
            "Optional begin timestamp in milliseconds.",
            false,
            None,
            Some(0),
        ),
        number(
            "end_timestamp",
            "End Timestamp",
            "Optional end timestamp in milliseconds.",
            false,
            None,
            Some(0),
        ),
        number(
            "max_events",
            "Max Events",
            "Maximum pull events retained by the TUI.",
            true,
            Some(128),
            Some(1),
        ),
    ];
    args.append(&mut extra);
    args
}

fn topic_target(form: &CommandFormState) -> anyhow::Result<TopicTarget> {
    let target = form.required_string("target")?;
    match form.enum_string("target_type")?.as_str() {
        "broker" => Ok(TopicTarget::Broker(target.into())),
        "cluster" => Ok(TopicTarget::Cluster(target.into())),
        value => bail!("invalid target_type: {value}"),
    }
}

fn broker_config_update_request(
    facade: &TuiAdminFacade,
    form: &CommandFormState,
) -> anyhow::Result<rocketmq_admin_core::core::broker::BrokerConfigUpdateRequest> {
    let entries: BTreeMap<String, String> = form.key_value_map("entries")?.into_iter().collect();
    facade
        .broker_config_update_request(
            form.optional_string("broker_addr"),
            form.optional_string("cluster_name"),
            entries,
            form.bool_value("rollback_enabled")?,
        )
        .context("failed to build broker config update request")
}

fn message_request_mode(mode: String) -> anyhow::Result<MessageRequestMode> {
    match mode.trim().to_ascii_lowercase().as_str() {
        "pull" => Ok(MessageRequestMode::Pull),
        "pop" => Ok(MessageRequestMode::Pop),
        value => bail!("invalid consume mode: {value}"),
    }
}

fn operation_target_label(broker_addr: Option<String>, cluster_name: Option<String>, fallback: &str) -> String {
    broker_addr
        .map(|broker_addr| format!("broker {broker_addr}"))
        .or_else(|| cluster_name.map(|cluster_name| format!("cluster {cluster_name}")))
        .unwrap_or_else(|| fallback.to_string())
}

fn optional_u64_arg(form: &CommandFormState, name: &str) -> anyhow::Result<Option<u64>> {
    form.optional_string(name)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|error| anyhow::anyhow!("{name} must be an unsigned integer: {error}"))
        })
        .transpose()
}

fn number_usize_arg(form: &CommandFormState, name: &str) -> anyhow::Result<usize> {
    let value = form.number_u64(name)?;
    usize::try_from(value).map_err(|error| anyhow::anyhow!("{name} is out of range for usize: {error}"))
}

fn export_output_or_view<T>(
    facade: &TuiAdminFacade,
    title: &str,
    form: &CommandFormState,
    value: &T,
    view: CommandResultViewModel,
) -> anyhow::Result<CommandResultViewModel>
where
    T: Serialize + ?Sized,
{
    let Some(output_path) = form.optional_string("output_path") else {
        return Ok(view);
    };
    let result = facade.write_export_json_file(output_path, form.bool_value("overwrite")?, value)?;
    Ok(CommandResultViewModel::export_file_written(title, &result))
}

trait DebugTail {
    fn with_debug_tail(self, value: &impl std::fmt::Debug) -> CommandResultViewModel;
}

impl DebugTail for CommandResultViewModel {
    fn with_debug_tail(self, value: &impl std::fmt::Debug) -> CommandResultViewModel {
        match self {
            CommandResultViewModel::OperationSummary(mut summary) => {
                summary.targets.push(format!("{value:#?}"));
                CommandResultViewModel::OperationSummary(summary)
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::command_catalog;
    use super::ArgKind;
    use super::ResultViewKind;
    use super::RiskLevel;
    use crate::state::CommandFormState;

    #[test]
    fn command_catalog_covers_facade_backed_admin_panel() {
        let catalog = command_catalog();
        let ids = catalog.iter().map(|command| command.id).collect::<HashSet<_>>();

        for expected in [
            "topic.list",
            "topic.cluster",
            "topic.route",
            "topic.status",
            "topic.update",
            "topic.update_perm",
            "topic.delete",
            "topic.order_conf",
            "topic.allocate_mq",
            "namesrv.config.query",
            "namesrv.config.update",
            "namesrv.kv.update",
            "namesrv.kv.delete",
            "namesrv.write_perm.add",
            "namesrv.write_perm.wipe",
            "broker.config.query",
            "broker.config.update_plan",
            "broker.config.update_apply",
            "broker.runtime_stats",
            "broker.consume_stats",
            "cluster.list",
            "cluster.broker_names",
            "cluster.send_message_rt",
            "connection.consumer",
            "connection.producer",
            "consumer.config",
            "consumer.running_info",
            "consumer.progress",
            "consumer.delete_subscription_group",
            "consumer.set_consume_mode",
            "offset.clone_group",
            "offset.consumer_status",
            "offset.skip_accumulated",
            "offset.reset_by_time",
            "offset.reset_by_time_old",
            "queue.consume_queue",
            "queue.rocksdb_cq_progress",
            "ha.status",
            "ha.sync_state_set",
            "stats.all",
            "producer.info",
            "producer.send_message",
            "producer.send_message_status",
            "producer.check_message_send_rt",
            "auth.user.get",
            "auth.user.list",
            "auth.acl.get",
            "auth.acl.list",
            "controller.config.query",
            "controller.metadata.query",
            "broker.epoch",
            "broker.cold_data_flow_ctr_info",
            "lite.broker_info",
            "lite.parent_topic_info",
            "lite.topic_info",
            "lite.group_info",
            "lite.client_info",
            "message.decode_id",
            "message.query_by_id",
            "message.query_by_key",
            "message.query_by_unique_key",
            "message.query_by_offset",
            "message.query_trace_by_id",
        ] {
            assert!(ids.contains(expected), "missing command {expected}");
        }
    }

    #[test]
    fn command_ids_are_unique() {
        let catalog = command_catalog();
        let ids = catalog.iter().map(|command| command.id).collect::<HashSet<_>>();
        assert_eq!(catalog.len(), ids.len());
    }

    #[test]
    fn command_search_matches_category_id_title_and_description() {
        let catalog = command_catalog();
        let topic_list = catalog.iter().find(|command| command.id == "topic.list").unwrap();

        assert!(topic_list.matches_query("topic"));
        assert!(topic_list.matches_query("Topic"));
        assert!(topic_list.matches_query("list"));
        assert!(topic_list.matches_query("cluster"));
        assert!(!topic_list.matches_query("producer info"));
    }

    #[test]
    fn dangerous_command_requires_target_confirmation() {
        let catalog = command_catalog();
        let command = catalog.iter().find(|command| command.id == "topic.delete").unwrap();
        assert_eq!(command.risk_level, RiskLevel::Dangerous);

        let mut form = CommandFormState::for_command(command);
        form.set_value("topic", "TopicA".to_string());

        assert_eq!(command.expected_confirmation(&form), Some("TopicA".to_string()));
    }

    #[test]
    fn mutating_command_requires_confirm_literal() {
        let catalog = command_catalog();
        let command = catalog.iter().find(|command| command.id == "topic.update").unwrap();
        let form = CommandFormState::for_command(command);

        assert_eq!(command.expected_confirmation(&form), Some("confirm".to_string()));
    }

    #[test]
    fn phase_three_catalog_exposes_mutating_management_commands() {
        let catalog = command_catalog();
        let ids = catalog.iter().map(|command| command.id).collect::<HashSet<_>>();

        for expected in [
            "auth.user.create",
            "auth.user.update",
            "auth.user.delete",
            "auth.user.copy",
            "auth.acl.create",
            "auth.acl.update",
            "auth.acl.delete",
            "auth.acl.copy",
            "controller.config.update",
            "controller.metadata.clean",
            "broker.clean_expired_cq",
            "broker.delete_expired_commit_log",
            "broker.clean_unused_topic",
            "broker.reset_master_flush_offset",
            "broker.cold_data_flow_ctr_update",
            "broker.cold_data_flow_ctr_remove",
            "broker.commit_log_read_ahead",
            "broker.switch_timer_engine",
            "consumer.update_subscription_group",
            "consumer.update_subscription_group_list",
            "lite.trigger_dispatch",
            "static_topic.update",
            "static_topic.remap",
        ] {
            assert!(ids.contains(expected), "missing phase 3 command {expected}");
        }
    }

    #[test]
    fn phase_three_dangerous_commands_require_target_confirmation() {
        let catalog = command_catalog();
        let command = catalog.iter().find(|command| command.id == "auth.user.delete").unwrap();
        assert_eq!(command.risk_level, RiskLevel::Dangerous);

        let mut form = CommandFormState::for_command(command);
        form.set_value("username", "admin-user".to_string());
        assert_eq!(command.expected_confirmation(&form), Some("admin-user".to_string()));

        let command = catalog
            .iter()
            .find(|command| command.id == "controller.metadata.clean")
            .unwrap();
        assert_eq!(command.risk_level, RiskLevel::Dangerous);

        let mut form = CommandFormState::for_command(command);
        form.set_value("broker_name", "broker-a".to_string());
        assert_eq!(command.expected_confirmation(&form), Some("broker-a".to_string()));
    }

    #[test]
    fn phase_four_catalog_exposes_core_ready_complex_workflows() {
        let catalog = command_catalog();
        let ids = catalog.iter().map(|command| command.id).collect::<HashSet<_>>();

        for expected in [
            "producer.send_message",
            "message.dump_compaction_log",
            "message.print",
            "message.print_by_queue",
            "message.consume",
            "export.configs",
            "export.metadata",
            "export.metadata_rocksdb",
            "export.pop_record",
        ] {
            assert!(ids.contains(expected), "missing command {expected}");
        }
    }

    #[test]
    fn phase_four_message_workflows_have_local_or_limit_guardrails() {
        let catalog = command_catalog();
        let dump = catalog
            .iter()
            .find(|command| command.id == "message.dump_compaction_log")
            .unwrap();
        assert!(dump.args.iter().any(|arg| arg.name == "file" && arg.required));

        for command_id in ["message.print", "message.print_by_queue", "message.consume"] {
            let command = catalog.iter().find(|command| command.id == command_id).unwrap();
            assert_eq!(command.result_view_kind, ResultViewKind::Table);
            assert!(command.args.iter().any(|arg| arg.name == "max_events" && arg.required));
        }

        let send_message = catalog
            .iter()
            .find(|command| command.id == "producer.send_message")
            .unwrap();
        assert_eq!(send_message.risk_level, RiskLevel::Mutating);
        assert!(send_message.args.iter().any(|arg| arg.name == "body" && arg.required));

        let pop_record = catalog
            .iter()
            .find(|command| command.id == "export.pop_record")
            .unwrap();
        assert_eq!(pop_record.risk_level, RiskLevel::Mutating);
        assert_eq!(pop_record.confirmation_field, None);
    }

    #[test]
    fn phase_four_export_commands_include_file_output_controls() {
        let catalog = command_catalog();

        for command_id in [
            "export.configs",
            "export.metadata",
            "export.metadata_rocksdb",
            "export.pop_record",
        ] {
            let command = catalog.iter().find(|command| command.id == command_id).unwrap();
            assert!(command.args.iter().any(|arg| {
                arg.name == "output_path" && !arg.required && matches!(arg.kind, ArgKind::OptionalString { .. })
            }));
            assert!(command.args.iter().any(|arg| {
                arg.name == "overwrite" && !arg.required && matches!(arg.kind, ArgKind::Bool { default: false })
            }));
        }
    }
}
