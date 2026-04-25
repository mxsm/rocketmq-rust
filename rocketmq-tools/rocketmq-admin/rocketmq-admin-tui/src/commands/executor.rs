use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::bail;
use anyhow::Context;
use rocketmq_admin_core::core::topic::TopicTarget;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use serde::Serialize;

use super::CommandSpec;
use crate::admin_facade::TuiAdminFacade;
use crate::state::CommandFormState;
use crate::view_model::CommandResultViewModel;

type CommandExecutionFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<CommandResultViewModel>> + 'a>>;

pub fn execute_command_with_progress<'a, F>(
    facade: &'a TuiAdminFacade,
    spec: &'a CommandSpec,
    form: &'a CommandFormState,
    mut progress: F,
) -> CommandExecutionFuture<'a>
where
    F: FnMut(String) + 'a,
{
    match spec.id {
        "topic.list" => Box::pin(async move {
            let result = {
                let result = facade.query_topic_list(form.optional_string("cluster_name")).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "topic.cluster" => Box::pin(async move {
            let result = {
                let result = facade.query_topic_clusters(form.required_string("topic")?).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "topic.route" => Box::pin(async move {
            let result = {
                let result = facade.query_topic_route(form.required_string("topic")?).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "topic.status" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_topic_status(form.required_string("topic")?, form.optional_string("cluster_name"))
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "topic.update" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "topic.update_perm" => Box::pin(async move {
            let result = {
                let request = facade.update_topic_perm_request(
                    form.required_string("topic")?,
                    topic_target(form)?,
                    form.number_i32("perm")?,
                )?;
                let result = facade.update_topic_perm(request).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "topic.delete" => Box::pin(async move {
            let result = {
                let topic = form.required_string("topic")?;
                let result = facade
                    .delete_topic(topic.clone(), form.optional_string("cluster_name"))
                    .await?;
                CommandResultViewModel::operation_success(spec.title, vec![topic]).with_debug_tail(&result)
            };
            Ok(result)
        }),
        "topic.order_conf" => Box::pin(async move {
            let result = {
                let result = facade
                    .apply_order_conf(
                        form.required_string("topic")?,
                        form.enum_string("method")?,
                        form.optional_string("order_conf"),
                    )
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "topic.allocate_mq" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_allocated_mq(form.required_string("topic")?, form.required_string("ip_list")?)
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "namesrv.config.query" => Box::pin(async move {
            let result = {
                let result = facade.query_namesrv_config().await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "namesrv.config.update" => Box::pin(async move {
            let result = {
                let result = facade
                    .update_namesrv_config(form.required_string("key")?, form.required_string("value")?)
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "namesrv.kv.update" => Box::pin(async move {
            let result = {
                let result = facade
                    .update_kv_config(
                        form.required_string("namespace")?,
                        form.required_string("key")?,
                        form.required_string("value")?,
                    )
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "namesrv.kv.delete" => Box::pin(async move {
            let result = {
                let key = form.required_string("key")?;
                let result = facade
                    .delete_kv_config(form.required_string("namespace")?, key.clone())
                    .await?;
                CommandResultViewModel::operation_success(spec.title, vec![key]).with_debug_tail(&result)
            };
            Ok(result)
        }),
        "namesrv.write_perm.add" => Box::pin(async move {
            let result = {
                let result = facade.add_write_perm(form.required_string("broker_name")?).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "namesrv.write_perm.wipe" => Box::pin(async move {
            let result = {
                let result = facade.wipe_write_perm(form.required_string("broker_name")?).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "auth.user.get" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_auth_user(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.required_string("username")?,
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "auth.user.list" => Box::pin(async move {
            let result = {
                let result = facade
                    .list_auth_users(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.optional_string("filter"),
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "auth.user.create" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "auth.user.update" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "auth.user.delete" => Box::pin(async move {
            let result = {
                let username = form.required_string("username")?;
                let result = facade
                    .delete_auth_user(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        username.clone(),
                    )
                    .await?;
                CommandResultViewModel::auth_operation(spec.title, &result).with_debug_tail(&username)
            };
            Ok(result)
        }),
        "auth.user.copy" => Box::pin(async move {
            let result = {
                let result = facade
                    .copy_auth_users(
                        form.required_string("from_broker")?,
                        form.required_string("to_broker")?,
                        form.optional_string("usernames"),
                    )
                    .await?;
                CommandResultViewModel::auth_copy_users(spec.title, &result)
            };
            Ok(result)
        }),
        "auth.acl.get" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_auth_acl(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.required_string("subject")?,
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "auth.acl.list" => Box::pin(async move {
            let result = {
                let result = facade
                    .list_auth_acl(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.optional_string("subject_filter"),
                        form.optional_string("resource_filter"),
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "auth.acl.create" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "auth.acl.update" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "auth.acl.delete" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "auth.acl.copy" => Box::pin(async move {
            let result = {
                let result = facade
                    .copy_auth_acl(
                        form.required_string("from_broker")?,
                        form.required_string("to_broker")?,
                        form.optional_string("subjects"),
                    )
                    .await?;
                CommandResultViewModel::auth_copy_acl(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.config.query" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_broker_config(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.optional_string("key_pattern"),
                    )
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.config.update_plan" => Box::pin(async move {
            let result = {
                let request = broker_config_update_request(facade, form)?;
                let result = facade.build_broker_config_update_plan(request).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.config.update_apply" => Box::pin(async move {
            let result = {
                let request = broker_config_update_request(facade, form)?;
                let result = facade.apply_broker_config_update(request).await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.runtime_stats" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_broker_runtime_stats(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                    )
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.consume_stats" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_broker_consume_stats(
                        form.required_string("broker_addr")?,
                        form.number_u64("timeout_millis")?,
                        form.number_i64("diff_level")?,
                        form.bool_value("is_order")?,
                    )
                    .await?;
                CommandResultViewModel::broker_consume_stats(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.epoch" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_broker_epoch(
                        form.optional_string("broker_name"),
                        form.optional_string("cluster_name"),
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.cold_data_flow_ctr_info" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_cold_data_flow_ctr_info(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.clean_expired_cq" => Box::pin(async move {
            let result = {
                let result = facade
                    .clean_expired_consume_queue(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.optional_string("topic"),
                        form.bool_value("dry_run")?,
                    )
                    .await?;
                CommandResultViewModel::clean_expired_consume_queue(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.delete_expired_commit_log" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "broker.clean_unused_topic" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "broker.reset_master_flush_offset" => Box::pin(async move {
            let result = {
                let broker_addr = form.required_string("broker_addr")?;
                facade
                    .reset_master_flush_offset(Some(broker_addr.clone()), Some(form.number_i64("offset")?))
                    .await?;
                CommandResultViewModel::operation_success(spec.title, vec![broker_addr])
            };
            Ok(result)
        }),
        "broker.cold_data_flow_ctr_update" => Box::pin(async move {
            let result = {
                let result = facade
                    .update_cold_data_flow_ctr_group_config(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.required_string("consumer_group")?,
                        form.required_string("threshold")?,
                    )
                    .await?;
                CommandResultViewModel::broker_operation(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.cold_data_flow_ctr_remove" => Box::pin(async move {
            let result = {
                let result = facade
                    .remove_cold_data_flow_ctr_group_config(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.required_string("consumer_group")?,
                    )
                    .await?;
                CommandResultViewModel::broker_operation(spec.title, &result)
            };
            Ok(result)
        }),
        "broker.commit_log_read_ahead" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "broker.switch_timer_engine" => Box::pin(async move {
            let result = {
                let result = facade
                    .switch_timer_engine(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                        form.enum_string("engine_type")?,
                    )
                    .await?;
                CommandResultViewModel::broker_operation(spec.title, &result)
            };
            Ok(result)
        }),
        "cluster.list" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_cluster_list(form.bool_value("more_stats")?, form.optional_string("cluster_name"))
                    .await?;
                CommandResultViewModel::cluster_list(spec.title, &result)
            };
            Ok(result)
        }),
        "cluster.broker_names" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "cluster.send_message_rt" => Box::pin(async move {
            let result = {
                let result = facade
                    .check_cluster_send_message_rt(
                        form.number_u64("amount")?,
                        form.number_u64("size")?,
                        form.optional_string("cluster_name"),
                    )
                    .await?;
                CommandResultViewModel::cluster_send_message_rt(spec.title, &result)
            };
            Ok(result)
        }),
        "controller.config.query" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_controller_config(form.required_string("controller_address")?)
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "controller.config.update" => Box::pin(async move {
            let result = {
                let controller_address = form.required_string("controller_address")?;
                facade
                    .update_controller_config(
                        controller_address.clone(),
                        form.required_string("key")?,
                        form.required_string("value")?,
                    )
                    .await?;
                CommandResultViewModel::operation_success(spec.title, vec![controller_address])
            };
            Ok(result)
        }),
        "controller.metadata.query" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_controller_metadata(form.required_string("controller_address")?)
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "controller.elect_master" => Box::pin(async move {
            let result = {
                let result = facade
                    .elect_controller_master(
                        form.required_string("controller_address")?,
                        form.required_string("cluster_name")?,
                        form.required_string("broker_name")?,
                        form.number_i64("broker_id")?,
                    )
                    .await?;
                CommandResultViewModel::controller_elect_master(spec.title, &result)
            };
            Ok(result)
        }),
        "controller.metadata.clean" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "container.add_broker" => Box::pin(async move {
            let result = {
                let result = facade
                    .add_broker_to_container(
                        form.required_string("broker_container_addr")?,
                        form.required_string("broker_config_path")?,
                    )
                    .await?;
                CommandResultViewModel::operation_success(
                    spec.title,
                    vec![format!(
                        "{} config {}",
                        result.broker_container_addr.as_str(),
                        result.target.as_str()
                    )],
                )
            };
            Ok(result)
        }),
        "container.remove_broker" => Box::pin(async move {
            let result = {
                let result = facade
                    .remove_broker_from_container(
                        form.required_string("broker_container_addr")?,
                        form.required_string("cluster_name")?,
                        form.required_string("broker_name")?,
                        form.number_i64("broker_id")?,
                    )
                    .await?;
                CommandResultViewModel::operation_success(
                    spec.title,
                    vec![format!(
                        "{} broker {}",
                        result.broker_container_addr.as_str(),
                        result.target.as_str()
                    )],
                )
            };
            Ok(result)
        }),
        "connection.consumer" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_consumer_connection(
                        form.required_string("consumer_group")?,
                        form.optional_string("broker_addr"),
                    )
                    .await?;
                CommandResultViewModel::consumer_connection(spec.title, &result)
            };
            Ok(result)
        }),
        "connection.producer" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_producer_connection(form.required_string("producer_group")?, form.required_string("topic")?)
                    .await?;
                CommandResultViewModel::producer_connection(spec.title, &result)
            };
            Ok(result)
        }),
        "consumer.config" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_consumer_config(form.required_string("group_name")?)
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "consumer.running_info" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_consumer_running_info(
                        form.required_string("group_name")?,
                        form.optional_string("client_id"),
                        form.optional_string("broker_addr"),
                        form.bool_value("jstack")?,
                    )
                    .await?;
                CommandResultViewModel::consumer_running_info(spec.title, &result)
            };
            Ok(result)
        }),
        "consumer.progress" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_consumer_progress(
                        form.optional_string("consumer_group"),
                        form.optional_string("topic_name"),
                        form.bool_value("show_client_ip")?,
                        form.optional_string("cluster"),
                    )
                    .await?;
                CommandResultViewModel::consumer_progress(spec.title, &result)
            };
            Ok(result)
        }),
        "consumer.start_monitoring" => Box::pin(async move {
            let result = {
                let max_events =
                    usize::try_from(form.number_u64("max_events")?).context("max_events is out of range for usize")?;
                let result = facade
                    .start_monitoring_with_progress(
                        form.number_u32("round_count")?,
                        form.number_u64("round_interval_millis")?,
                        form.bool_value("include_undone_msgs")?,
                        form.bool_value("include_running_info")?,
                        max_events,
                        &mut progress,
                    )
                    .await?;
                CommandResultViewModel::consumer_monitoring(spec.title, &result)
            };
            Ok(result)
        }),
        "consumer.delete_subscription_group" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "consumer.set_consume_mode" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "consumer.update_subscription_group" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "consumer.update_subscription_group_list" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "offset.clone_group" => Box::pin(async move {
            let result = {
                facade
                    .clone_group_offset(
                        form.required_string("src_group")?,
                        form.required_string("dest_group")?,
                        form.required_string("topic")?,
                        form.bool_value("offline")?,
                    )
                    .await?;
                CommandResultViewModel::operation_success(spec.title, vec![form.required_string("dest_group")?])
            };
            Ok(result)
        }),
        "offset.consumer_status" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "offset.skip_accumulated" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "offset.reset_by_time" => Box::pin(async move {
            let result = {
                let group = form.required_string("group")?;
                let result = facade
                    .reset_offset_by_time(
                        group.clone(),
                        form.required_string("topic")?,
                        form.timestamp_millis("timestamp")?,
                    )
                    .await?;
                CommandResultViewModel::operation_success(spec.title, vec![group]).with_debug_tail(&result)
            };
            Ok(result)
        }),
        "offset.reset_by_time_old" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "queue.consume_queue" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "queue.rocksdb_cq_progress" => Box::pin(async move {
            let result = {
                let result = facade
                    .check_rocksdb_cq_write_progress(
                        form.required_string("cluster_name")?,
                        form.optional_string("topic"),
                        form.optional_i64("check_from")?,
                    )
                    .await?;
                CommandResultViewModel::rocksdb_cq_progress(spec.title, &result)
            };
            Ok(result)
        }),
        "ha.status" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_ha_status(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                    )
                    .await?;
                CommandResultViewModel::from_debug(spec.title, &result)
            };
            Ok(result)
        }),
        "ha.sync_state_set" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_sync_state_set(
                        form.required_string("controller_address")?,
                        form.optional_string("broker_name"),
                        form.optional_string("cluster_name"),
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "stats.all" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_stats_all(form.bool_value("active_topic")?, form.optional_string("topic"))
                    .await?;
                CommandResultViewModel::stats_all(spec.title, &result)
            };
            Ok(result)
        }),
        "producer.info" => Box::pin(async move {
            let result = {
                let result = facade.query_producer_info(form.required_string("broker_addr")?).await?;
                CommandResultViewModel::producer_info(spec.title, &result)
            };
            Ok(result)
        }),
        "producer.send_message" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "producer.send_message_status" => Box::pin(async move {
            let result = {
                let result = facade
                    .send_message_status(
                        form.required_string("broker_name")?,
                        usize::try_from(form.number_u64("message_size")?)?,
                        form.number_u32("count")?,
                    )
                    .await?;
                CommandResultViewModel::send_message_status(spec.title, &result)
            };
            Ok(result)
        }),
        "producer.check_message_send_rt" => Box::pin(async move {
            let result = {
                let result = facade
                    .check_message_send_rt(
                        form.required_string("topic")?,
                        form.number_u64("amount")?,
                        usize::try_from(form.number_u64("size")?)?,
                    )
                    .await?;
                CommandResultViewModel::check_message_send_rt(spec.title, &result)
            };
            Ok(result)
        }),
        "lite.broker_info" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_broker_lite_info(
                        form.optional_string("broker_addr"),
                        form.optional_string("cluster_name"),
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "lite.parent_topic_info" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_parent_topic_info(form.required_string("parent_topic")?)
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "lite.topic_info" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_lite_topic_info(
                        form.required_string("parent_topic")?,
                        form.required_string("lite_topic")?,
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "lite.group_info" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_lite_group_info(
                        form.required_string("parent_topic")?,
                        form.required_string("group")?,
                        form.optional_string("lite_topic"),
                        form.optional_i32("top_k")?,
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "lite.client_info" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_lite_client_info(
                        form.required_string("parent_topic")?,
                        form.required_string("group")?,
                        form.required_string("client_id")?,
                    )
                    .await?;
                CommandResultViewModel::from_serializable(spec.title, &result)
            };
            Ok(result)
        }),
        "lite.trigger_dispatch" => Box::pin(async move {
            let result = {
                let result = facade
                    .trigger_lite_dispatch(
                        form.required_string("parent_topic")?,
                        form.required_string("group")?,
                        form.optional_string("client_id"),
                        form.optional_string("broker_name"),
                    )
                    .await?;
                CommandResultViewModel::lite_trigger_dispatch(spec.title, &result)
            };
            Ok(result)
        }),
        "message.decode_id" => Box::pin(async move {
            let result = {
                let result = facade.decode_message_id(form.required_string("message_ids")?)?;
                CommandResultViewModel::decoded_message_ids(spec.title, &result)
            };
            Ok(result)
        }),
        "message.query_by_id" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_message_by_id(
                        form.required_string("message_ids")?,
                        form.optional_string("topic"),
                        form.number_u64("timeout_millis")?,
                    )
                    .await?;
                CommandResultViewModel::message_query_by_id(spec.title, &result)
            };
            Ok(result)
        }),
        "message.query_by_key" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "message.query_by_unique_key" => Box::pin(async move {
            let result = {
                let result = facade
                    .query_message_by_unique_key(
                        form.required_string("msg_id")?,
                        None,
                        None,
                        form.required_string("topic")?,
                        form.bool_value("show_all")?,
                        form.optional_string("cluster"),
                        form.optional_i64("start_time")?,
                        form.optional_i64("end_time")?,
                    )
                    .await?;
                CommandResultViewModel::message_query_by_unique_key(spec.title, &result)
            };
            Ok(result)
        }),
        "message.direct_consume" => Box::pin(async move {
            let result = {
                let result = facade
                    .direct_consume_message(
                        form.required_string("topic")?,
                        form.required_string("msg_id")?,
                        form.required_string("consumer_group")?,
                        form.required_string("client_id")?,
                        form.optional_string("cluster"),
                    )
                    .await?;
                CommandResultViewModel::direct_consume_message(spec.title, &result)
            };
            Ok(result)
        }),
        "message.track_by_id" => Box::pin(async move {
            let result = {
                let result = facade
                    .message_track(
                        form.required_string("message_ids")?,
                        form.required_string("topic")?,
                        form.optional_string("cluster"),
                        form.number_u64("timeout_millis")?,
                    )
                    .await?;
                CommandResultViewModel::message_track(spec.title, &result)
            };
            Ok(result)
        }),
        "message.query_by_offset" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "message.query_trace_by_id" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "message.dump_compaction_log" => Box::pin(async move {
            let result = {
                let result = facade.dump_compaction_log(Some(form.required_string("file")?))?;
                CommandResultViewModel::dump_compaction_log(spec.title, &result)
            };
            Ok(result)
        }),
        "message.print" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "message.print_by_queue" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "message.consume" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "export.configs" => Box::pin(async move {
            let result = {
                let result = facade.export_configs(form.required_string("cluster_name")?).await?;
                export_output_or_view(
                    facade,
                    spec.title,
                    form,
                    &result,
                    CommandResultViewModel::export_configs(spec.title, &result),
                )?
            };
            Ok(result)
        }),
        "export.metrics" => Box::pin(async move {
            let result = {
                let result = facade
                    .export_metrics(
                        form.required_string("cluster_name")?,
                        optional_u64_arg(form, "timeout_millis")?,
                    )
                    .await?;
                export_output_or_view(
                    facade,
                    spec.title,
                    form,
                    &result,
                    CommandResultViewModel::export_metrics(spec.title, &result),
                )?
            };
            Ok(result)
        }),
        "export.metadata" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "export.metadata_rocksdb" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "export.metadata_rocksdb_rpc" => Box::pin(async move {
            let result = {
                let result = facade
                    .export_rocksdb_config_rpc(
                        form.optional_string("cluster_name"),
                        form.optional_string("broker_addr"),
                        form.required_string("config_types")?,
                        optional_u64_arg(form, "timeout_millis")?,
                    )
                    .await?;
                CommandResultViewModel::export_rocksdb_config_rpc(spec.title, &result)
            };
            Ok(result)
        }),
        "export.pop_record" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "static_topic.update" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        "static_topic.remap" => Box::pin(async move {
            let result = {
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
            };
            Ok(result)
        }),
        unknown => Box::pin(async move { bail!("unknown command id: {unknown}") }),
    }
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
