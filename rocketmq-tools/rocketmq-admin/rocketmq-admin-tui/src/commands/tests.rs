use std::collections::BTreeMap;
use std::collections::HashSet;
use std::mem::size_of_val;

use super::command_catalog;
use super::execute_command_with_progress;
use super::ArgKind;
use super::CommandCategory;
use super::ResultViewKind;
use super::RiskLevel;
use crate::admin_facade::TuiAdminFacade;
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
        "consumer.start_monitoring",
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
        "controller.elect_master",
        "container.add_broker",
        "container.remove_broker",
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
        "message.direct_consume",
        "message.track_by_id",
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
fn executor_dispatch_future_stays_boxed_to_avoid_stack_growth() {
    let catalog = command_catalog();
    let command = catalog.iter().find(|command| command.id == "topic.list").unwrap();
    let facade = TuiAdminFacade::default();
    let form = CommandFormState::for_command(command);

    let future = execute_command_with_progress(&facade, command, &form, |_| {});

    assert!(size_of_val(&future) <= 32);
}

#[test]
fn phase_six_command_catalog_category_snapshot() {
    let catalog = command_catalog();
    let mut counts = BTreeMap::new();
    for command in &catalog {
        *counts.entry(command.category).or_insert(0) += 1;
        assert!(!command.id.trim().is_empty());
        assert!(!command.title.trim().is_empty());
        assert!(!command.description.trim().is_empty());
    }

    assert_eq!(catalog.len(), 102);
    assert_eq!(
        counts,
        BTreeMap::from([
            (CommandCategory::Auth, 12),
            (CommandCategory::Broker, 15),
            (CommandCategory::Cluster, 3),
            (CommandCategory::Connection, 2),
            (CommandCategory::Consumer, 8),
            (CommandCategory::Container, 2),
            (CommandCategory::Controller, 5),
            (CommandCategory::Export, 6),
            (CommandCategory::Ha, 2),
            (CommandCategory::Lite, 6),
            (CommandCategory::Message, 12),
            (CommandCategory::NameServer, 6),
            (CommandCategory::Offset, 5),
            (CommandCategory::Producer, 4),
            (CommandCategory::Queue, 2),
            (CommandCategory::StaticTopic, 2),
            (CommandCategory::Stats, 1),
            (CommandCategory::Topic, 9),
        ])
    );
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

    let command = catalog
        .iter()
        .find(|command| command.id == "controller.elect_master")
        .unwrap();
    assert_eq!(command.risk_level, RiskLevel::Dangerous);

    let mut form = CommandFormState::for_command(command);
    form.set_value("broker_name", "broker-a".to_string());
    assert_eq!(command.expected_confirmation(&form), Some("broker-a".to_string()));

    let command = catalog
        .iter()
        .find(|command| command.id == "container.remove_broker")
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
        "export.metrics",
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
        "export.metrics",
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

#[test]
fn phase_five_catalog_exposes_bounded_monitoring_command() {
    let catalog = command_catalog();
    let command = catalog
        .iter()
        .find(|command| command.id == "consumer.start_monitoring")
        .unwrap();

    assert_eq!(command.risk_level, RiskLevel::Safe);
    assert_eq!(command.result_view_kind, ResultViewKind::Table);
    assert!(command.args.iter().any(|arg| {
        arg.name == "round_count"
            && arg.required
            && matches!(
                arg.kind,
                ArgKind::Number {
                    default: Some(1),
                    min: Some(1)
                }
            )
    }));
    assert!(command.args.iter().any(|arg| arg.name == "max_events" && arg.required));
}

#[test]
fn phase_five_catalog_exposes_rocksdb_consumer_offsets() {
    let catalog = command_catalog();
    let command = catalog
        .iter()
        .find(|command| command.id == "export.metadata_rocksdb")
        .unwrap();
    let config_type = command.args.iter().find(|arg| arg.name == "config_type").unwrap();

    assert!(matches!(
        &config_type.kind,
        ArgKind::Enum {
            values,
            ..
        } if values.contains(&"consumerOffsets")
    ));

    let command = catalog
        .iter()
        .find(|command| command.id == "export.metadata_rocksdb_rpc")
        .expect("rocksdb rpc command");
    assert_eq!(command.risk_level, RiskLevel::Mutating);
    assert_eq!(command.result_view_kind, ResultViewKind::OperationSummary);
    assert!(command
        .args
        .iter()
        .any(|arg| arg.name == "config_types" && arg.required));
}

#[test]
fn phase_five_catalog_exposes_export_metrics() {
    let catalog = command_catalog();
    let command = catalog.iter().find(|command| command.id == "export.metrics").unwrap();

    assert_eq!(command.risk_level, RiskLevel::Safe);
    assert_eq!(command.result_view_kind, ResultViewKind::Table);
    assert!(command
        .args
        .iter()
        .any(|arg| arg.name == "cluster_name" && arg.required));
    assert!(command.args.iter().any(|arg| {
        arg.name == "timeout_millis"
            && !arg.required
            && matches!(
                arg.kind,
                ArgKind::Number {
                    default: Some(10000),
                    min: Some(1)
                }
            )
    }));
    assert!(command
        .args
        .iter()
        .any(|arg| arg.name == "output_path" && !arg.required));
}

#[test]
fn phase_five_catalog_exposes_controller_elect_master() {
    let catalog = command_catalog();
    let command = catalog
        .iter()
        .find(|command| command.id == "controller.elect_master")
        .unwrap();

    assert_eq!(command.risk_level, RiskLevel::Dangerous);
    assert_eq!(command.result_view_kind, ResultViewKind::Table);
    assert_eq!(command.confirmation_field, Some("broker_name"));
    assert!(command
        .args
        .iter()
        .any(|arg| arg.name == "controller_address" && arg.required));
    assert!(command
        .args
        .iter()
        .any(|arg| arg.name == "cluster_name" && arg.required));
    assert!(command.args.iter().any(|arg| arg.name == "broker_name" && arg.required));
    assert!(command.args.iter().any(|arg| {
        arg.name == "broker_id"
            && arg.required
            && matches!(
                arg.kind,
                ArgKind::Number {
                    default: Some(1),
                    min: Some(0)
                }
            )
    }));
}

#[test]
fn phase_five_catalog_exposes_container_broker_commands() {
    let catalog = command_catalog();
    let ids = catalog.iter().map(|command| command.id).collect::<HashSet<_>>();

    assert!(ids.contains("container.add_broker"));
    assert!(ids.contains("container.remove_broker"));

    let command = catalog
        .iter()
        .find(|command| command.id == "container.add_broker")
        .expect("container add broker command");
    assert_eq!(command.category, CommandCategory::Container);
    assert_eq!(command.risk_level, RiskLevel::Dangerous);
    assert_eq!(command.confirmation_field, Some("broker_container_addr"));
    assert!(command
        .args
        .iter()
        .any(|arg| arg.name == "broker_config_path" && arg.required));
}

#[test]
fn phase_five_catalog_exposes_message_direct_consume_and_track_commands() {
    let catalog = command_catalog();

    let direct = catalog
        .iter()
        .find(|command| command.id == "message.direct_consume")
        .expect("direct consume command");
    assert_eq!(direct.risk_level, RiskLevel::Mutating);
    assert_eq!(direct.result_view_kind, ResultViewKind::Table);
    assert!(direct
        .args
        .iter()
        .any(|arg| arg.name == "consumer_group" && arg.required));
    assert!(direct.args.iter().any(|arg| arg.name == "client_id" && arg.required));

    let track = catalog
        .iter()
        .find(|command| command.id == "message.track_by_id")
        .expect("message track command");
    assert_eq!(track.risk_level, RiskLevel::Safe);
    assert_eq!(track.result_view_kind, ResultViewKind::Table);
    assert!(track.args.iter().any(|arg| arg.name == "message_ids" && arg.required));
    assert!(track
        .args
        .iter()
        .any(|arg| arg.name == "timeout_millis" && arg.required));
}
