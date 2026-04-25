use super::TuiAdminFacade;
use rocketmq_admin_core::core::consumer::MonitoringEvent;
use rocketmq_admin_core::core::message::MessagePullEvent;

#[test]
fn facade_builds_topic_cluster_request_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
    let request = facade.topic_cluster_request(" TestTopic ").unwrap();

    assert_eq!(request.topic().as_str(), "TestTopic");
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn facade_builds_topic_route_request_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");
    let request = facade.topic_route_request(" RouteTopic ").unwrap();

    assert_eq!(request.topic().as_str(), "RouteTopic");
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn facade_builds_topic_status_request_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");
    let request = facade
        .topic_status_request(" StatusTopic ", Some(" DefaultCluster ".to_string()))
        .unwrap();

    assert_eq!(request.topic().as_str(), "StatusTopic");
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    assert_eq!(
        request.cluster_name().map(|value| value.as_str()),
        Some("DefaultCluster")
    );
}

#[test]
fn facade_builds_additional_topic_requests_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    assert_eq!(
        facade
            .topic_list_request(Some(" DefaultCluster ".to_string()))
            .cluster_name()
            .map(|value| value.as_str()),
        Some("DefaultCluster")
    );
    assert_eq!(
        facade
            .delete_topic_request(" TestTopic ", Some(" DefaultCluster ".to_string()))
            .unwrap()
            .cluster_name()
            .as_str(),
        "DefaultCluster"
    );
    assert_eq!(
        facade
            .order_conf_request(" TestTopic ", "put", Some(" broker-a:4 ".to_string()))
            .unwrap()
            .order_conf(),
        Some("broker-a:4")
    );
    assert_eq!(
        facade
            .allocate_mq_request(" TestTopic ", " 192.168.1.1 ")
            .unwrap()
            .ip_list()
            .as_str(),
        "192.168.1.1"
    );
}

#[test]
fn facade_builds_update_topic_requests_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    let update_topic = facade
        .update_topic_request(
            " TestTopic ",
            rocketmq_admin_core::core::topic::TopicTarget::Broker("127.0.0.1:10911".into()),
            8,
            8,
            Some(6),
            Some(false),
            Some(false),
            Some(false),
        )
        .unwrap();
    assert_eq!(update_topic.config().topic_name.as_str(), "TestTopic");

    let update_perm = facade
        .update_topic_perm_request(
            " TestTopic ",
            rocketmq_admin_core::core::topic::TopicTarget::Cluster("DefaultCluster".into()),
            6,
        )
        .unwrap();
    assert_eq!(update_perm.perm(), 6);
}

#[test]
fn facade_builds_namesrv_requests_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876;127.0.0.2:9876 ");

    assert_eq!(facade.namesrv_config_query_request().unwrap().namesrv_addrs().len(), 2);
    let update_config = facade.namesrv_config_update_request(" deleteWhen ", " 04 ").unwrap();
    assert!(update_config
        .properties()
        .iter()
        .any(|(key, value)| key.as_str() == "deleteWhen" && value.as_str() == "04"));
    assert_eq!(
        facade
            .kv_config_update_request(" ns ", " key ", " value ")
            .unwrap()
            .namespace()
            .as_str(),
        "ns"
    );
    assert_eq!(
        facade.kv_config_delete_request(" ns ", " key ").unwrap().key().as_str(),
        "key"
    );
    assert_eq!(
        facade.write_perm_request(" broker-a ").unwrap().broker_name().as_str(),
        "broker-a"
    );
}

#[test]
fn facade_builds_broker_config_request_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
    let request = facade
        .broker_config_query_request(
            None,
            Some(" DefaultCluster ".to_string()),
            Some(" ^flush.* ".to_string()),
        )
        .unwrap();

    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    assert_eq!(request.key_pattern(), Some("^flush.*"));
    assert!(matches!(
        request.target(),
        rocketmq_admin_core::core::broker::BrokerTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"
    ));

    let mut entries = std::collections::BTreeMap::new();
    entries.insert(" flushDiskType ".to_string(), " ASYNC_FLUSH ".to_string());
    let update_request = facade
        .broker_config_update_request(Some(" 127.0.0.1:10911 ".to_string()), None, entries, false)
        .unwrap();
    assert_eq!(update_request.namesrv_addr(), Some("127.0.0.1:9876"));
    assert!(!update_request.rollback_enabled());
    assert!(matches!(
        update_request.target(),
        rocketmq_admin_core::core::broker::BrokerTarget::BrokerAddr(addr) if addr.as_str() == "127.0.0.1:10911"
    ));
}

#[test]
fn facade_builds_broker_runtime_stats_request_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
    let request = facade
        .broker_runtime_stats_request(None, Some(" DefaultCluster ".to_string()))
        .unwrap();

    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
    assert!(matches!(
        request.target(),
        rocketmq_admin_core::core::broker::BrokerTarget::ClusterName(cluster) if cluster.as_str() == "DefaultCluster"
    ));
}

#[test]
fn facade_builds_broker_consume_stats_request_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");
    let request = facade
        .broker_consume_stats_request(" 127.0.0.1:10911 ", 3_000, 42, true)
        .unwrap();

    assert_eq!(request.broker_addr().as_str(), "127.0.0.1:10911");
    assert_eq!(request.timeout_millis(), 3_000);
    assert_eq!(request.diff_level(), 42);
    assert!(request.is_order());
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn facade_exposes_topic_service_futures_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    std::mem::drop(facade.query_topic_clusters("TestTopic"));
    std::mem::drop(facade.query_topic_route("TestTopic"));
    std::mem::drop(facade.query_topic_status("TestTopic", Some("DefaultCluster".to_string())));
    std::mem::drop(facade.query_topic_list(Some("DefaultCluster".to_string())));
    std::mem::drop(facade.delete_topic("TestTopic", Some("DefaultCluster".to_string())));
    std::mem::drop(facade.apply_order_conf("TestTopic", "get", None));
    std::mem::drop(facade.query_allocated_mq("TestTopic", "192.168.1.1"));

    let update_topic = facade
        .update_topic_request(
            "TestTopic",
            rocketmq_admin_core::core::topic::TopicTarget::Broker("127.0.0.1:10911".into()),
            8,
            8,
            Some(6),
            Some(false),
            Some(false),
            Some(false),
        )
        .unwrap();
    std::mem::drop(facade.create_or_update_topic(update_topic));

    let update_perm = facade
        .update_topic_perm_request(
            "TestTopic",
            rocketmq_admin_core::core::topic::TopicTarget::Cluster("DefaultCluster".into()),
            6,
        )
        .unwrap();
    std::mem::drop(facade.update_topic_perm(update_perm));
}

#[test]
fn facade_exposes_namesrv_service_futures_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    std::mem::drop(facade.query_namesrv_config());
    std::mem::drop(facade.update_namesrv_config("deleteWhen", "04"));
    std::mem::drop(facade.update_kv_config("ns", "key", "value"));
    std::mem::drop(facade.delete_kv_config("ns", "key"));
    std::mem::drop(facade.add_write_perm("broker-a"));
    std::mem::drop(facade.wipe_write_perm("broker-a"));
}

#[test]
fn facade_exposes_broker_service_futures_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    std::mem::drop(facade.query_broker_config(None, Some("DefaultCluster".to_string()), Some("^flush.*".to_string())));
    std::mem::drop(facade.query_broker_runtime_stats(None, Some("DefaultCluster".to_string())));
    std::mem::drop(facade.query_broker_consume_stats("127.0.0.1:10911", 3_000, 0, false));

    let mut entries = std::collections::BTreeMap::new();
    entries.insert("flushDiskType".to_string(), "ASYNC_FLUSH".to_string());
    let update_request = facade
        .broker_config_update_request(Some("127.0.0.1:10911".to_string()), None, entries, true)
        .unwrap();
    std::mem::drop(facade.build_broker_config_update_plan(update_request.clone()));
    std::mem::drop(facade.apply_broker_config_update(update_request));
}

#[test]
fn facade_builds_operational_requests_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");

    let cluster_list = facade.cluster_list_request(true, Some(" DefaultCluster ".to_string()));
    assert_eq!(cluster_list.namesrv_addr(), Some("127.0.0.1:9876"));
    assert_eq!(
        cluster_list.cluster_name().map(|value| value.as_str()),
        Some("DefaultCluster")
    );

    let consumer_connection = facade
        .consumer_connection_request(" GroupA ", Some(" 127.0.0.1:10911 ".to_string()))
        .unwrap();
    assert_eq!(consumer_connection.consumer_group().as_str(), "GroupA");
    assert_eq!(consumer_connection.broker_addr(), Some("127.0.0.1:10911"));

    let progress = facade
        .consumer_progress_request(
            Some(" GroupA ".to_string()),
            Some(" TopicA ".to_string()),
            true,
            Some(" DefaultCluster ".to_string()),
        )
        .unwrap();
    assert_eq!(progress.consumer_group().unwrap().as_str(), "GroupA");
    assert!(progress.show_client_ip());

    let monitoring = facade
        .start_monitoring_request(2, 1_000, true, false, Some(32))
        .unwrap();
    assert_eq!(monitoring.namesrv_addr(), Some("127.0.0.1:9876"));
    assert_eq!(monitoring.round_count(), 2);
    assert_eq!(monitoring.round_interval_millis(), 1_000);
    assert!(monitoring.include_undone_msgs());
    assert!(!monitoring.include_running_info());
    assert_eq!(monitoring.max_events(), Some(32));

    let reset = facade
        .reset_offset_by_time_request(" GroupA ", " TopicA ", 1234)
        .unwrap();
    assert_eq!(reset.group().as_str(), "GroupA");
    assert_eq!(reset.namesrv_addr(), Some("127.0.0.1:9876"));

    let queue = facade
        .query_consume_queue_request(
            " TopicA ",
            1,
            10,
            20,
            Some(" 127.0.0.1:10911 ".to_string()),
            Some(" GroupA ".to_string()),
        )
        .unwrap();
    assert_eq!(queue.topic().as_str(), "TopicA");
    assert_eq!(queue.namesrv_addr(), Some("127.0.0.1:9876"));

    let ha = facade
        .ha_status_request(Some(" 127.0.0.1:10911 ".to_string()), None)
        .unwrap();
    assert_eq!(ha.namesrv_addr(), Some("127.0.0.1:9876"));

    let send_status = facade.send_message_status_request(" broker-a ", 128, 2).unwrap();
    assert_eq!(send_status.broker_name().as_str(), "broker-a");
    assert_eq!(send_status.message_size(), 128);
    assert_eq!(send_status.count(), 2);

    let send_message = facade
        .send_message_request(
            " TopicA ",
            " body ",
            Some(" key ".to_string()),
            Some(" tag ".to_string()),
            Some(" broker-a ".to_string()),
            Some(1),
            true,
        )
        .unwrap();
    assert_eq!(send_message.topic().as_str(), "TopicA");
    assert_eq!(send_message.namesrv_addr(), Some("127.0.0.1:9876"));

    let send_rt = facade.check_message_send_rt_request(" TopicA ", 2, 128).unwrap();
    assert_eq!(send_rt.topic().as_str(), "TopicA");
    assert_eq!(send_rt.amount(), 2);
    assert_eq!(send_rt.size(), 128);
}

#[test]
fn facade_exposes_operational_service_futures_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    std::mem::drop(facade.query_cluster_list(false, Some("DefaultCluster".to_string())));
    std::mem::drop(facade.query_cluster_broker_names(Some("DefaultCluster".to_string())));
    std::mem::drop(facade.check_cluster_send_message_rt(2, 128, Some("DefaultCluster".to_string())));
    std::mem::drop(facade.query_consumer_connection("GroupA", Some("127.0.0.1:10911".to_string())));
    std::mem::drop(facade.query_producer_connection("ProducerGroupA", "TopicA"));
    std::mem::drop(facade.query_consumer_config("GroupA"));
    std::mem::drop(facade.query_consumer_running_info(
        "GroupA",
        Some("client-a".to_string()),
        Some("127.0.0.1:10911".to_string()),
        false,
    ));
    std::mem::drop(facade.query_consumer_progress(
        Some("GroupA".to_string()),
        Some("TopicA".to_string()),
        false,
        Some("DefaultCluster".to_string()),
    ));
    std::mem::drop(facade.start_monitoring_with_progress(1, 1_000, true, true, 16, |_| {}));
    std::mem::drop(facade.clone_group_offset("SourceGroup", "DestGroup", "TopicA", false));
    std::mem::drop(facade.query_consumer_status("GroupA", "TopicA", Some("client-a".to_string())));
    std::mem::drop(facade.skip_accumulated_message("GroupA", "TopicA", Some("DefaultCluster".to_string()), Some(true)));
    std::mem::drop(facade.reset_offset_by_time("GroupA", "TopicA", 1234));
    std::mem::drop(facade.reset_offset_by_time_old(
        "GroupA",
        "TopicA",
        1234,
        Some(true),
        Some("DefaultCluster".to_string()),
    ));
    std::mem::drop(facade.query_consume_queue(
        "TopicA",
        1,
        10,
        20,
        Some("127.0.0.1:10911".to_string()),
        Some("GroupA".to_string()),
    ));
    std::mem::drop(facade.check_rocksdb_cq_write_progress("DefaultCluster", Some("TopicA".to_string()), Some(1000)));
    std::mem::drop(facade.query_ha_status(Some("127.0.0.1:10911".to_string()), None));
    std::mem::drop(facade.query_sync_state_set("127.0.0.1:9878", Some("broker-a".to_string()), None));
    std::mem::drop(facade.query_stats_all(false, Some("TopicA".to_string())));
    std::mem::drop(facade.query_producer_info("127.0.0.1:10911"));
    std::mem::drop(facade.send_message(
        "TopicA",
        "body",
        Some("key".to_string()),
        Some("tag".to_string()),
        None,
        None,
        false,
    ));
    std::mem::drop(facade.send_message_status("broker-a", 128, 2));
    std::mem::drop(facade.check_message_send_rt("TopicA", 2, 128));
}

#[test]
fn facade_builds_phase_three_mutating_requests_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");

    let create_user = facade
        .auth_create_user_request(
            Some(" 127.0.0.1:10911 ".to_string()),
            None,
            " admin ",
            " secret ",
            Some(" NORMAL ".to_string()),
        )
        .unwrap();
    assert_eq!(create_user.username().as_str(), "admin");
    assert_eq!(create_user.password().as_str(), "secret");
    assert_eq!(create_user.namesrv_addr(), Some("127.0.0.1:9876"));

    let update_user = facade
        .auth_update_user_request(
            None,
            Some(" DefaultCluster ".to_string()),
            " admin ",
            None,
            Some(" SUPER ".to_string()),
            None,
        )
        .unwrap();
    assert_eq!(update_user.username().as_str(), "admin");
    assert_eq!(update_user.user_type(), Some("SUPER"));

    let delete_user = facade
        .auth_delete_user_request(Some(" 127.0.0.1:10911 ".to_string()), None, " admin ")
        .unwrap();
    assert_eq!(delete_user.username().as_str(), "admin");

    let copy_users = facade
        .auth_copy_users_request(
            " 127.0.0.1:10911 ",
            " 127.0.0.2:10911 ",
            Some(" admin,guest ".to_string()),
        )
        .unwrap();
    assert_eq!(copy_users.from_broker().as_str(), "127.0.0.1:10911");
    assert_eq!(copy_users.usernames().map(|names| names.len()), Some(2));

    let create_acl = facade
        .auth_create_acl_request(
            Some(" 127.0.0.1:10911 ".to_string()),
            None,
            " User:admin ",
            " Topic:TopicA,Group:GroupA ",
            " PUB,SUB ",
            " ALLOW ",
            Some(" 127.0.0.1 ".to_string()),
        )
        .unwrap();
    assert_eq!(create_acl.subject().as_str(), "User:admin");
    assert_eq!(create_acl.resources().len(), 2);
    assert_eq!(create_acl.actions().len(), 2);
    assert_eq!(create_acl.namesrv_addr(), Some("127.0.0.1:9876"));

    let delete_acl = facade
        .auth_delete_acl_request(
            None,
            Some(" DefaultCluster ".to_string()),
            " User:admin ",
            Some(" Topic:TopicA ".to_string()),
        )
        .unwrap();
    assert_eq!(delete_acl.subject().as_str(), "User:admin");
    assert_eq!(delete_acl.resource(), Some("Topic:TopicA"));

    let controller_update = facade
        .controller_config_update_request(" 127.0.0.1:9878 ", " enableElectUncleanMaster ", " true ")
        .unwrap();
    assert_eq!(controller_update.controller_servers().len(), 1);
    assert_eq!(controller_update.properties().len(), 1);

    let controller_elect = facade
        .controller_elect_master_request(" 127.0.0.1:9878 ", " DefaultCluster ", " broker-a ", 1)
        .unwrap();
    assert_eq!(controller_elect.controller_addr().as_str(), "127.0.0.1:9878");
    assert_eq!(controller_elect.cluster_name().as_str(), "DefaultCluster");
    assert_eq!(controller_elect.broker_name().as_str(), "broker-a");
    assert_eq!(controller_elect.broker_id(), 1);

    let controller_clean = facade
        .controller_metadata_clean_request(
            " 127.0.0.1:9878 ",
            " broker-a ",
            Some(" 1;2 ".to_string()),
            Some(" DefaultCluster ".to_string()),
            false,
        )
        .unwrap();
    assert_eq!(controller_clean.broker_name().as_str(), "broker-a");
    assert_eq!(
        controller_clean.broker_controller_ids_to_clean().unwrap().as_str(),
        "1;2"
    );

    let clean_cq = facade
        .clean_expired_consume_queue_request(
            Some(" 127.0.0.1:10911 ".to_string()),
            None,
            Some(" TopicA ".to_string()),
            true,
        )
        .unwrap();
    assert!(clean_cq.dry_run());
    assert_eq!(clean_cq.topic().unwrap().as_str(), "TopicA");

    let reset_flush = facade
        .reset_master_flush_offset_request(Some(" 127.0.0.1:10912 ".to_string()), Some(1024))
        .unwrap();
    assert_eq!(reset_flush.broker_addr().as_str(), "127.0.0.1:10912");
    assert_eq!(reset_flush.master_flush_offset(), 1024);

    let read_ahead = facade
        .commit_log_read_ahead_request(
            Some(" 127.0.0.1:10911 ".to_string()),
            None,
            Some("0".to_string()),
            false,
            false,
            Some("4096".to_string()),
            None,
            false,
        )
        .unwrap();
    assert!(read_ahead.has_updates());

    let lite = facade
        .trigger_lite_dispatch_request(
            " ParentTopic ",
            " GroupA ",
            Some(" client-a ".to_string()),
            Some(" broker-a ".to_string()),
        )
        .unwrap();
    assert_eq!(lite.parent_topic().as_str(), "ParentTopic");
    assert_eq!(lite.group().as_str(), "GroupA");

    let update_static = facade
        .update_static_topic_request(
            " StaticTopic ",
            " broker-a,broker-b ",
            " 4 ",
            Some(" DefaultCluster ".into()),
        )
        .unwrap();
    assert_eq!(update_static.topic().as_str(), "StaticTopic");
    assert_eq!(update_static.broker_names().len(), 2);

    let remap_static = facade
        .remapping_static_topic_request(
            " StaticTopic ",
            Some(" broker-a ".to_string()),
            Some(" DefaultCluster ".to_string()),
            Some(true),
        )
        .unwrap();
    assert_eq!(remap_static.topic().as_str(), "StaticTopic");
    assert!(remap_static.force_replace());

    let container_add = facade
        .container_add_broker_request(" 127.0.0.1:10911 ", " /tmp/broker.conf ")
        .unwrap();
    assert_eq!(container_add.broker_container_addr().as_str(), "127.0.0.1:10911");
    assert_eq!(container_add.broker_config_path().as_str(), "/tmp/broker.conf");
    assert_eq!(container_add.namesrv_addr(), Some("127.0.0.1:9876"));

    let container_remove = facade
        .container_remove_broker_request(" 127.0.0.1:10911 ", " DefaultCluster ", " broker-a ", 1)
        .unwrap();
    assert_eq!(container_remove.broker_container_addr().as_str(), "127.0.0.1:10911");
    assert_eq!(container_remove.broker_identity(), "DefaultCluster:broker-a:1");
}

#[test]
fn facade_exposes_phase_three_service_futures_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    std::mem::drop(facade.create_auth_user(
        Some("127.0.0.1:10911".to_string()),
        None,
        "admin",
        "secret",
        Some("NORMAL".to_string()),
    ));
    std::mem::drop(facade.update_auth_user(
        None,
        Some("DefaultCluster".to_string()),
        "admin",
        None,
        Some("SUPER".to_string()),
        None,
    ));
    std::mem::drop(facade.delete_auth_user(Some("127.0.0.1:10911".to_string()), None, "admin"));
    std::mem::drop(facade.copy_auth_users("127.0.0.1:10911", "127.0.0.2:10911", Some("admin".to_string())));
    std::mem::drop(facade.create_auth_acl(
        Some("127.0.0.1:10911".to_string()),
        None,
        "User:admin",
        "Topic:TopicA",
        "PUB",
        "ALLOW",
        None,
    ));
    std::mem::drop(facade.update_auth_acl(
        Some("127.0.0.1:10911".to_string()),
        None,
        "User:admin",
        "Topic:TopicA",
        "PUB",
        "ALLOW",
        None,
    ));
    std::mem::drop(facade.delete_auth_acl(
        Some("127.0.0.1:10911".to_string()),
        None,
        "User:admin",
        Some("Topic:TopicA".to_string()),
    ));
    std::mem::drop(facade.copy_auth_acl("127.0.0.1:10911", "127.0.0.2:10911", Some("User:admin".to_string())));
    std::mem::drop(facade.update_controller_config("127.0.0.1:9878", "enableElectUncleanMaster", "true"));
    std::mem::drop(facade.clean_controller_metadata(
        "127.0.0.1:9878",
        "broker-a",
        Some("1;2".to_string()),
        Some("DefaultCluster".to_string()),
        false,
    ));
    std::mem::drop(facade.add_broker_to_container("127.0.0.1:10911", "/tmp/broker.conf"));
    std::mem::drop(facade.remove_broker_from_container("127.0.0.1:10911", "DefaultCluster", "broker-a", 1));
    std::mem::drop(facade.clean_expired_consume_queue(
        Some("127.0.0.1:10911".to_string()),
        None,
        Some("TopicA".to_string()),
        true,
    ));
    std::mem::drop(facade.delete_expired_commit_log(Some("127.0.0.1:10911".to_string()), None));
    std::mem::drop(facade.clean_unused_topic(Some("127.0.0.1:10911".to_string()), None));
    std::mem::drop(facade.reset_master_flush_offset(Some("127.0.0.1:10912".to_string()), Some(1024)));
    std::mem::drop(facade.update_cold_data_flow_ctr_group_config(
        Some("127.0.0.1:10911".to_string()),
        None,
        "GroupA",
        "1024",
    ));
    std::mem::drop(facade.remove_cold_data_flow_ctr_group_config(Some("127.0.0.1:10911".to_string()), None, "GroupA"));
    std::mem::drop(facade.set_commit_log_read_ahead(
        Some("127.0.0.1:10911".to_string()),
        None,
        Some("0".to_string()),
        false,
        false,
        Some("4096".to_string()),
        None,
        false,
    ));
    std::mem::drop(facade.switch_timer_engine(Some("127.0.0.1:10911".to_string()), None, "R"));
    std::mem::drop(facade.update_subscription_group(
        Some("127.0.0.1:10911".to_string()),
        None,
        "GroupA",
        true,
        true,
        true,
        false,
        1,
        16,
        0,
        1,
        true,
        0,
        15,
    ));
    std::mem::drop(facade.update_subscription_group_list(
        Some("127.0.0.1:10911".to_string()),
        None,
        "GroupA,GroupB",
        true,
        true,
        true,
        false,
        1,
        16,
        0,
        1,
        true,
        0,
        15,
    ));
    std::mem::drop(facade.trigger_lite_dispatch(
        "ParentTopic",
        "GroupA",
        Some("client-a".to_string()),
        Some("broker-a".to_string()),
    ));
    std::mem::drop(facade.update_static_topic("StaticTopic", "broker-a", "4", Some("DefaultCluster".to_string())));
    std::mem::drop(facade.remapping_static_topic("StaticTopic", Some("broker-a".to_string()), None, Some(false)));
}

#[test]
fn facade_builds_phase_four_complex_workflow_requests_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");

    let export_configs = facade.export_configs_request(" DefaultCluster ").unwrap();
    assert_eq!(export_configs.cluster_name().as_str(), "DefaultCluster");
    assert_eq!(export_configs.namesrv_addr(), Some("127.0.0.1:9876"));

    let export_metrics = facade.export_metrics_request(" DefaultCluster ", Some(5000)).unwrap();
    assert_eq!(export_metrics.cluster_name().as_str(), "DefaultCluster");
    assert_eq!(export_metrics.namesrv_addr(), Some("127.0.0.1:9876"));
    assert_eq!(export_metrics.timeout_millis(), 5000);

    let export_metadata = facade
        .export_metadata_request(Some(" DefaultCluster ".to_string()), None, false, true, true)
        .unwrap();
    assert_eq!(export_metadata.namesrv_addr(), Some("127.0.0.1:9876"));
    assert!(export_metadata.special_topic());

    let rocksdb = facade.export_metadata_rocksdb_request(" ./store/config ", " topics ", true);
    assert_eq!(rocksdb.config_type(), "topics");
    assert!(rocksdb.json_enable());

    let rocksdb_rpc = facade
        .export_rocksdb_config_rpc_request(
            Some(" DefaultCluster ".to_string()),
            None,
            " topics;consumerOffsets; ",
            Some(5000),
        )
        .unwrap();
    assert_eq!(rocksdb_rpc.config_types().len(), 2);
    assert_eq!(rocksdb_rpc.timeout_millis(), 5000);
    assert_eq!(rocksdb_rpc.namesrv_addr(), Some("127.0.0.1:9876"));

    let pop_record = facade
        .export_pop_record_request(None, Some(" 127.0.0.1:10911 ".to_string()), true, Some(5000))
        .unwrap();
    assert!(pop_record.dry_run());
    assert_eq!(pop_record.timeout_millis(), 5000);
    assert_eq!(pop_record.namesrv_addr(), Some("127.0.0.1:9876"));

    let dump = facade.dump_compaction_log_request(Some(" ./compact.log ".to_string()));
    assert_eq!(dump.file().unwrap().to_string_lossy(), "./compact.log");

    let print = facade
        .print_messages_request(
            " TopicA ",
            " TagA ",
            Some(1_700_000_000_000),
            Some(1_700_000_100_000),
            Some(" ParentTopic ".to_string()),
        )
        .unwrap();
    assert_eq!(print.topic().as_str(), "TopicA");
    assert_eq!(print.namesrv_addr(), Some("127.0.0.1:9876"));

    let consume = facade
        .consume_messages_request(
            " TopicA ",
            Some(" broker-a ".to_string()),
            Some(0),
            Some(10),
            Some(" GroupA ".to_string()),
            None,
            None,
            32,
        )
        .unwrap();
    assert_eq!(consume.topic().as_str(), "TopicA");
    assert_eq!(consume.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn facade_exposes_phase_four_service_futures_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    std::mem::drop(facade.export_configs("DefaultCluster"));
    std::mem::drop(facade.export_metrics("DefaultCluster", Some(5000)));
    std::mem::drop(facade.export_metadata(Some("DefaultCluster".to_string()), None, false, false, false));
    std::mem::drop(facade.export_rocksdb_config_rpc(Some("DefaultCluster".to_string()), None, "topics", Some(5000)));
    std::mem::drop(facade.export_pop_records(None, Some("127.0.0.1:10911".to_string()), true, Some(5000)));
    std::mem::drop(facade.print_messages_with_progress("TopicA", "*", None, None, None, 64, |_| {}));
    std::mem::drop(facade.print_messages_by_queue_with_progress(
        "TopicA",
        "broker-a",
        0,
        "*",
        None,
        None,
        true,
        false,
        64,
        |_| {},
    ));
    std::mem::drop(facade.consume_messages_with_progress("TopicA", None, None, None, None, None, None, 32, 64, |_| {}));
}

#[test]
fn phase_four_message_pull_progress_messages_include_event_counts() {
    let message = super::operations::message_pull_progress_message(3, &MessagePullEvent::Separator);

    assert!(message.contains("3"));
    assert!(message.contains("separator"));
}

#[test]
fn phase_five_monitoring_progress_messages_include_event_context() {
    let message = super::operations::monitoring_progress_message(
        2,
        &MonitoringEvent::UndoneMsgs {
            round: 1,
            consumer_group: "GroupA".to_string(),
            topic: "TopicA".to_string(),
            undone_msgs_total: 42,
            undone_msgs_single_mq: 40,
            undone_msgs_delay_time_millis: None,
        },
    );

    assert!(message.contains("2"));
    assert!(message.contains("GroupA/TopicA"));
    assert!(message.contains("42"));
}

#[test]
fn facade_builds_phase_one_read_only_requests_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr(" 127.0.0.1:9876 ");

    let get_user = facade
        .auth_get_user_request(Some(" 127.0.0.1:10911 ".to_string()), None, " admin ")
        .unwrap();
    assert_eq!(get_user.username().as_str(), "admin");

    let list_acl = facade
        .auth_list_acl_request(
            None,
            Some(" DefaultCluster ".to_string()),
            Some(" User:* ".to_string()),
            Some(" TopicA ".to_string()),
        )
        .unwrap();
    assert_eq!(list_acl.subject_filter(), Some("User:*"));
    assert_eq!(list_acl.resource_filter(), Some("TopicA"));

    let controller_config = facade
        .controller_config_query_request(" 127.0.0.1:9878;127.0.0.2:9878 ")
        .unwrap();
    assert_eq!(controller_config.controller_servers().len(), 2);
    assert_eq!(controller_config.namesrv_addr(), Some("127.0.0.1:9876"));

    let broker_epoch = facade
        .broker_epoch_request(Some(" broker-a ".to_string()), None)
        .unwrap();
    assert_eq!(broker_epoch.namesrv_addr(), Some("127.0.0.1:9876"));

    let lite_topic = facade.lite_topic_info_request(" ParentTopic ", " LiteTopic ").unwrap();
    assert_eq!(lite_topic.parent_topic().as_str(), "ParentTopic");
    assert_eq!(lite_topic.lite_topic().as_str(), "LiteTopic");

    let decode = facade
        .decode_message_id_request(" C0A8010100002A9F0000000000000064 ")
        .unwrap();
    assert_eq!(decode.message_ids().len(), 1);

    let query_by_id = facade
        .query_message_by_id_request(" C0A8010100002A9F0000000000000064 ", Some(" TopicA ".to_string()), 3000)
        .unwrap();
    assert_eq!(query_by_id.message_ids().len(), 1);

    facade
        .query_message_by_unique_key_request(
            " C0A8010100002A9F0000000000000064 ",
            Some(" GroupA ".to_string()),
            Some(" client-a ".to_string()),
            " TopicA ",
            false,
            Some(" DefaultCluster ".to_string()),
            None,
            None,
        )
        .unwrap();

    facade
        .direct_consume_message_request(
            " TopicA ",
            " C0A8010100002A9F0000000000000064 ",
            " GroupA ",
            " client-a ",
            Some(" DefaultCluster ".to_string()),
        )
        .unwrap();

    let track = facade
        .message_track_request(
            " C0A8010100002A9F0000000000000064 ",
            " TopicA ",
            Some(" DefaultCluster ".to_string()),
            3000,
        )
        .unwrap();
    assert_eq!(track.message_ids().len(), 1);
}

#[test]
fn facade_exposes_phase_one_read_only_service_futures_without_cli_types() {
    let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

    std::mem::drop(facade.query_auth_user(Some("127.0.0.1:10911".to_string()), None, "admin"));
    std::mem::drop(facade.list_auth_users(None, Some("DefaultCluster".to_string()), Some("admin".to_string())));
    std::mem::drop(facade.query_auth_acl(Some("127.0.0.1:10911".to_string()), None, "User:admin"));
    std::mem::drop(facade.list_auth_acl(
        None,
        Some("DefaultCluster".to_string()),
        Some("User:*".to_string()),
        Some("TopicA".to_string()),
    ));
    std::mem::drop(facade.query_controller_config("127.0.0.1:9878"));
    std::mem::drop(facade.query_controller_metadata("127.0.0.1:9878"));
    std::mem::drop(facade.elect_controller_master("127.0.0.1:9878", "DefaultCluster", "broker-a", 1));
    std::mem::drop(facade.query_broker_epoch(Some("broker-a".to_string()), None));
    std::mem::drop(facade.query_cold_data_flow_ctr_info(Some("127.0.0.1:10911".to_string()), None));
    std::mem::drop(facade.query_broker_lite_info(Some("127.0.0.1:10911".to_string()), None));
    std::mem::drop(facade.query_parent_topic_info("ParentTopic"));
    std::mem::drop(facade.query_lite_topic_info("ParentTopic", "LiteTopic"));
    std::mem::drop(facade.query_lite_group_info("ParentTopic", "GroupA", Some("LiteTopic".to_string()), Some(10)));
    std::mem::drop(facade.query_lite_client_info("ParentTopic", "GroupA", "client-a"));
    std::mem::drop(facade.decode_message_id("C0A8010100002A9F0000000000000064"));
    std::mem::drop(facade.query_message_by_id("C0A8010100002A9F0000000000000064", Some("TopicA".to_string()), 3000));
    std::mem::drop(facade.query_message_by_key("TopicA", "KeyA", None, None, 32, None, None, None));
    std::mem::drop(facade.query_message_by_unique_key(
        "C0A8010100002A9F0000000000000064",
        None,
        None,
        "TopicA",
        false,
        None,
        None,
        None,
    ));
    std::mem::drop(facade.direct_consume_message(
        "TopicA",
        "C0A8010100002A9F0000000000000064",
        "GroupA",
        "client-a",
        None,
    ));
    std::mem::drop(facade.message_track("C0A8010100002A9F0000000000000064", "TopicA", None, 3000));
    std::mem::drop(facade.query_message_by_offset("TopicA", "broker-a", 0, 0, None));
    std::mem::drop(facade.query_message_trace_by_id("C0A8010100002A9F0000000000000064", None, None, None, 32));
}
