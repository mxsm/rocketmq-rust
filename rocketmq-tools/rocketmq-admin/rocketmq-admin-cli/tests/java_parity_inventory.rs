use std::collections::HashMap;
use std::collections::HashSet;

use clap::CommandFactory;
use rocketmq_admin_cli::rocketmq_cli::RocketMQCli;

#[test]
fn java_core_commands_exclude_broker_container_and_keep_active_operations_reachable() {
    let command = RocketMQCli::command();
    let domains = command
        .get_subcommands()
        .map(|domain| {
            (
                domain.get_name().to_string(),
                domain
                    .get_subcommands()
                    .map(|subcommand| subcommand.get_name().to_string())
                    .collect::<HashSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    let java_raw_operations = [
        ("container", "addBroker"),
        ("nameserver", "addWritePerm"),
        ("topic", "allocateMQ"),
        ("broker", "brokerConsumeStats"),
        ("broker", "brokerStatus"),
        ("message", "checkMsgSendRT"),
        ("queue", "checkRocksdbCqWriteProgress"),
        ("controller", "cleanBrokerMetadata"),
        ("broker", "cleanExpiredCQ"),
        ("broker", "cleanUnusedTopic"),
        ("offset", "cloneGroupOffset"),
        ("cluster", "clusterList"),
        ("cluster", "clusterRT"),
        ("message", "consumeMessage"),
        ("connection", "consumerConnection"),
        ("consumer", "consumerProgress"),
        ("consumer", "consumerStatus"),
        ("auth", "copyAcl"),
        ("auth", "copyUser"),
        ("auth", "createAcl"),
        ("auth", "createUser"),
        ("auth", "deleteAcl"),
        ("broker", "deleteExpiredCommitLog"),
        ("nameserver", "deleteKvConfig"),
        ("consumer", "deleteSubGroup"),
        ("topic", "deleteTopic"),
        ("auth", "deleteUser"),
        ("message", "dumpCompactionLog"),
        ("controller", "electMaster"),
        ("export", "exportConfigs"),
        ("export", "exportMetadata"),
        ("export", "exportMetadataInRocksDB"),
        ("export", "exportMetrics"),
        ("export", "exportPopRecord"),
        ("auth", "getAcl"),
        ("broker", "getBrokerConfig"),
        ("broker", "getBrokerEpoch"),
        ("lite", "getBrokerLiteInfo"),
        ("broker", "getColdDataFlowCtrInfo"),
        ("consumer", "getConsumerConfig"),
        ("controller", "getControllerConfig"),
        ("controller", "getControllerMetaData"),
        ("lite", "getLiteClientInfo"),
        ("lite", "getLiteGroupInfo"),
        ("lite", "getLiteTopicInfo"),
        ("nameserver", "getNamesrvConfig"),
        ("lite", "getParentTopicInfo"),
        ("ha", "getSyncStateSet"),
        ("auth", "getUser"),
        ("ha", "haStatus"),
        ("auth", "listAcl"),
        ("auth", "listUser"),
        ("message", "printMsg"),
        ("message", "printMsgByQueue"),
        ("producer", "producer"),
        ("connection", "producerConnection"),
        ("queue", "queryCq"),
        ("message", "queryMsgById"),
        ("message", "queryMsgByKey"),
        ("message", "queryMsgByOffset"),
        ("message", "queryMsgByUniqueKey"),
        ("message", "queryMsgTraceById"),
        ("topic", "remappingStaticTopic"),
        ("container", "removeBroker"),
        ("broker", "removeColdDataFlowCtrGroupConfig"),
        ("broker", "resetMasterFlushOffset"),
        ("offset", "resetOffsetByTime"),
        ("export", "rocksDBConfigToJson"),
        ("message", "sendMessage"),
        ("broker", "sendMsgStatus"),
        ("broker", "setCommitLogReadAheadMode"),
        ("consumer", "setConsumeMode"),
        ("offset", "skipAccumulatedMessage"),
        ("consumer", "startMonitoring"),
        ("stats", "statsAll"),
        ("broker", "switchTimerEngine"),
        ("topic", "topicClusterList"),
        ("topic", "topicList"),
        ("topic", "topicRoute"),
        ("topic", "topicStatus"),
        ("lite", "triggerLiteDispatch"),
        ("auth", "updateAcl"),
        ("broker", "updateBrokerConfig"),
        ("broker", "updateColdDataFlowCtrGroupConfig"),
        ("controller", "updateControllerConfig"),
        ("nameserver", "updateKvConfig"),
        ("nameserver", "updateNamesrvConfig"),
        ("topic", "updateOrderConf"),
        ("topic", "updateStaticTopic"),
        ("consumer", "updateSubGroup"),
        ("consumer", "updateSubGroupList"),
        ("topic", "updateTopic"),
        ("topic", "updateTopicList"),
        ("topic", "updateTopicPerm"),
        ("auth", "updateUser"),
        ("nameserver", "wipeWritePerm"),
    ];

    let excluded_operations = HashSet::from([("container", "addBroker"), ("container", "removeBroker")]);
    assert_eq!(java_raw_operations.len(), 96);
    assert_eq!(excluded_operations.len(), 2);

    let mut active_count = 0;
    for (domain, command_name) in java_raw_operations {
        if excluded_operations.contains(&(domain, command_name)) {
            assert!(
                domains
                    .get(domain)
                    .is_none_or(|commands| !commands.contains(command_name)),
                "excluded BrokerContainer command {domain}.{command_name} must not be reachable"
            );
            continue;
        }
        active_count += 1;
        let commands = domains
            .get(domain)
            .unwrap_or_else(|| panic!("expected Rust CLI domain {domain} to exist"));
        assert!(
            commands.contains(command_name),
            "expected Java command {command_name} to be reachable under Rust domain {domain}"
        );
    }
    assert_eq!(active_count, 94);
    assert!(!domains.contains_key("container"));
}
