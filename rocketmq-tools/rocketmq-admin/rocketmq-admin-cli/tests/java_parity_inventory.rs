use std::collections::HashMap;
use std::collections::HashSet;

use clap::CommandFactory;
use rocketmq_admin_cli::rocketmq_cli::RocketMQCli;

#[test]
fn java_registered_commands_are_reachable_under_expected_rust_domains() {
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

    let java_registered_commands = [
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

    assert_eq!(java_registered_commands.len(), 96);

    for (domain, command_name) in java_registered_commands {
        let commands = domains
            .get(domain)
            .unwrap_or_else(|| panic!("expected Rust CLI domain {domain} to exist"));
        assert!(
            commands.contains(command_name),
            "expected Java command {command_name} to be reachable under Rust domain {domain}"
        );
    }
}
