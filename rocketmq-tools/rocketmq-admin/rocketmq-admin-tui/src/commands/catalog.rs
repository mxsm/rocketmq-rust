use super::ArgKind;
use super::ArgSpec;
use super::CommandCategory;
use super::CommandExecutor;
use super::CommandSpec;
use super::ResultViewKind;
use super::RiskLevel;

pub fn command_catalog() -> Vec<CommandSpec> {
    let mut commands = Vec::new();
    topic_commands(&mut commands);
    nameserver_commands(&mut commands);
    auth_commands(&mut commands);
    broker_commands(&mut commands);
    cluster_commands(&mut commands);
    controller_commands(&mut commands);
    container_commands(&mut commands);
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
            "controller.elect_master",
            CommandCategory::Controller,
            "Elect Controller Master",
            "Re-elect the specified broker as master through the controller leader.",
            RiskLevel::Dangerous,
            vec![
                required_string(
                    "controller_address",
                    "Controller",
                    "Controller address.",
                    "127.0.0.1:9878",
                ),
                required_string("cluster_name", "Cluster", "Cluster name.", "DefaultCluster"),
                required_string("broker_name", "Broker Name", "Broker name.", "broker-a"),
                number(
                    "broker_id",
                    "Broker ID",
                    "Broker id to elect as master.",
                    true,
                    Some(1),
                    Some(0),
                ),
            ],
            ResultViewKind::Table,
            Some("broker_name"),
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

fn container_commands(commands: &mut Vec<CommandSpec>) {
    commands.extend([
        spec(
            "container.add_broker",
            CommandCategory::Container,
            "Add Broker To Container",
            "Add a broker to the specified broker container using a broker config path.",
            RiskLevel::Dangerous,
            vec![
                required_string(
                    "broker_container_addr",
                    "Broker Container Addr",
                    "Broker container address.",
                    "127.0.0.1:10911",
                ),
                required_string(
                    "broker_config_path",
                    "Broker Config Path",
                    "Broker config path visible to the broker container.",
                    "/path/to/broker.conf",
                ),
            ],
            ResultViewKind::OperationSummary,
            Some("broker_container_addr"),
        ),
        spec(
            "container.remove_broker",
            CommandCategory::Container,
            "Remove Broker From Container",
            "Remove a broker identity from the specified broker container.",
            RiskLevel::Dangerous,
            vec![
                required_string(
                    "broker_container_addr",
                    "Broker Container Addr",
                    "Broker container address.",
                    "127.0.0.1:10911",
                ),
                required_string("cluster_name", "Cluster", "Cluster name.", "DefaultCluster"),
                required_string("broker_name", "Broker Name", "Broker name.", "broker-a"),
                number("broker_id", "Broker ID", "Broker id to remove.", true, Some(1), Some(0)),
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
            "consumer.start_monitoring",
            CommandCategory::Consumer,
            "Start Monitoring",
            "Run bounded consumer monitoring rounds and stream typed monitor events.",
            RiskLevel::Safe,
            vec![
                number(
                    "round_count",
                    "Rounds",
                    "Monitoring round count.",
                    true,
                    Some(1),
                    Some(1),
                ),
                number(
                    "round_interval_millis",
                    "Round Interval",
                    "Delay between rounds in milliseconds.",
                    true,
                    Some(60_000),
                    Some(1),
                ),
                bool_arg(
                    "include_undone_msgs",
                    "Undone Msgs",
                    "Report consumer lag by topic.",
                    true,
                ),
                bool_arg(
                    "include_running_info",
                    "Running Info",
                    "Report consumer running-info diagnostics.",
                    true,
                ),
                number(
                    "max_events",
                    "Max Events",
                    "Maximum monitor events to keep.",
                    true,
                    Some(128),
                    Some(1),
                ),
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
            "Query messages by unique message key.",
            RiskLevel::Safe,
            vec![
                required_string(
                    "msg_id",
                    "Message ID",
                    "Unique message ID.",
                    "7F0000010007D8260BF075769D36C348",
                ),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
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
            "message.direct_consume",
            CommandCategory::Message,
            "Direct Consume Message",
            "Ask one push-consumer client to directly consume a message for diagnostics.",
            RiskLevel::Mutating,
            vec![
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                required_string(
                    "msg_id",
                    "Message ID",
                    "Message ID or unique message key.",
                    "7F0000010007D8260BF075769D36C348",
                ),
                required_string("consumer_group", "Consumer Group", "Consumer group.", "GroupA"),
                required_string("client_id", "Client ID", "Consumer client id.", "client-a"),
                optional_string("cluster", "Cluster", "Optional cluster name.", "DefaultCluster"),
            ],
            ResultViewKind::Table,
            None,
        ),
        spec(
            "message.track_by_id",
            CommandCategory::Message,
            "Track Message By ID",
            "Query message consumer track detail by message ID.",
            RiskLevel::Safe,
            vec![
                required_string(
                    "message_ids",
                    "Message IDs",
                    "Message IDs separated by comma, semicolon, or whitespace.",
                    "7F0000010007D8260BF075769D36C348",
                ),
                required_string("topic", "Topic", "Topic name.", "TopicA"),
                optional_string("cluster", "Cluster", "Optional cluster name.", "DefaultCluster"),
                number(
                    "timeout_millis",
                    "Timeout",
                    "Per-message track timeout in milliseconds.",
                    true,
                    Some(3000),
                    Some(1),
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
            "export.metrics",
            CommandCategory::Export,
            "Export Metrics",
            "Aggregate broker runtime metrics for a cluster into the Java-compatible export model.",
            RiskLevel::Safe,
            with_export_output_args(vec![
                required_string("cluster_name", "Cluster", "Cluster name.", "DefaultCluster"),
                number(
                    "timeout_millis",
                    "Timeout",
                    "Optional broker request timeout in milliseconds.",
                    false,
                    Some(10000),
                    Some(1),
                ),
            ]),
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
                    &["topics", "subscriptionGroups", "consumerOffsets"],
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
            "export.metadata_rocksdb_rpc",
            CommandCategory::Export,
            "Export RocksDB Config RPC",
            "Ask broker-side RocksDB config export to write JSON on the target broker.",
            RiskLevel::Mutating,
            vec![
                optional_string(
                    "cluster_name",
                    "Cluster",
                    "Cluster name for master broker targets.",
                    "DefaultCluster",
                ),
                optional_string(
                    "broker_addr",
                    "Broker Addr",
                    "Single broker address.",
                    "127.0.0.1:10911",
                ),
                required_string(
                    "config_types",
                    "Config Types",
                    "Semicolon-separated config types.",
                    "topics;subscriptionGroups;consumerOffsets",
                ),
                number(
                    "timeout_millis",
                    "Timeout",
                    "Request timeout in milliseconds.",
                    false,
                    Some(30000),
                    Some(1),
                ),
            ],
            ResultViewKind::OperationSummary,
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
