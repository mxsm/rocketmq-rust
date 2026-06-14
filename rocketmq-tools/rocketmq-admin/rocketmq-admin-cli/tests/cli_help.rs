use std::process::Command;

#[test]
fn help_output_exposes_admin_cli() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .arg("--help")
        .output()
        .expect("run rocketmq-admin-cli --help");

    assert!(
        output.status.success(),
        "expected --help to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Rocketmq Rust admin commands"),
        "help output should describe the admin CLI, got: {stdout}"
    );
    assert!(
        stdout.contains("generate-completion"),
        "help output should expose shell completion, got: {stdout}"
    );
}

#[test]
fn bash_completion_output_exposes_root_command() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .args(["--generate-completion", "bash"])
        .output()
        .expect("run rocketmq-admin-cli --generate-completion bash");

    assert!(
        output.status.success(),
        "expected completion generation to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("_rocketmq-admin-cli"),
        "bash completion should include the root completion function, got: {stdout}"
    );
    assert!(
        stdout.contains("complete"),
        "bash completion should include the shell registration line, got: {stdout}"
    );
}

#[test]
fn topic_help_exposes_migrated_topic_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .args(["topic", "--help"])
        .output()
        .expect("run rocketmq-admin-cli topic --help");

    assert!(
        output.status.success(),
        "expected topic help to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    for command in [
        "topicList",
        "deleteTopic",
        "updateOrderConf",
        "updateTopic",
        "allocateMQ",
    ] {
        assert!(
            stdout.contains(command),
            "topic help should expose migrated command {command}, got: {stdout}"
        );
    }
}

#[test]
fn broker_help_exposes_update_broker_config_slice() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .args(["broker", "--help"])
        .output()
        .expect("run rocketmq-admin-cli broker --help");

    assert!(
        output.status.success(),
        "expected broker help to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("updateBrokerConfig"),
        "broker help should expose the migrated updateBrokerConfig slice, got: {stdout}"
    );
    assert!(
        stdout.contains("getBrokerConfig"),
        "broker help should expose broker config query command, got: {stdout}"
    );
}

#[test]
fn container_help_exposes_java_container_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .args(["container", "--help"])
        .output()
        .expect("run rocketmq-admin-cli container --help");

    assert!(
        output.status.success(),
        "expected container help to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    for command in ["addBroker", "removeBroker"] {
        assert!(
            stdout.contains(command),
            "container help should expose Java command {command}, got: {stdout}"
        );
    }
}

#[test]
fn controller_help_exposes_java_controller_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .args(["controller", "--help"])
        .output()
        .expect("run rocketmq-admin-cli controller --help");

    assert!(
        output.status.success(),
        "expected controller help to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    for command in ["electMaster", "getControllerMetaData"] {
        assert!(
            stdout.contains(command),
            "controller help should expose Java command {command}, got: {stdout}"
        );
    }
}

#[test]
fn export_help_exposes_java_export_commands() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .args(["export", "--help"])
        .output()
        .expect("run rocketmq-admin-cli export --help");

    assert!(
        output.status.success(),
        "expected export help to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    for command in ["exportMetrics", "rocksDBConfigToJson"] {
        assert!(
            stdout.contains(command),
            "export help should expose Java command {command}, got: {stdout}"
        );
    }
}

#[test]
fn java_renamed_commands_keep_old_rust_aliases() {
    for args in [
        &["auth", "listUser", "--help"][..],
        &["auth", "listUsers", "--help"][..],
        &["message", "printMsg", "--help"][..],
        &["message", "printMessage", "--help"][..],
        &["controller", "getControllerMetaData", "--help"][..],
        &["controller", "getControllerMetadata", "--help"][..],
        &["message", "QueryMsgTraceById", "--help"][..],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
            .args(args)
            .output()
            .unwrap_or_else(|error| panic!("run rocketmq-admin-cli {args:?}: {error}"));

        assert!(
            output.status.success(),
            "expected alias command {args:?} to show help, stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn java_broker_name_short_option_is_normalized_before_clap_parse() {
    let output = Command::new(env!("CARGO_BIN_EXE_rocketmq-admin-cli"))
        .args([
            "controller",
            "electMaster",
            "-a",
            "127.0.0.1:9878",
            "-b",
            "1",
            "-bn",
            "broker-a",
            "-c",
            "DefaultCluster",
            "--help",
        ])
        .output()
        .expect("run rocketmq-admin-cli controller electMaster -bn ... --help");

    assert!(
        output.status.success(),
        "expected -bn normalized help to exit successfully, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
