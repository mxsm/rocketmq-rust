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
