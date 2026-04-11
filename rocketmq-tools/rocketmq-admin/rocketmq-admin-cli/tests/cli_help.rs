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
