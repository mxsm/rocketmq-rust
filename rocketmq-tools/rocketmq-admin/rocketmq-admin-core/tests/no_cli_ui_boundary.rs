#[test]
fn core_does_not_expose_cli_or_terminal_ui_modules() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read rocketmq-admin-core Cargo.toml");
    let lib_rs = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"))
        .expect("read rocketmq-admin-core src/lib.rs");

    assert!(
        !manifest.contains("colored"),
        "rocketmq-admin-core should not depend on colored terminal output"
    );
    assert!(
        !manifest.contains("clap"),
        "rocketmq-admin-core should not depend on clap command-line parsing"
    );
    assert!(
        !manifest.contains("dialoguer"),
        "rocketmq-admin-core should not depend on dialoguer terminal prompts"
    );
    assert!(
        !manifest.contains("indicatif"),
        "rocketmq-admin-core should not depend on indicatif terminal progress"
    );
    assert!(
        !manifest.contains("tabled"),
        "rocketmq-admin-core should not depend on tabled terminal rendering"
    );
    assert!(
        !lib_rs.contains("pub mod cli"),
        "rocketmq-admin-core should not expose CLI adapter modules"
    );
    assert!(
        !lib_rs.contains("pub mod commands"),
        "rocketmq-admin-core should not expose clap command modules"
    );
    assert!(
        !lib_rs.contains("pub mod ui"),
        "rocketmq-admin-core should not expose terminal UI modules"
    );
}

#[test]
fn core_gates_rocksdb_metadata_export_behind_feature() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read rocketmq-admin-core Cargo.toml");

    assert!(
        manifest.contains("rocksdb-export"),
        "rocketmq-admin-core should expose a named RocksDB metadata export feature"
    );
    assert!(
        manifest.contains("rocksdb") && manifest.contains("optional = true"),
        "rocketmq-admin-core should keep rocksdb optional instead of pulling it into every consumer"
    );
}
