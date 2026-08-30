// Copyright 2026 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::path::Path;

const STATIC_BUSINESS_CONSTRUCTORS: [&str; 3] = [
    "RemotingCommand::create_",
    "RemotingCommand::new_request",
    "RemotingCommand::new_response",
];

const APPLICATION_FACTORY_HELPERS: [&str; 6] = [
    "command_from_error_with_opaque(",
    "command_from_error_with_remark_and_opaque(",
    "internal_error_with_opaque(",
    "request_code_not_supported_with_opaque(",
    "request_code_not_supported_with_remark(",
    "request_code_not_supported_with_remark_and_opaque(",
];

const OWNER_BYPASS_CONSTRUCTORS: [&str; 2] = [
    "Broker2Client::default()",
    "RpcClientUtils::create_command_for_rpc_response(",
];

fn production_prefix(source: &str) -> &str {
    let cfg_test = source.find("#[cfg(test)]\nmod tests");
    let plain_test = source.find("mod tests {");
    match (cfg_test, plain_test) {
        (Some(left), Some(right)) => &source[..left.min(right)],
        (Some(index), None) | (None, Some(index)) => &source[..index],
        (None, None) => source,
    }
}

fn collect_rust_files(root: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(root).expect("audit directory should be readable") {
        let path = entry.expect("audit entry should be readable").path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| matches!(name.to_str(), Some("admin_broker_processor" | "tests")))
            {
                continue;
            }
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            if path.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                name == "admin_broker_processor.rs" || name == "tests.rs" || name.ends_with("_tests.rs")
            }) {
                continue;
            }
            files.push(path);
        }
    }
}

#[test]
fn broker_request_processor_owners_do_not_bypass_the_injected_factory() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rust_files(&manifest.join("src/processor"), &mut files);
    files.extend([
        manifest.join("src/processor.rs"),
        manifest.join("src/client/net/broker_to_client.rs"),
        manifest.join("src/latency/broker_fast_failure.rs"),
        manifest.join("src/topic/manager/topic_queue_mapping_manager.rs"),
        manifest.join("src/broker_runtime/request_pipeline.rs"),
    ]);

    let mut bypasses = Vec::new();
    for file in files {
        let source = fs::read_to_string(&file).expect("audited Rust source should be readable");
        let production = production_prefix(&source);
        for (line_index, line) in production.lines().enumerate() {
            if STATIC_BUSINESS_CONSTRUCTORS
                .iter()
                .any(|constructor| line.contains(constructor))
                || APPLICATION_FACTORY_HELPERS.iter().any(|helper| line.contains(helper))
                || OWNER_BYPASS_CONSTRUCTORS
                    .iter()
                    .any(|constructor| line.contains(constructor))
            {
                bypasses.push(format!(
                    "{}:{}: {}",
                    file.strip_prefix(manifest)
                        .expect("audited file should be below the crate root")
                        .display(),
                    line_index + 1,
                    line.trim()
                ));
            }
        }
    }

    let consumer_request_handler = manifest.join("src/processor/admin_broker_processor/consumer_request_handler.rs");
    let source = fs::read_to_string(&consumer_request_handler).expect("audited Rust source should be readable");
    let production = production_prefix(&source);
    for (line_index, line) in production.lines().enumerate() {
        if OWNER_BYPASS_CONSTRUCTORS
            .iter()
            .any(|constructor| line.contains(constructor))
        {
            bypasses.push(format!(
                "{}:{}: {}",
                consumer_request_handler
                    .strip_prefix(manifest)
                    .expect("audited file should be below the crate root")
                    .display(),
                line_index + 1,
                line.trim()
            ));
        }
    }

    assert!(
        bypasses.is_empty(),
        "Broker request processor production paths bypass the injected RemotingCommandFactory:\n{}",
        bypasses.join("\n")
    );
}
