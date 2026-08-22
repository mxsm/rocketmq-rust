// Copyright 2023 The RocketMQ Rust Authors
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

const BROKER_RUNTIME: &str = include_str!("../src/broker_runtime.rs");
const COMPOSITION: &str = include_str!("../src/broker_runtime/composition.rs");
const CONTROL_PLANE: &str = include_str!("../src/broker_runtime/control_plane.rs");
const CONTROL_PLANE_AUTH: &str = include_str!("../src/broker_runtime/control_plane/auth.rs");
const DATA_PLANE: &str = include_str!("../src/broker_runtime/data_plane.rs");
const REQUEST_PIPELINE: &str = include_str!("../src/broker_runtime/request_pipeline.rs");
const REQUEST_PIPELINE_STARTUP: &str = include_str!("../src/broker_runtime/request_pipeline/startup.rs");
const LIFECYCLE: &str = include_str!("../src/broker_runtime/lifecycle.rs");
const METADATA: &str = include_str!("../src/broker_runtime/metadata.rs");
const PROXY_FACADE: &str = include_str!("../src/proxy_facade.rs");

fn is_use_declaration(line: &str) -> bool {
    line.starts_with("use ") || (line.starts_with("pub") && line.contains(" use "))
}

fn delimiter_delta(line: &str) -> i32 {
    line.chars().fold(0, |depth, character| match character {
        '(' | '[' | '{' => depth + 1,
        ')' | ']' | '}' => depth - 1,
        _ => depth,
    })
}

fn trim_leading_block_comments<'a>(mut line: &'a str, in_block_comment: &mut bool) -> &'a str {
    loop {
        let trimmed = line.trim();
        if *in_block_comment {
            let Some(end) = trimmed.find("*/") else {
                return "";
            };
            *in_block_comment = false;
            line = &trimmed[end + 2..];
            continue;
        }
        if let Some(comment) = trimmed.strip_prefix("/*") {
            let Some(end) = comment.find("*/") else {
                *in_block_comment = true;
                return "";
            };
            line = &comment[end + 2..];
            continue;
        }
        return trimmed;
    }
}

fn production_code_line_count(source: &str) -> usize {
    let mut count = 0;
    let mut in_attribute = false;
    let mut attribute_depth = 0;
    let mut in_block_comment = false;
    let mut in_use_declaration = false;
    let mut skipping_test_item = false;
    let mut test_item_started = false;
    let mut test_item_depth = 0;
    let mut test_item_has_block = false;

    for source_line in source.lines() {
        let line = trim_leading_block_comments(source_line, &mut in_block_comment);
        if line.is_empty() || line.starts_with("//") {
            continue;
        }

        if skipping_test_item {
            if !test_item_started && line.starts_with("#[") {
                continue;
            }
            test_item_started = true;
            test_item_depth += delimiter_delta(line);
            test_item_has_block |= line.contains('{');
            if test_item_depth == 0 && (test_item_has_block || line.ends_with(';') || line.ends_with(',')) {
                skipping_test_item = false;
                test_item_started = false;
                test_item_has_block = false;
            }
            continue;
        }

        if line == "#[cfg(test)]" {
            skipping_test_item = true;
            test_item_depth = 0;
            continue;
        }

        if in_attribute {
            attribute_depth += delimiter_delta(line);
            if attribute_depth <= 0 {
                in_attribute = false;
            }
            continue;
        }
        if line.starts_with("#[") {
            attribute_depth = delimiter_delta(line);
            in_attribute = attribute_depth > 0;
            continue;
        }

        if in_use_declaration {
            if line.contains(';') {
                in_use_declaration = false;
            }
            continue;
        }
        if is_use_declaration(line) {
            in_use_declaration = !line.contains(';');
            continue;
        }

        count += 1;
    }

    count
}

fn assert_reviewable(name: &str, source: &str, maximum: usize) {
    let actual = production_code_line_count(source);
    assert!(
        actual <= maximum,
        "{name} has {actual} production code lines; limit is {maximum}"
    );
}

#[test]
fn broker_runtime_facade_owns_only_composition_and_lifecycle() {
    let declaration = BROKER_RUNTIME
        .split("pub(crate) struct BrokerRuntime {")
        .nth(1)
        .and_then(|source| source.split('}').next())
        .expect("BrokerRuntime declaration should exist");
    let fields = declaration
        .lines()
        .map(str::trim)
        .filter(|line| line.contains(':'))
        .collect::<Vec<_>>();

    assert_eq!(
        fields,
        ["composition: BrokerComposition,", "lifecycle: BrokerLifecycle,"]
    );
}

#[test]
fn broker_components_have_narrow_constructor_contracts() {
    for (name, source) in [
        ("BrokerControlPlane", CONTROL_PLANE),
        ("BrokerDataPlane", DATA_PLANE),
        ("BrokerRequestPipeline", REQUEST_PIPELINE),
        ("BrokerLifecycle", LIFECYCLE),
        ("BrokerMetadata", METADATA),
    ] {
        let contract = source
            .split("impl BrokerRuntime")
            .next()
            .expect("component declaration should precede runtime behavior");
        assert!(
            contract.contains("fn new"),
            "{name} must define an explicit constructor"
        );
        assert!(
            !contract.contains("BrokerRuntimeState"),
            "{name} must not accept or retain the complete BrokerRuntimeState"
        );
    }

    assert!(
        COMPOSITION.contains("state: Box<BrokerRuntimeState<BrokerMessageStore>>"),
        "BrokerComposition must remain the exclusive BrokerRuntimeState owner"
    );
}

#[test]
fn broker_runtime_is_split_into_reviewable_production_modules() {
    assert_reviewable("BrokerRuntime", BROKER_RUNTIME, 1_500);
    assert_reviewable("BrokerRequestPipeline", REQUEST_PIPELINE, 800);
    assert_reviewable("BrokerRequestPipeline startup", REQUEST_PIPELINE_STARTUP, 800);
    assert_reviewable("BrokerControlPlane", CONTROL_PLANE, 800);
    assert_reviewable("BrokerControlPlane auth", CONTROL_PLANE_AUTH, 800);
    assert_reviewable("BrokerDataPlane", DATA_PLANE, 800);
    assert_reviewable("BrokerMetadata", METADATA, 800);
}

#[test]
fn production_line_count_excludes_non_production_source() {
    let source = r#"
// module comment
use std::{
    sync::Arc,
};

#[derive(Debug)]
struct Runtime {
    #[cfg(test)]
    test_only: bool,
    value: Arc<()>,
}

/*
 * implementation note
 */
#[cfg(test)]
mod tests {
    #[test]
    fn test_only() {}
}

fn run() {}
"#;

    assert_eq!(production_code_line_count(source), 4);
}

#[test]
fn transient_requests_do_not_create_component_groups() {
    let process_request = PROXY_FACADE
        .split("pub async fn process_request_with_timeout")
        .nth(1)
        .and_then(|source| source.split("\n    }\n}").next())
        .expect("ProxyBrokerFacade::process_request_with_timeout should exist");

    assert!(
        process_request.contains("&self.local_request_tasks"),
        "local requests must reuse the component owner created during facade composition"
    );
    assert!(
        !process_request.contains(".child("),
        "a transient local request must not create a task-group child"
    );
}
