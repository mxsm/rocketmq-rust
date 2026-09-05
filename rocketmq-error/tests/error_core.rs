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

use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::mem::size_of;
use std::process::Command;
use std::sync::Arc;

use rocketmq_error::fields;
use rocketmq_error::BacktracePolicy;
use rocketmq_error::CanonicalCondition;
use rocketmq_error::ComponentId;
use rocketmq_error::Error;
use rocketmq_error::ErrorClass;
use rocketmq_error::ErrorContext;
use rocketmq_error::Exposure;
use rocketmq_error::FaultAttribution;
use rocketmq_error::SharedError;
use rocketmq_error::ViewValueRef;
use rocketmq_error::CORE_INTERNAL_FAILURE;
use rocketmq_error::ROUTE_TOPIC_NOT_FOUND;
use rocketmq_error::STORAGE_STATE_CORRUPTED;

#[derive(Debug)]
struct OuterCause {
    source: io::Error,
}

impl fmt::Display for OuterCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("outer-cause")
    }
}

impl StdError for OuterCause {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
struct ReplacementCause;

impl fmt::Display for ReplacementCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("replacement-cause")
    }
}

impl StdError for ReplacementCause {}

#[test]
fn canonical_error_is_one_pointer_and_exposes_descriptor_policy() {
    assert!(size_of::<Error>() <= 2 * size_of::<usize>());

    let error = Error::new(&CORE_INTERNAL_FAILURE);
    assert_eq!(error.descriptor(), &CORE_INTERNAL_FAILURE);
    assert_eq!(error.code(), CORE_INTERNAL_FAILURE.code());
    assert_eq!(error.class(), ErrorClass::INTERNAL);
    assert_eq!(error.condition(), CanonicalCondition::Internal);
    assert_eq!(error.fault(), FaultAttribution::Unknown);
    assert_eq!(error.component(), ComponentId::CORE);
    assert_eq!(error.exposure(), Exposure::Generic);
    assert_eq!(error.backtrace_policy(), BacktracePolicy::OnDemand);
    assert_eq!(error.projection(), CORE_INTERNAL_FAILURE.projection());
}

#[test]
fn typed_sources_remain_direct_and_follow_the_original_causal_order() {
    let error = Error::caused_by(
        &CORE_INTERNAL_FAILURE,
        OuterCause {
            source: io::Error::other("typed-leaf"),
        },
    );

    let outer = StdError::source(&error)
        .and_then(|source| source.downcast_ref::<OuterCause>())
        .expect("direct typed source");
    let leaf = outer
        .source()
        .and_then(|source| source.downcast_ref::<io::Error>())
        .expect("second-level typed source");
    assert_eq!(leaf.to_string(), "typed-leaf");
}

#[test]
fn context_and_latest_source_survive_sharing_without_recapturing_location() {
    let expected_line = line!() + 1;
    let error = Error::caused_by(&ROUTE_TOPIC_NOT_FOUND, io::Error::other("first-source"));
    assert_eq!(error.location().line(), expected_line);
    assert!(error.location().file().ends_with("error_core.rs"));

    let error = error
        .with_context(ErrorContext::new().with_text(fields::TOPIC, "orders"))
        .with_source(ReplacementCause);
    assert_eq!(error.location().line(), expected_line);
    assert!(StdError::source(&error)
        .and_then(|source| source.downcast_ref::<ReplacementCause>())
        .is_some());
    assert!(StdError::source(&error)
        .and_then(|source| source.downcast_ref::<io::Error>())
        .is_none());

    let shared: SharedError = Arc::new(error);
    let cloned = Arc::clone(&shared);
    assert!(Arc::ptr_eq(&shared, &cloned));
    assert_eq!(shared.location(), cloned.location());
    assert_eq!(shared.context(), cloned.context());
    let first_source = StdError::source(shared.as_ref()).expect("shared source");
    let cloned_source = StdError::source(cloned.as_ref()).expect("cloned source");
    assert!(std::ptr::eq(first_source, cloned_source));

    let public = shared.public_view().expect("valid public view");
    assert_eq!(
        public.fields().next().expect("topic").value(),
        ViewValueRef::Text("orders")
    );
}

#[test]
fn fixed_and_redacted_formatting_never_renders_source_location_or_secret_values() {
    const SOURCE_SENTINEL: &str = "canonical-source-secret";
    let error = Error::caused_by(&STORAGE_STATE_CORRUPTED, io::Error::other(SOURCE_SENTINEL)).with_context(
        ErrorContext::new()
            .with_text(fields::STORE_OPERATION, "read")
            .with_text(fields::STORE_COMPONENT, "commit_log")
            .with_secret_presence(fields::STORE_DETAIL_PRESENT)
            .with_secret_presence(fields::SOURCE_PRESENT),
    );

    let display = error.to_string();
    let debug = format!("{error:?}");
    assert_eq!(display, "storage.state.corrupted: Storage state is corrupted");
    assert!(!display.contains(SOURCE_SENTINEL));
    assert!(!debug.contains(SOURCE_SENTINEL));
    assert!(!debug.contains(error.location().file()));
    assert!(!debug.contains("error_core::"));

    let public = error.public_view().expect("valid public view");
    assert!(public.fields().next().is_none());
    let public_debug = format!("{public:?}");
    assert!(!public_debug.contains(SOURCE_SENTINEL));
    assert!(!public_debug.contains("commit_log"));

    let diagnostic = error.diagnostic_view().expect("valid diagnostic view");
    let fields = diagnostic.fields().collect::<Vec<_>>();
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[0].value(), ViewValueRef::Text("read"));
    assert_eq!(fields[1].value(), ViewValueRef::Text("commit_log"));
    assert_eq!(fields[2].value(), ViewValueRef::Redacted);
    assert_eq!(fields[3].value(), ViewValueRef::Redacted);
    assert!(!format!("{diagnostic:?}").contains(SOURCE_SENTINEL));
}

#[test]
fn backtrace_policy_uses_isolated_standard_environment_probes() {
    run_backtrace_probe("on_demand_backtrace_enabled_probe", "1");
    run_backtrace_probe("on_demand_backtrace_disabled_probe", "0");
}

fn run_backtrace_probe(test_name: &str, rust_backtrace: &str) {
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--ignored", "--exact", test_name])
        .env("RUST_BACKTRACE", rust_backtrace)
        .env_remove("RUST_LIB_BACKTRACE")
        .status()
        .expect("spawn isolated backtrace probe");
    assert!(status.success(), "backtrace probe {test_name} failed");
}

#[test]
#[ignore = "executed in an isolated child process"]
fn on_demand_backtrace_enabled_probe() {
    let internal = Error::new(&CORE_INTERNAL_FAILURE);
    let ordinary = Error::new(&ROUTE_TOPIC_NOT_FOUND);
    assert!(internal.backtrace().is_some());
    assert!(ordinary.backtrace().is_none());
    let debug = format!("{internal:?}");
    assert!(!debug.contains("on_demand_backtrace_enabled_probe"));
    assert!(!debug.contains(internal.location().file()));
}

#[test]
#[ignore = "executed in an isolated child process"]
fn on_demand_backtrace_disabled_probe() {
    assert!(Error::new(&CORE_INTERNAL_FAILURE).backtrace().is_none());
    assert!(Error::new(&ROUTE_TOPIC_NOT_FOUND).backtrace().is_none());
}
