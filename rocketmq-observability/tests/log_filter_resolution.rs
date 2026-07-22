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

use rocketmq_observability::LogFilterInputs;
use rocketmq_observability::LogFilterResolver;
use rocketmq_observability::LogFilterSource;
use rocketmq_observability::LoggingOverrides;
use rocketmq_observability::ObservabilityError;
use rocketmq_observability::DEFAULT_LOG_FILTER;

#[test]
fn resolver_uses_the_documented_priority_order() {
    let cases = [
        (
            LogFilterInputs {
                runtime: Some("rocketmq_runtime=trace"),
                cli: Some("rocketmq_cli=debug"),
                environment: Some("warn"),
                config: Some("error"),
                legacy_config: None,
            },
            "rocketmq_runtime=trace",
            LogFilterSource::Runtime,
        ),
        (
            LogFilterInputs {
                cli: Some("rocketmq_cli=debug"),
                environment: Some("warn"),
                config: Some("error"),
                ..LogFilterInputs::default()
            },
            "rocketmq_cli=debug",
            LogFilterSource::Cli,
        ),
        (
            LogFilterInputs {
                environment: Some("warn"),
                config: Some("error"),
                ..LogFilterInputs::default()
            },
            "warn",
            LogFilterSource::Env,
        ),
        (
            LogFilterInputs {
                config: Some("error"),
                ..LogFilterInputs::default()
            },
            "error",
            LogFilterSource::Config,
        ),
        (LogFilterInputs::default(), DEFAULT_LOG_FILTER, LogFilterSource::Default),
    ];

    for (inputs, expected_filter, expected_source) in cases {
        let resolved = LogFilterResolver::resolve(inputs).expect("filter inputs should resolve");
        assert_eq!(resolved.filter(), expected_filter);
        assert_eq!(resolved.source(), expected_source);
    }
}

#[test]
fn resolver_accepts_matching_config_aliases_and_rejects_conflicts() {
    let matching = LogFilterResolver::resolve(LogFilterInputs {
        config: Some(" info "),
        legacy_config: Some("info"),
        ..LogFilterInputs::default()
    })
    .expect("matching aliases should be accepted");
    assert_eq!(matching.filter(), "info");
    assert_eq!(matching.source(), LogFilterSource::Config);

    let error = LogFilterResolver::resolve(LogFilterInputs {
        config: Some("info"),
        legacy_config: Some("debug"),
        ..LogFilterInputs::default()
    })
    .expect_err("conflicting aliases must fail");
    assert!(matches!(error, ObservabilityError::InvalidConfig(_)));
}

#[test]
fn resolver_rejects_explicit_blank_and_invalid_filters() {
    for candidate in ["   ", "rocketmq_store==debug"] {
        let error = LogFilterResolver::resolve(LogFilterInputs {
            cli: Some(candidate),
            ..LogFilterInputs::default()
        })
        .expect_err("an explicit invalid filter must fail");

        assert!(matches!(error, ObservabilityError::InvalidLogFilter { .. }));
    }
}

#[test]
fn logging_overrides_deserialize_nested_and_java_compatible_fields() {
    let nested: LoggingOverrides = serde_yaml::from_str(
        r#"
logging:
  filter: info,rocketmq_broker=debug
  reload:
    enabled: true
"#,
    )
    .expect("nested logging overrides should deserialize");
    assert_eq!(nested.logging.filter.as_deref(), Some("info,rocketmq_broker=debug"));
    assert!(nested.logging.reload.enabled);
    assert_eq!(nested.log_filter, None);

    let legacy: LoggingOverrides =
        serde_yaml::from_str("logFilter: warn\n").expect("Java-compatible logFilter should deserialize");
    assert_eq!(legacy.log_filter.as_deref(), Some("warn"));
}
