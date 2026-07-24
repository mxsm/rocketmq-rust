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

use std::fs;
use std::path::PathBuf;

use config::Config;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use tracing::info;

use crate::config::validate_namesrv_config_source;
use crate::NamesrvConfig;

pub fn parse_command_and_config_file(config_file: PathBuf) -> RocketMQResult<NamesrvConfig> {
    let source = fs::read_to_string(&config_file).map_err(|error| {
        RocketMQError::nameserver_config_invalid(format!("failed to read '{}': {error}", config_file.display()))
    })?;
    validate_namesrv_config_source(&source)?;

    let namesrv_config = Config::builder()
        .add_source(config::File::from(config_file.clone()))
        .build()
        .and_then(Config::try_deserialize::<NamesrvConfig>)
        .map_err(|error| {
            RocketMQError::nameserver_config_invalid(format!("failed to parse '{}': {error}", config_file.display()))
        })?;
    info!("rocketmq-namesrv config: {:?}", namesrv_config);
    Ok(namesrv_config)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use super::*;

    fn temp_config_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("rocketmq-namesrv-{name}-{}-{nanos}.toml", std::process::id()))
    }

    #[test]
    fn namesrv_config_parse_reads_selected_toml_fields() {
        let path = temp_config_path("selected-fields");
        fs::write(
            &path,
            r#"
rocketmqHome = "/tmp/rocketmq"
kvConfigPath = "/tmp/rocketmq/kvConfig.json"
"#,
        )
        .expect("test config should be written");

        let config = parse_command_and_config_file(path.clone()).expect("config should parse");
        fs::remove_file(path).expect("test config should be removed");

        assert_eq!(config.rocketmq_home, "/tmp/rocketmq");
        assert_eq!(config.kv_config_path, "/tmp/rocketmq/kvConfig.json");
    }

    #[test]
    fn namesrv_config_parse_loads_example_file() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resource/namesrv-example.toml");

        let config = parse_command_and_config_file(path).expect("example config should parse");

        assert_eq!(config.rocketmq_home, "/opt/rocketmq");
        assert_eq!(config.kv_config_path, "/home/rocketmq/rocketmq-namesrv/kvConfig.json");
    }

    #[test]
    fn namesrv_config_parse_loads_dev_baseline_file() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resource/namesrv.toml");

        let config = parse_command_and_config_file(path).expect("dev baseline config should parse");

        assert_eq!(config.rocketmq_home, "/tmp/rocketmq");
        assert_eq!(config.kv_config_path, "/tmp/rocketmq/kvConfig.json");
        assert_eq!(config.config_store_path, "/tmp/rocketmq/rocketmq-namesrv.properties");
    }

    #[test]
    fn namesrv_config_parse_loads_production_baseline_file() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resource/namesrv-production.toml");

        let config = parse_command_and_config_file(path).expect("production baseline config should parse");

        assert_eq!(config.rocketmq_home, "/opt/rocketmq");
        assert_eq!(config.kv_config_path, "/opt/rocketmq/data/kvConfig.json");
        assert_eq!(
            config.config_store_path,
            "/opt/rocketmq/data/rocketmq-namesrv.properties"
        );
        assert_eq!(config.client_request_thread_pool_nums, 16);
        assert_eq!(config.default_thread_pool_nums, 32);
        assert_eq!(config.client_request_thread_pool_queue_capacity, 100000);
        assert_eq!(config.default_thread_pool_queue_capacity, 20000);
        assert_eq!(config.unregister_broker_queue_capacity, 5000);
    }

    #[test]
    fn namesrv_config_parse_rejects_missing_file() {
        let error = parse_command_and_config_file(temp_config_path("missing"))
            .expect_err("missing config must fail explicitly");

        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::Tools(rocketmq_error::ToolsError::NameServerConfigInvalid { .. })
        ));
    }

    #[test]
    fn namesrv_config_parse_rejects_removed_route_manager_switch() {
        let path = temp_config_path("removed-route-manager-switch");
        fs::write(&path, "useRouteInfoManagerV2 = false\n").expect("test config should be written");

        let error =
            parse_command_and_config_file(path.clone()).expect_err("removed route manager switch must fail explicitly");
        fs::remove_file(path).expect("test config should be removed");

        assert!(matches!(
            error,
            rocketmq_error::RocketMQError::Tools(rocketmq_error::ToolsError::NameServerConfigInvalid { .. })
        ));
        assert!(error.to_string().contains("useRouteInfoManagerV2"));
    }
}
