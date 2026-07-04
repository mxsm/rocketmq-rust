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

use std::path::PathBuf;

use config::Config;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use tracing::info;

pub fn parse_command_and_config_file(config_file: PathBuf) -> anyhow::Result<NamesrvConfig, anyhow::Error> {
    let namesrv_config = Config::builder()
        .add_source(config::File::with_name(
            config_file.to_string_lossy().into_owned().as_str(),
        ))
        .build()
        .map_or(NamesrvConfig::default(), |result| {
            result.try_deserialize::<NamesrvConfig>().unwrap_or_default()
        });
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
useRouteInfoManagerV2 = false
"#,
        )
        .expect("test config should be written");

        let config = parse_command_and_config_file(path.clone()).expect("config should parse");
        fs::remove_file(path).expect("test config should be removed");

        assert_eq!(config.rocketmq_home, "/tmp/rocketmq");
        assert_eq!(config.kv_config_path, "/tmp/rocketmq/kvConfig.json");
        assert!(!config.use_route_info_manager_v2);
    }

    #[test]
    fn namesrv_config_parse_loads_example_file() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resource/namesrv-example.toml");

        let config = parse_command_and_config_file(path).expect("example config should parse");

        assert_eq!(config.rocketmq_home, "/opt/rocketmq");
        assert_eq!(config.kv_config_path, "/home/rocketmq/rocketmq-namesrv/kvConfig.json");
        assert!(config.use_route_info_manager_v2);
    }

    #[test]
    fn namesrv_config_parse_falls_back_to_default_for_missing_file() {
        let config =
            parse_command_and_config_file(temp_config_path("missing")).expect("missing config should fall back");
        let default_config = NamesrvConfig::default();

        assert_eq!(config.rocketmq_home, default_config.rocketmq_home);
        assert_eq!(config.kv_config_path, default_config.kv_config_path);
        assert_eq!(
            config.use_route_info_manager_v2,
            default_config.use_route_info_manager_v2
        );
    }
}
