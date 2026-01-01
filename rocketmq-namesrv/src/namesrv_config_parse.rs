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
