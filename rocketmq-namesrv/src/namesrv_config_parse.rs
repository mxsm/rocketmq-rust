/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use config::Config;
use rocketmq_common::common::namesrv::namesrv_config::NamesrvConfig;
use tracing::info;

fn parse_command_and_config_file() -> anyhow::Result<(), anyhow::Error> {
    let namesrv_config = Config::builder()
        .add_source(config::File::with_name(
            format!(
                "rocketmq-namesrv{}resource{}rocketmq-namesrv.toml",
                std::path::MAIN_SEPARATOR,
                std::path::MAIN_SEPARATOR
            )
            .as_str(),
        ))
        .build()
        .unwrap();
    let result = namesrv_config.try_deserialize::<NamesrvConfig>().unwrap();
    info!("rocketmq-namesrv config: {:?}", result);
    Ok(())
}
