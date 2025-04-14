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

use std::fmt::Debug;
use std::path::PathBuf;

use config::Config;
use rocketmq_error::RocketMQResult;
use serde::Deserialize;

pub fn parse_config_file<'de, C>(config_file: PathBuf) -> RocketMQResult<C>
where
    C: Default + Debug + Deserialize<'de>,
{
    let config_file = match Config::builder()
        .add_source(config::File::with_name(&config_file.to_string_lossy()))
        .build()
    {
        Ok(cfg) => cfg.try_deserialize::<C>().unwrap_or_default(),
        Err(err) => {
            return Err(rocketmq_error::RocketmqError::ConfigError(err.to_string()));
        }
    };
    Ok(config_file)
}
