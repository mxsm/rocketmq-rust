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

use std::fmt::Debug;
use std::path::PathBuf;

use config::Config;
use serde::Deserialize;

use crate::RuntimeError;
use crate::RuntimeResult;

/// Parses config file.
pub fn parse_config_file<'de, C>(config_file: PathBuf) -> RuntimeResult<C>
where
    C: Debug + Deserialize<'de>,
{
    let cfg = Config::builder()
        .add_source(config::File::from(config_file.as_path()))
        .build()
        .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
    let config_file = cfg
        .try_deserialize::<C>()
        .map_err(|error| RuntimeError::Configuration(error.to_string()))?;
    Ok(config_file)
}
