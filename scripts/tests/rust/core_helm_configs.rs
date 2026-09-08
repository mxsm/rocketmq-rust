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

use std::path::PathBuf;

pub fn rendered_configs(service: &str) -> Vec<PathBuf> {
    let root = std::env::var_os("CORE_HELM_CONFIG_DIR")
        .expect("export real Helm configs with scripts/core_helm_configs.py --output <directory>");
    let mut configs = std::fs::read_dir(PathBuf::from(root).join(service))
        .expect("rendered service config directory")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "toml"))
        .collect::<Vec<_>>();
    configs.sort();
    assert!(!configs.is_empty(), "no rendered configs for {service}");
    configs
}
