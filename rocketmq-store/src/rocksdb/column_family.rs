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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RocksDbColumnFamily {
    Default,
    ConsumeQueueOffset,
    Timer,
    Transaction,
    PopState,
    Config(String),
}

impl RocksDbColumnFamily {
    pub fn name(&self) -> &str {
        match self {
            Self::Default => "default",
            Self::ConsumeQueueOffset => "offset",
            Self::Timer => "timer",
            Self::Transaction => "trans",
            Self::PopState => "popState",
            Self::Config(name) => name.as_str(),
        }
    }
}
