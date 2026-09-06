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

use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TraceType {
    #[default]
    Pub,
    SubBefore,
    SubAfter,
    EndTransaction,
    Recall,
}

impl TraceType {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "Pub" => Some(Self::Pub),
            "SubBefore" => Some(Self::SubBefore),
            "SubAfter" => Some(Self::SubAfter),
            "EndTransaction" => Some(Self::EndTransaction),
            "Recall" => Some(Self::Recall),
            _ => None,
        }
    }
}

impl Display for TraceType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Pub => "Pub",
            Self::SubBefore => "SubBefore",
            Self::SubAfter => "SubAfter",
            Self::EndTransaction => "EndTransaction",
            Self::Recall => "Recall",
        })
    }
}
