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

use std::fmt::Display;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum RequestType {
    Stream,
}

impl Display for RequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestType::Stream => write!(f, "STREAM"),
        }
    }
}

impl RequestType {
    pub fn value_of(code: u8) -> Option<Self> {
        match code {
            0 => Some(RequestType::Stream),
            _ => None,
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            RequestType::Stream => 0,
        }
    }
}
