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

use rocketmq_admin_core::core::admin::AdminBuilder;
use rocketmq_admin_core::core::message::MessagePullEvent;

#[derive(Debug, Clone, Default)]
pub struct TuiAdminFacade {
    namesrv_addr: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MessagePullCapture {
    pub events: Vec<MessagePullEvent>,
    pub event_limit: usize,
    pub truncated: bool,
}

impl TuiAdminFacade {
    #[allow(dead_code)]
    pub fn with_namesrv_addr(addr: impl Into<String>) -> Self {
        Self {
            namesrv_addr: Some(addr.into()),
        }
    }

    pub fn set_namesrv_addr(&mut self, addr: Option<String>) {
        self.namesrv_addr = addr.map(|addr| addr.trim().to_string()).filter(|addr| !addr.is_empty());
    }

    pub fn namesrv_addr(&self) -> Option<&str> {
        self.namesrv_addr.as_deref()
    }

    #[allow(dead_code)]
    pub fn admin_builder(&self) -> AdminBuilder {
        let builder = AdminBuilder::new();
        match &self.namesrv_addr {
            Some(addr) => builder.namesrv_addr(addr),
            None => builder,
        }
    }
}

mod operations;

#[cfg(test)]
mod tests;
