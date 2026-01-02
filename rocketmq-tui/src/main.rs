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

#![allow(dead_code)]
#![allow(unused_mut)]
#![allow(unused_variables)]

mod action;
mod rocketmq_tui_app;
mod ui;

use rocketmq_rust::rocketmq;

use crate::rocketmq_tui_app::RocketmqTuiApp;

#[rocketmq::main]
async fn main() -> anyhow::Result<()> {
    let terminal = ratatui::try_init()?;
    let result = RocketmqTuiApp::default().run(terminal).await;
    ratatui::try_restore()?;
    result
}
