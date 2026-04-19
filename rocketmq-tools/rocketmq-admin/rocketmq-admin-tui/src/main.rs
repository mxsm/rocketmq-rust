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

mod action;
mod admin_facade;
mod commands;
mod event;
mod rocketmq_tui_app;
mod state;
mod ui;
mod view_model;

use rocketmq_rust::rocketmq;

use crate::rocketmq_tui_app::RocketmqTuiApp;

#[rocketmq::main]
async fn main() -> anyhow::Result<()> {
    let terminal = ratatui::try_init()?;
    let local = tokio::task::LocalSet::new();
    let result = local.run_until(RocketmqTuiApp::default().run(terminal)).await;
    ratatui::try_restore()?;
    result
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::admin::AdminBuilder;

    use crate::admin_facade::TuiAdminFacade;

    #[test]
    fn admin_facade_builds_core_admin_builder() {
        let facade = TuiAdminFacade::with_namesrv_addr("127.0.0.1:9876");

        assert_eq!(facade.namesrv_addr(), Some("127.0.0.1:9876"));

        let _builder: AdminBuilder = facade.admin_builder();
    }
}
