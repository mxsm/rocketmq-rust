// Copyright 2025-2026 The RocketMQ Rust Authors
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

use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_error::Result;
use rocketmq_remoting::protocol::remoting_command;

#[rocketmq_rust::main]
pub async fn main() -> Result<()> {
    rocketmq_common::log::init_logger_with_level(rocketmq_common::log::Level::INFO)?;
    const LOGO: &str = r#"
        ______           _        _  ___  ________       ______          _     _____             _             _ _
        | ___ \         | |      | | |  \/  |  _  |      | ___ \        | |   /  __ \           | |           | | |
        | |_/ /___   ___| | _____| |_| .  . | | | |______| |_/ /   _ ___| |_  | /  \/ ___  _ __ | |_ _ __ ___ | | | ___ _ __
        |    // _ \ / __| |/ / _ \ __| |\/| | | | |______|    / | | / __| __| | |    / _ \| '_ \| __| '__/ _ \| | |/ _ \ '__|
        | |\ \ (_) | (__|   <  __/ |_| |  | \ \/' /      | |\ \ |_| \__ \ |_  | \__/\ (_) | | | | |_| | | (_) | | |  __/ |
        \_| \_\___/ \___|_|\_\___|\__\_|  |_/\_/\_\      \_| \_\__,_|___/\__|  \____/\___/|_| |_|\__|_|  \___/|_|_|\___|_|
    "#;
    println!("        {}", LOGO);
    EnvUtils::put_property(
        remoting_command::REMOTING_VERSION_KEY,
        (CURRENT_VERSION as u32).to_string(),
    );
    // Initialize the controller here
    println!("RocketMQ Controller is starting...");
    // Add your initialization code here
    Ok(())
}
