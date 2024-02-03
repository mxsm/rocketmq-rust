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

use clap::Parser;
use rocketmq_broker::{broker_controller::BrokerController, command::Args};
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_rust::rocketmq;

#[rocketmq::main]
async fn main() -> anyhow::Result<()> {
    rocketmq_common::log::init_logger();
    let controller = create_broker_controller()?;
    start_broker_controller(controller)?;
    Ok(())
}

fn create_broker_controller() -> anyhow::Result<BrokerController> {
    let _args = Args::parse();
    Ok(BrokerController::new(BrokerConfig::default()))
}

fn start_broker_controller(broker_controller: BrokerController) -> anyhow::Result<()> {
    let mut broker_controller = broker_controller;
    broker_controller.start();
    Ok(())
}
