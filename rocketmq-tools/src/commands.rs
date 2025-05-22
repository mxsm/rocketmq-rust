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
mod namesrv_commands;
mod topic_commands;

use clap::Parser;
use clap::Subcommand;
use tabled::settings::Style;
use tabled::Table;
use tabled::Tabled;

#[derive(Debug, Parser, Clone)]
pub struct CommonArgs {
    /// The name of the topic
    #[arg(
        short = 'n',
        long = "namesrvAddr",
        required = true,
        help = "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'"
    )]
    pub namesrv_addr: String,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(subcommand)]
    #[command(about = "Name server commands")]
    #[command(name = "nameserver")]
    NameServer(namesrv_commands::NameServerCommands),

    #[command(subcommand)]
    #[command(about = "Topic commands")]
    Topic(topic_commands::TopicCommands),

    #[command(about = "Category commands show")]
    Show(ClassificationTablePrint),
}

#[derive(Tabled, Clone)]
struct Command {
    #[tabled(rename = "Category")]
    category: &'static str,

    #[tabled(rename = "Command")]
    command: &'static str,

    #[tabled(rename = "Remark")]
    remark: &'static str,
}

#[derive(Parser)]
pub(crate) struct ClassificationTablePrint;

impl ClassificationTablePrint {
    pub fn print(&self) {
        let commands: Vec<Command> = vec![
            Command {
                category: "Topic",
                command: "allocateMQ",
                remark: "Allocate MQ.",
            },
            Command {
                category: "NameServer",
                command: "getNamesrvConfig",
                remark: "Get configs of name server.",
            },
        ];
        let mut table = Table::new(commands);
        table.with(Style::extended());
        print!("{table}");
    }
}
