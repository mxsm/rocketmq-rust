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
use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;

#[derive(Parser, Debug)]
#[command(author = "mxsm", version = "0.2.0", about = "RocketMQ CLI(Rust)")]
pub struct RootCli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(
        arg_required_else_help = true,
        author = "mxsm",
        version = "0.2.0",
        about = "read message log file"
    )]
    ReadMessageLog {
        #[arg(
            short,
            long,
            value_name = "FILE",
            default_missing_value = "None",
            help = "message log file path"
        )]
        config: Option<PathBuf>,

        #[arg(
            short = 'f',
            long,
            value_name = "FROM",
            default_missing_value = "None",
            help = "The number of data started to be read, default to read from the beginning. \
                    start from 0"
        )]
        from: Option<u32>,

        #[arg(
            short = 't',
            long,
            value_name = "TO",
            default_missing_value = "None",
            help = "The position of the data for ending the reading, defaults to reading until \
                    the end of the file."
        )]
        to: Option<u32>,
    },
}
