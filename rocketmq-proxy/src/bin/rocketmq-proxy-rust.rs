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

use std::env;
use std::path::PathBuf;

use rocketmq_error::RocketMQError;
use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyError;
use rocketmq_proxy::ProxyMode;
use rocketmq_proxy::ProxyResult;
use rocketmq_proxy::ProxyRuntime;
use tracing::info;

#[tokio::main]
async fn main() -> ProxyResult<()> {
    rocketmq_common::log::init_logger_with_level(rocketmq_common::log::Level::INFO)?;

    let args = Args::parse()?;
    let mut config = match args.config_file {
        Some(ref path) => ProxyConfig::load_from_file(path)?,
        None => ProxyConfig::default(),
    };
    apply_overrides(&mut config, &args)?;

    if args.print_config {
        print_config(&config);
        return Ok(());
    }

    info!(
        "Starting RocketMQ proxy: mode={:?}, grpc={}, remotingEnabled={}, remoting={}",
        config.mode, config.grpc.listen_addr, config.remoting.enabled, config.remoting.listen_addr
    );

    ProxyRuntime::new(config)
        .serve_with_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("failed to listen for ctrl-c: {error}");
            }
        })
        .await
}

struct Args {
    config_file: Option<PathBuf>,
    mode: Option<ProxyMode>,
    grpc_listen_addr: Option<String>,
    remoting_listen_addr: Option<String>,
    enable_remoting: bool,
    namesrv_addr: Option<String>,
    print_config: bool,
}

impl Args {
    fn parse() -> ProxyResult<Self> {
        let mut args = env::args().skip(1);
        let mut parsed = Args {
            config_file: None,
            mode: None,
            grpc_listen_addr: None,
            remoting_listen_addr: None,
            enable_remoting: false,
            namesrv_addr: None,
            print_config: false,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-c" | "--config" => parsed.config_file = Some(PathBuf::from(next_value(&mut args, arg.as_str())?)),
                "--mode" => parsed.mode = Some(parse_mode(next_value(&mut args, arg.as_str())?.as_str())?),
                "--grpcListenAddr" => parsed.grpc_listen_addr = Some(next_value(&mut args, arg.as_str())?),
                "--remotingListenAddr" => parsed.remoting_listen_addr = Some(next_value(&mut args, arg.as_str())?),
                "--enableRemoting" => parsed.enable_remoting = true,
                "--namesrvAddr" | "-n" => parsed.namesrv_addr = Some(next_value(&mut args, arg.as_str())?),
                "--printConfig" => parsed.print_config = true,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => {
                    return Err(ProxyError::from(RocketMQError::illegal_argument(format!(
                        "unknown proxy argument '{arg}'. Use --help for usage."
                    ))));
                }
            }
        }

        Ok(parsed)
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, name: &str) -> ProxyResult<String> {
    args.next()
        .ok_or_else(|| RocketMQError::illegal_argument(format!("missing value for {name}")))
        .map_err(Into::into)
}

fn parse_mode(value: &str) -> ProxyResult<ProxyMode> {
    match value {
        "cluster" | "Cluster" => Ok(ProxyMode::Cluster),
        "local" | "Local" => Ok(ProxyMode::Local),
        _ => Err(ProxyError::from(RocketMQError::illegal_argument(format!(
            "invalid proxy mode '{value}', expected cluster or local"
        )))),
    }
}

fn apply_overrides(config: &mut ProxyConfig, args: &Args) -> ProxyResult<()> {
    if let Some(mode) = args.mode {
        config.mode = mode;
    }
    if let Some(addr) = &args.grpc_listen_addr {
        config.grpc.listen_addr = addr.clone();
    }
    if let Some(addr) = &args.remoting_listen_addr {
        config.remoting.listen_addr = addr.clone();
    }
    if args.enable_remoting {
        config.remoting.enabled = true;
    }
    if let Some(addr) = &args.namesrv_addr {
        config.cluster.namesrv_addr = Some(addr.clone());
    }

    config.grpc.socket_addr()?;
    if config.remoting.enabled {
        config.remoting.socket_addr()?;
    }
    Ok(())
}

fn print_usage() {
    println!(
        "rocketmq-proxy-rust [-c <proxy.toml>] [--mode cluster|local] [--grpcListenAddr <host:port>] \
         [--enableRemoting] [--remotingListenAddr <host:port>] [--namesrvAddr <host:port>] [--printConfig]"
    );
}

fn print_config(config: &ProxyConfig) {
    println!("mode = {:?}", config.mode);
    println!("grpc.listenAddr = {}", config.grpc.listen_addr);
    println!("remoting.enabled = {}", config.remoting.enabled);
    println!("remoting.listenAddr = {}", config.remoting.listen_addr);
    println!("cluster.namesrvAddr = {:?}", config.cluster.namesrv_addr);
}
