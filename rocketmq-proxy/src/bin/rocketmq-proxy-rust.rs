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

#![recursion_limit = "256"]

use std::env;
use std::path::PathBuf;

use rocketmq_error::RocketMQError;
use rocketmq_proxy::ProxyConfig;
use rocketmq_proxy::ProxyError;
use rocketmq_proxy::ProxyMode;
use rocketmq_proxy::ProxyResult;
use rocketmq_proxy::ProxyRuntime;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ServiceContext;
use rocketmq_runtime::ServiceLifecycle;
use rocketmq_runtime::ServiceLifecycleState;
use rocketmq_runtime::ShutdownReason;
use tracing::info;

const ENTRYPOINT_MAX_BLOCKING_THREADS: usize = 64;

fn main() -> ProxyResult<()> {
    let owner = RuntimeOwner::new(proxy_runtime_config()).map_err(proxy_runtime_error("build proxy runtime"))?;
    let service_context = owner.context().service_context("rocketmq-proxy-runtime");
    let lifecycle = ServiceLifecycle::from_env("rocketmq-proxy").map_err(|error| ProxyError::Transport {
        message: format!("invalid Proxy lifecycle configuration: {error}"),
    })?;

    let run_result = owner.block_on(run(service_context, lifecycle.clone()));
    if run_result.is_err() {
        lifecycle.mark_failed();
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let shutdown_result = owner
        .shutdown_runtime_blocking_until(shutdown_request.deadline)
        .map_err(proxy_runtime_error("shutdown proxy runtime"));

    match (run_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(report)) => {
            if !report.is_healthy() {
                lifecycle.mark_failed();
                tracing::warn!(
                    report = %report.to_json(),
                    "proxy runtime shutdown report is unhealthy"
                );
                return Err(ProxyError::Transport {
                    message: "Proxy runtime shutdown report is unhealthy".to_string(),
                });
            }
            Ok(())
        }
    }
}

fn proxy_runtime_config() -> RuntimeConfig {
    let mut config = RuntimeConfig::proxy_default();
    config.max_blocking_threads = ENTRYPOINT_MAX_BLOCKING_THREADS;
    config
}

fn proxy_runtime_error(action: &'static str) -> impl FnOnce(rocketmq_runtime::RuntimeError) -> ProxyError {
    move |error| ProxyError::Transport {
        message: format!("failed to {action}: {error}"),
    }
}

async fn run(service_context: ServiceContext, lifecycle: ServiceLifecycle) -> ProxyResult<()> {
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

    lifecycle
        .start(&service_context)
        .await
        .map_err(|error| ProxyError::Transport {
            message: format!("failed to start Proxy lifecycle boundary: {error}"),
        })?;

    let bootstrap_config = build_proxy_telemetry_bootstrap_config(&config);
    let telemetry_guard =
        rocketmq_observability::logging::install_global(&bootstrap_config).map_err(|error| ProxyError::Transport {
            message: format!("failed to initialize proxy telemetry bootstrap: {error}"),
        })?;
    log_telemetry_bootstrap(&bootstrap_config, telemetry_guard.subscriber_install_status());

    info!(
        "Starting RocketMQ proxy: mode={:?}, grpc={}, remotingEnabled={}, remoting={}",
        config.mode, config.grpc.listen_addr, config.remoting.enabled, config.remoting.listen_addr
    );
    let serve_result = ProxyRuntime::builder(config)
        .with_service_context(service_context)
        .build()
        .serve_with_lifecycle(lifecycle.clone())
        .await;
    if serve_result.is_err() {
        lifecycle.mark_failed();
        lifecycle.request_shutdown(ShutdownReason::Internal);
    }
    let shutdown_request = lifecycle
        .shutdown_request()
        .unwrap_or_else(|| lifecycle.request_shutdown(ShutdownReason::Internal));
    let telemetry_report = telemetry_guard.shutdown_with_timeout(shutdown_request.deadline.remaining());
    let shutdown_result = telemetry_report.into_result().map_err(|error| ProxyError::Transport {
        message: format!("failed to shutdown proxy telemetry bootstrap: {error}"),
    });

    match (serve_result, shutdown_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(_report)) if lifecycle.state() == ServiceLifecycleState::Failed => Err(ProxyError::Transport {
            message: "Proxy lifecycle failed while observing or completing shutdown".to_string(),
        }),
        (Ok(()), Ok(_report)) => {
            lifecycle.mark_stopped();
            Ok(())
        }
    }
}

fn build_proxy_telemetry_bootstrap_config(_config: &ProxyConfig) -> rocketmq_observability::TelemetryBootstrapConfig {
    let mut observability = rocketmq_observability::ObservabilityConfig {
        service_name: "rocketmq-proxy".to_string(),
        service_namespace: "rocketmq".to_string(),
        node_type: "proxy".to_string(),
        node_id: "proxy".to_string(),
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    observability.subscriber_install_policy = rocketmq_observability::SubscriberInstallPolicy::Required;

    let mut logging = rocketmq_observability::LoggingConfig::default();
    logging.file.file_name_prefix = "rocketmq-proxy".to_string();

    rocketmq_observability::TelemetryBootstrapConfig { observability, logging }
}

fn log_telemetry_bootstrap(
    config: &rocketmq_observability::TelemetryBootstrapConfig,
    subscriber_install_status: rocketmq_observability::SubscriberInstallStatus,
) {
    info!(
        metrics_exporter = ?config.observability.metrics.exporter,
        trace_exporter = ?config.observability.traces.exporter,
        log_exporter = ?config.observability.logs.exporter,
        subscriber_installed = subscriber_install_status.installed,
        file_log_enabled = config.logging.file.enabled,
        "proxy telemetry bootstrap initialized"
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_telemetry_bootstrap_uses_required_logging_defaults() {
        let config = build_proxy_telemetry_bootstrap_config(&ProxyConfig::default());

        assert_eq!(config.observability.service_name, "rocketmq-proxy");
        assert_eq!(
            config.observability.subscriber_install_policy,
            rocketmq_observability::SubscriberInstallPolicy::Required
        );
        assert!(!config.observability.enabled);
        assert!(config.logging.enabled);
        assert!(config.logging.console.enabled);
        assert!(!config.logging.file.enabled);
        assert_eq!(config.logging.file.file_name_prefix, "rocketmq-proxy");
    }
}
