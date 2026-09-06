// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use cheetah_string::CheetahString;
use clap::Parser;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::utils::crc32_utils;
use rocketmq_model::version::RocketMqVersion;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_protocol::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::DefaultRequestProcessor;
use rocketmq_transport::api::OutboundRequestOutcome;
use rocketmq_transport::api::TransportClient;
use rocketmq_transport::api::TransportClientConfig;

const REQUEST_TIMEOUT_MILLIS: u64 = 3_000;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    namesrv: CheetahString,
    #[arg(long, default_value = "ParityProbeCluster")]
    cluster: CheetahString,
    #[arg(long, default_value = "parityProbeBroker")]
    broker: CheetahString,
    #[arg(long, default_value = "127.0.0.1:21911")]
    broker_addr: CheetahString,
    #[arg(long, default_value = "NamesrvParityProbeTopic")]
    topic: CheetahString,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let owner = RuntimeOwner::plan(RuntimeConfig::server_default("namesrv-parity-probe"))
        .expect("test runtime configuration is valid")
        .build()
        .context("build parity probe runtime")?;
    let result = owner.block_on(run(owner.root_context().component("parity-probe"), args));
    let shutdown = owner.shutdown_runtime_blocking_until(ShutdownDeadline::after(Duration::from_secs(5)));
    result?;
    shutdown.context("shutdown parity probe runtime")?;
    Ok(())
}

async fn run(service: rocketmq_runtime::ChildServiceContext, args: Args) -> Result<()> {
    let client = Arc::new(
        TransportClient::builder(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            service.component("client"),
        )
        .build()
        .context("build parity probe client")?,
    );
    client.update_name_server_address_list(vec![args.namesrv.clone()]).await;
    client.start().await.context("start parity probe client")?;

    let initial = register_request(&args, 4, 1, true);
    expect_success(&client, &args.namesrv, "register", initial).await?;
    expect_route(&client, &args, "route-after-register", ResponseCode::Success).await?;

    let update = register_request(&args, 8, 2, false);
    expect_success(&client, &args.namesrv, "topic-update", update).await?;
    expect_route(&client, &args, "route-after-update", ResponseCode::Success).await?;

    let mut unregister = RemotingCommand::create_request_command(
        RequestCode::UnregisterBroker,
        UnRegisterBrokerRequestHeader {
            cluster_name: args.cluster.clone(),
            broker_addr: args.broker_addr.clone(),
            broker_name: args.broker.clone(),
            broker_id: 0,
        },
    );
    unregister.make_custom_header_to_net();
    expect_success(&client, &args.namesrv, "unregister", unregister).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match expect_route(&client, &args, "route-after-unregister", ResponseCode::TopicNotExist).await {
            Ok(()) => break,
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error),
        }
    }

    let report = client.shutdown_with_report(Duration::from_secs(3)).await;
    if !report.is_healthy() {
        bail!("parity probe client shutdown was unhealthy: {report:?}");
    }
    Ok(())
}

fn register_request(
    args: &Args,
    queues: u32,
    generation: i64,
    include_full_snapshot_sentinel: bool,
) -> RemotingCommand {
    let mut wrapper = TopicConfigAndMappingSerializeWrapper::default();
    wrapper
        .topic_config_serialize_wrapper
        .data_version
        .set_counter(generation);
    wrapper
        .topic_config_serialize_wrapper
        .data_version
        .set_timestamp(generation);
    wrapper.topic_config_serialize_wrapper.topic_config_table.insert(
        args.topic.clone(),
        TopicConfig::with_perm(args.topic.clone(), queues, queues, 6),
    );
    if include_full_snapshot_sentinel {
        let sentinel = CheetahString::from(format!("{}-FullSnapshotSentinel", args.topic));
        wrapper
            .topic_config_serialize_wrapper
            .topic_config_table
            .insert(sentinel.clone(), TopicConfig::with_perm(sentinel, 1, 1, 6));
    }
    let body = RegisterBrokerBody::new(wrapper, Vec::new()).encode(false);
    let mut request = RemotingCommand::create_request_command(
        RequestCode::RegisterBroker,
        RegisterBrokerRequestHeader::new(
            args.broker.clone(),
            args.broker_addr.clone(),
            args.cluster.clone(),
            args.broker_addr.clone(),
            0,
            Some(60_000),
            Some(false),
            false,
            crc32_utils::crc32(body.as_slice()),
        ),
    )
    .set_version(RocketMqVersion::V5_0_0 as i32)
    .set_body(body);
    request.make_custom_header_to_net();
    request
}

async fn expect_success(
    client: &TransportClient<DefaultRequestProcessor>,
    namesrv: &CheetahString,
    step: &str,
    request: RemotingCommand,
) -> Result<()> {
    let outcome = client
        .invoke_request(Some(namesrv), request, REQUEST_TIMEOUT_MILLIS)
        .await
        .with_context(|| format!("{step} request failed"))?;
    let response = match outcome {
        OutboundRequestOutcome::Response(response) => response,
        OutboundRequestOutcome::Rejected(rejection) => {
            bail!("{step} request was rejected: {rejection:?}")
        }
        OutboundRequestOutcome::Contract(contract) => {
            bail!("{step} request contract failed: {contract:?}")
        }
    };
    record_response(step, &response);
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        bail!("{step} returned {:?}", ResponseCode::from(response.code()));
    }
    Ok(())
}

async fn expect_route(
    client: &TransportClient<DefaultRequestProcessor>,
    args: &Args,
    step: &str,
    expected: ResponseCode,
) -> Result<()> {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::GetRouteinfoByTopic,
        GetRouteInfoRequestHeader::new(args.topic.clone(), Some(false)),
    );
    request.make_custom_header_to_net();
    let outcome = client
        .invoke_request(Some(&args.namesrv), request, REQUEST_TIMEOUT_MILLIS)
        .await
        .with_context(|| format!("{step} request failed"))?;
    let response = match outcome {
        OutboundRequestOutcome::Response(response) => response,
        OutboundRequestOutcome::Rejected(rejection) => {
            bail!("{step} request was rejected: {rejection:?}")
        }
        OutboundRequestOutcome::Contract(contract) => {
            bail!("{step} request contract failed: {contract:?}")
        }
    };
    record_response(step, &response);
    if ResponseCode::from(response.code()) != expected {
        bail!(
            "{step} returned {:?}, expected {expected:?}",
            ResponseCode::from(response.code())
        );
    }
    Ok(())
}

fn record_response(step: &str, response: &RemotingCommand) {
    let body = response.body().map_or(&[][..], AsRef::as_ref);
    println!(
        "step={step} responseCode={} opaque={} bodyBytes={} bodyCrc32={:08x} remark={}",
        response.code(),
        response.opaque(),
        body.len(),
        crc32_utils::crc32(body),
        response.remark().map_or("", CheetahString::as_str)
    );
}
