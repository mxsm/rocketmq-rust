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

use std::collections::HashMap;
use std::env;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hmac::digest::KeyInit;
use hmac::Hmac;
use hmac::Mac;
use rocketmq_proxy::v2;
use rocketmq_proxy::v2::messaging_service_client::MessagingServiceClient;
use sha1::Sha1;
use tonic::metadata::MetadataValue;
use tonic::Request;

type HmacSha1 = Hmac<Sha1>;

fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}"))
}

fn positive_usize(name: &str) -> Result<usize, String> {
    let raw = required_env(name)?;
    raw.parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn authenticated_request<T>(
    value: T,
    client_id: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<Request<T>, String> {
    let date_time = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let mut mac = HmacSha1::new_from_slice(secret_key.as_bytes()).map_err(|error| error.to_string())?;
    mac.update(date_time.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    let authorization =
        format!("MQv2-HMAC-SHA1 Credential={access_key}, SignedHeaders=x-mq-date-time, Signature={signature}");

    let mut request = Request::new(value);
    request.metadata_mut().insert(
        "x-mq-date-time",
        MetadataValue::try_from(date_time).map_err(|error| error.to_string())?,
    );
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(authorization).map_err(|error| error.to_string())?,
    );
    request.metadata_mut().insert(
        "channel-id",
        MetadataValue::try_from(client_id).map_err(|error| error.to_string())?,
    );
    request.metadata_mut().insert(
        "x-mq-client-id",
        MetadataValue::try_from(client_id).map_err(|error| error.to_string())?,
    );
    Ok(request)
}

fn resource(name: &str) -> v2::Resource {
    v2::Resource {
        resource_namespace: String::new(),
        name: name.to_owned(),
    }
}

fn receive_request(topic: &str, group: &str, queue_id: i32) -> v2::ReceiveMessageRequest {
    v2::ReceiveMessageRequest {
        group: Some(resource(group)),
        message_queue: Some(v2::MessageQueue {
            topic: Some(resource(topic)),
            id: queue_id,
            permission: v2::Permission::ReadWrite as i32,
            broker: None,
            accept_message_types: vec![v2::MessageType::Normal as i32],
        }),
        filter_expression: None,
        batch_size: 1,
        invisible_duration: Some(prost_types::Duration { seconds: 30, nanos: 0 }),
        auto_renew: false,
        long_polling_timeout: Some(prost_types::Duration { seconds: 20, nanos: 0 }),
        attempt_id: None,
    }
}

fn send_request(topic: &str, run_token: &str, sequence: usize) -> v2::SendMessageRequest {
    v2::SendMessageRequest {
        messages: vec![v2::Message {
            topic: Some(resource(topic)),
            user_properties: HashMap::new(),
            system_properties: Some(v2::SystemProperties {
                message_id: format!("{run_token}-{sequence}"),
                body_encoding: v2::Encoding::Identity as i32,
                message_type: v2::MessageType::Normal as i32,
                ..Default::default()
            }),
            body: Bytes::from(format!("proxy-live-{sequence}")),
        }],
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = required_env("PROXY_ENDPOINT")?;
    let topic = required_env("FAULT_TOPIC")?;
    let group = required_env("FAULT_GROUP")?;
    let run_token = required_env("FAULT_RUN_TOKEN")?;
    let access_key = required_env("ROCKETMQ_ACL_ACCESS_KEY")?;
    let secret_key = required_env("ROCKETMQ_ACL_SECRET_KEY")?;
    let long_pollers = positive_usize("LONG_POLLERS")?;
    let ordered_sends = positive_usize("ORDERED_SENDS")?;
    let client = MessagingServiceClient::connect(endpoint.clone()).await?;

    let mut receives = FuturesUnordered::new();
    for index in 0..long_pollers {
        let mut receive_client = client.clone();
        let request = authenticated_request(
            receive_request(&topic, &group, (index % 8) as i32),
            &format!("{run_token}-receive-{index}"),
            &access_key,
            &secret_key,
        )?;
        receives.push(tokio::spawn(async move {
            match receive_client.receive_message(request).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    while let Some(item) = stream.next().await {
                        item?;
                    }
                    Ok::<_, tonic::Status>("completed")
                }
                Err(status) => Err(status),
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut send_client = client.clone();
    let mut sends_ok = 0usize;
    for sequence in 0..ordered_sends {
        let request = authenticated_request(
            send_request(&topic, &run_token, sequence),
            &format!("{run_token}-send"),
            &access_key,
            &secret_key,
        )?;
        let response = tokio::time::timeout(Duration::from_secs(30), send_client.send_message(request)).await??;
        let payload = response.into_inner();
        if payload.entries.len() == 1
            && payload.entries[0]
                .status
                .as_ref()
                .is_some_and(|status| status.code == v2::Code::Ok as i32)
        {
            sends_ok += 1;
            println!("send-sequence={sequence} status=OK");
        } else {
            println!("send-sequence={sequence} status={payload:?}");
        }
    }

    let mut receive_completed = 0usize;
    let mut receive_overloaded = 0usize;
    let mut receive_other_error = 0usize;
    while let Some(joined) = receives.next().await {
        match joined? {
            Ok(_) => receive_completed += 1,
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::ResourceExhausted | tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) =>
            {
                receive_overloaded += 1;
                println!("receive-overload code={:?} message={}", status.code(), status.message());
            }
            Err(status) => {
                receive_other_error += 1;
                println!("receive-error code={:?} message={}", status.code(), status.message());
            }
        }
    }
    println!(
        "proxy-live-summary long_pollers={long_pollers} receive_completed={receive_completed} \
         receive_overloaded={receive_overloaded} receive_other_error={receive_other_error} \
         sends_ok={sends_ok} ordered_sends={ordered_sends}"
    );

    if sends_ok != ordered_sends || receive_overloaded == 0 {
        return Err("live Proxy load did not preserve send progress and produce typed overload".into());
    }
    Ok(())
}
