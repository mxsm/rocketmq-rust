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

use std::time::Duration;

use crate::grpc::service::consumer::clamp_duration;
use crate::proto::v2;
use crate::session::TelemetryCommandKind;
use crate::status::ProxyStatusMapper;
use crate::ClientSessionRegistry;
use crate::ProxyError;
use crate::SessionConfig;

pub const DEFAULT_MAX_BODY_SIZE_BYTES: i32 = 4 * 1024 * 1024;
pub const DEFAULT_PRODUCER_MAX_ATTEMPTS: i32 = 3;
const DEFAULT_PRODUCER_BACKOFF_INITIAL_MS: u64 = 10;
const DEFAULT_PRODUCER_BACKOFF_MAX_MS: u64 = 1_000;
const DEFAULT_PRODUCER_BACKOFF_MULTIPLIER: f32 = 2.0;
pub const DEFAULT_CONSUMER_MAX_ATTEMPTS: i32 = 17;
pub const DEFAULT_CONSUMER_RECEIVE_BATCH_SIZE: i32 = 32;
const DEFAULT_CONSUMER_LONG_POLLING_TIMEOUT_MS: u64 = 20_000;
pub const DEFAULT_CONSUMER_CUSTOMIZED_BACKOFF_MS: [u64; 18] = [
    1_000, 5_000, 10_000, 30_000, 60_000, 120_000, 180_000, 240_000, 300_000, 360_000, 420_000, 480_000, 540_000,
    600_000, 1_200_000, 1_800_000, 3_600_000, 7_200_000,
];

/// Applies Core defaults to client settings without consulting a backend.
pub fn merged_settings(settings: &v2::Settings, session_config: &SessionConfig) -> v2::Settings {
    let mut merged = settings.clone();
    let mut backoff_policy = None;
    match merged.pub_sub.as_mut() {
        Some(v2::settings::PubSub::Publishing(publishing)) => {
            if publishing.max_body_size <= 0 {
                publishing.max_body_size = DEFAULT_MAX_BODY_SIZE_BYTES;
            }
            publishing.validate_message_type = true;
            backoff_policy = Some(default_producer_retry_policy());
        }
        Some(v2::settings::PubSub::Subscription(subscription)) => {
            subscription.receive_batch_size = Some(DEFAULT_CONSUMER_RECEIVE_BATCH_SIZE);
            let timeout = Duration::from_millis(DEFAULT_CONSUMER_LONG_POLLING_TIMEOUT_MS);
            subscription.long_polling_timeout = Some(duration_to_proto_duration(clamp_duration(
                timeout,
                session_config.min_long_polling_timeout(),
                session_config.max_long_polling_timeout(),
            )));
            backoff_policy = Some(default_consumer_retry_policy());
            if is_lite_client(merged.client_type) {
                subscription.lite_subscription_quota.get_or_insert(1200);
                subscription.max_lite_topic_size.get_or_insert(64);
            }
        }
        None => {}
    }
    if let Some(backoff_policy) = backoff_policy {
        merged.backoff_policy = Some(backoff_policy);
    }
    merged
}

pub fn send_reconnect_endpoints<C>(
    sessions: &ClientSessionRegistry<C>,
    client_id: &str,
    nonce: impl Into<String>,
) -> bool {
    let nonce = nonce.into();
    send_tracked_command(
        sessions,
        client_id,
        TelemetryCommandKind::ReconnectEndpoints,
        nonce.as_str(),
        v2::telemetry_command::Command::ReconnectEndpointsCommand(v2::ReconnectEndpointsCommand {
            nonce: nonce.clone(),
        }),
    )
}

pub fn send_print_thread_stack_trace<C>(
    sessions: &ClientSessionRegistry<C>,
    client_id: &str,
    nonce: impl Into<String>,
) -> bool {
    let nonce = nonce.into();
    send_tracked_command(
        sessions,
        client_id,
        TelemetryCommandKind::PrintThreadStackTrace,
        nonce.as_str(),
        v2::telemetry_command::Command::PrintThreadStackTraceCommand(v2::PrintThreadStackTraceCommand {
            nonce: nonce.clone(),
        }),
    )
}

pub fn send_verify_message<C>(
    sessions: &ClientSessionRegistry<C>,
    client_id: &str,
    nonce: impl Into<String>,
    message: v2::Message,
) -> bool {
    let nonce = nonce.into();
    send_tracked_command(
        sessions,
        client_id,
        TelemetryCommandKind::VerifyMessage,
        nonce.as_str(),
        v2::telemetry_command::Command::VerifyMessageCommand(v2::VerifyMessageCommand {
            nonce: nonce.clone(),
            message: Some(message),
        }),
    )
}

pub fn send_recover_orphaned_transaction<C>(
    sessions: &ClientSessionRegistry<C>,
    client_id: &str,
    message: v2::Message,
    transaction_id: impl Into<String>,
) -> bool {
    sessions.send_telemetry_command(
        client_id,
        command(v2::telemetry_command::Command::RecoverOrphanedTransactionCommand(
            v2::RecoverOrphanedTransactionCommand {
                message: Some(message),
                transaction_id: transaction_id.into(),
            },
        )),
    )
}

pub fn send_notify_unsubscribe_lite<C>(
    sessions: &ClientSessionRegistry<C>,
    client_id: &str,
    lite_topic: impl Into<String>,
) -> bool {
    let lite_topic = lite_topic.into();
    if !sessions.register_pending_lite_unsubscribe_notice(client_id, lite_topic.as_str()) {
        return false;
    }
    if sessions.send_telemetry_command(
        client_id,
        command(v2::telemetry_command::Command::NotifyUnsubscribeLiteCommand(
            v2::NotifyUnsubscribeLiteCommand {
                lite_topic: lite_topic.clone(),
            },
        )),
    ) {
        true
    } else {
        let _ = sessions.remove_pending_lite_unsubscribe_notice(client_id, lite_topic.as_str());
        false
    }
}

/// Handles client telemetry reports that do not require authorization.
///
/// Settings return `None` because the facade must authorize the merged settings
/// before storing them. Every other command is fully handled here.
pub fn handle_client_report<C>(
    sessions: &ClientSessionRegistry<C>,
    client_id: Option<&str>,
    command_value: &v2::telemetry_command::Command,
) -> Option<v2::TelemetryCommand> {
    match command_value {
        v2::telemetry_command::Command::Settings(_) => None,
        v2::telemetry_command::Command::ThreadStackTrace(report) => Some(match client_id {
            Some(client_id)
                if sessions.complete_print_thread_stack_trace(
                    client_id,
                    report.nonce.as_str(),
                    report.thread_stack_trace.clone(),
                ) =>
            {
                telemetry_status(ProxyStatusMapper::ok())
            }
            Some(_) => telemetry_status(ProxyStatusMapper::from_code(
                v2::Code::BadRequest,
                "client reported a thread stack trace for an unknown telemetry nonce",
            )),
            None => telemetry_status(ProxyStatusMapper::from_error(&ProxyError::ClientIdRequired)),
        }),
        v2::telemetry_command::Command::VerifyMessageResult(report) => Some(match client_id {
            Some(client_id) if sessions.complete_verify_message(client_id, report.nonce.as_str()) => {
                telemetry_status(ProxyStatusMapper::ok())
            }
            Some(_) => telemetry_status(ProxyStatusMapper::from_code(
                v2::Code::BadRequest,
                "client reported a verify-message result for an unknown telemetry nonce",
            )),
            None => telemetry_status(ProxyStatusMapper::from_error(&ProxyError::ClientIdRequired)),
        }),
        _ => Some(telemetry_status(ProxyStatusMapper::from_code(
            v2::Code::BadRequest,
            "client sent an unsupported telemetry command",
        ))),
    }
}

pub fn telemetry_status(status: v2::Status) -> v2::TelemetryCommand {
    v2::TelemetryCommand {
        status: Some(status),
        command: None,
    }
}

fn send_tracked_command<C>(
    sessions: &ClientSessionRegistry<C>,
    client_id: &str,
    kind: TelemetryCommandKind,
    nonce: &str,
    payload: v2::telemetry_command::Command,
) -> bool {
    if !sessions.register_pending_telemetry_command(client_id, kind, nonce) {
        return false;
    }
    if sessions.send_telemetry_command(client_id, command(payload)) {
        true
    } else {
        let _ = sessions.remove_pending_telemetry_command(client_id, kind, nonce);
        false
    }
}

fn command(payload: v2::telemetry_command::Command) -> v2::TelemetryCommand {
    v2::TelemetryCommand {
        status: Some(ProxyStatusMapper::ok()),
        command: Some(payload),
    }
}

fn duration_to_proto_duration(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: duration.as_secs() as i64,
        nanos: duration.subsec_nanos() as i32,
    }
}

fn default_producer_retry_policy() -> v2::RetryPolicy {
    v2::RetryPolicy {
        max_attempts: DEFAULT_PRODUCER_MAX_ATTEMPTS,
        strategy: Some(v2::retry_policy::Strategy::ExponentialBackoff(v2::ExponentialBackoff {
            initial: Some(duration_to_proto_duration(Duration::from_millis(
                DEFAULT_PRODUCER_BACKOFF_INITIAL_MS,
            ))),
            max: Some(duration_to_proto_duration(Duration::from_millis(
                DEFAULT_PRODUCER_BACKOFF_MAX_MS,
            ))),
            multiplier: DEFAULT_PRODUCER_BACKOFF_MULTIPLIER,
        })),
    }
}

fn default_consumer_retry_policy() -> v2::RetryPolicy {
    v2::RetryPolicy {
        max_attempts: DEFAULT_CONSUMER_MAX_ATTEMPTS,
        strategy: Some(v2::retry_policy::Strategy::CustomizedBackoff(v2::CustomizedBackoff {
            next: DEFAULT_CONSUMER_CUSTOMIZED_BACKOFF_MS
                .iter()
                .map(|millis| duration_to_proto_duration(Duration::from_millis(*millis)))
                .collect(),
        })),
    }
}

fn is_lite_client(client_type: Option<i32>) -> bool {
    matches!(
        client_type.and_then(|value| v2::ClientType::try_from(value).ok()),
        Some(v2::ClientType::LitePushConsumer | v2::ClientType::LiteSimpleConsumer)
    )
}
