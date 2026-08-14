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

// Telemetry command gRPC ingress.

use crate::proto::v2;
use crate::session::TelemetryCommandKind;
use crate::status::ProxyStatusMapper;
use crate::ClientSessionRegistry;
use crate::ProxyError;

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
