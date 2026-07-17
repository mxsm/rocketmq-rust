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

use crate::AckMessagePlan;
use crate::AckMessageRequest;
use crate::ChangeInvisibleDurationPlan;
use crate::ChangeInvisibleDurationRequest;
use crate::ClientSessionRegistry;
use crate::ClientSettingsSnapshot;
use crate::ProxyContextWithPrincipal;
use crate::ProxyResult;
use crate::ReceiptHandleRegistration;
use crate::ReceiveMessagePlan;
use crate::ReceiveMessageRequest;
use crate::ResourceIdentity;
use crate::SessionConfig;

const POP_RECEIPT_HANDLE_PROPERTY: &str = "POP_CK";

pub fn effective_receive_request<C, P>(
    sessions: &ClientSessionRegistry<C>,
    session_config: &SessionConfig,
    context: &ProxyContextWithPrincipal<P>,
    mut request: ReceiveMessageRequest,
) -> ProxyResult<ReceiveMessageRequest> {
    if let Some(client_id) = context.client_id() {
        if let Some(settings) = sessions.settings_for_client(client_id) {
            apply_receive_settings(&mut request, &settings);
        }
    }

    request.long_polling_timeout = clamp_duration(
        request.long_polling_timeout,
        session_config.min_long_polling_timeout(),
        session_config.max_long_polling_timeout(),
    );
    if let Some(deadline) = context.deadline() {
        request.long_polling_timeout = request.long_polling_timeout.min(deadline);
    }
    Ok(request)
}

pub fn apply_receive_settings(request: &mut ReceiveMessageRequest, settings: &ClientSettingsSnapshot) {
    let Some(subscription) = settings.subscription.as_ref() else {
        return;
    };
    if let Some(receive_batch_size) = subscription.receive_batch_size {
        request.batch_size = request.batch_size.min(receive_batch_size.max(1));
    }
    request.target.fifo |= subscription.fifo;
}

pub fn track_received_receipt_handles<C, P>(
    sessions: &ClientSessionRegistry<C>,
    session_config: &SessionConfig,
    context: &ProxyContextWithPrincipal<P>,
    request: &ReceiveMessageRequest,
    plan: &ReceiveMessagePlan,
) {
    if !(session_config.auto_renew_enabled && request.auto_renew) {
        return;
    }
    let Some(client_id) = context.client_id() else {
        return;
    };

    for message in &plan.messages {
        let Some(receipt_handle) = message.message.property(POP_RECEIPT_HANDLE_PROPERTY).map(str::to_owned) else {
            continue;
        };
        sessions.track_receipt_handle(ReceiptHandleRegistration {
            client_id: client_id.to_owned(),
            group: request.group.clone(),
            topic: ResourceIdentity::new(
                request.target.topic.namespace().to_owned(),
                message.message.topic().to_owned(),
            ),
            message_id: message.message.msg_id().to_owned(),
            receipt_handle,
            invisible_duration: message.invisible_duration,
        });
    }
}

pub fn reconcile_ack_result<C, P>(
    sessions: &ClientSessionRegistry<C>,
    context: &ProxyContextWithPrincipal<P>,
    request: &AckMessageRequest,
    plan: &AckMessagePlan,
) {
    let Some(client_id) = context.client_id() else {
        return;
    };
    for entry in &plan.entries {
        if entry.status.is_ok() {
            let _ = sessions.remove_receipt_handle_matching(
                client_id,
                &request.group,
                &request.topic,
                entry.message_id.as_str(),
                entry.receipt_handle.as_str(),
            );
        }
    }
}

pub fn reconcile_change_invisible_result<C, P>(
    sessions: &ClientSessionRegistry<C>,
    context: &ProxyContextWithPrincipal<P>,
    request: &ChangeInvisibleDurationRequest,
    plan: &ChangeInvisibleDurationPlan,
) {
    let Some(client_id) = context.client_id() else {
        return;
    };
    if !plan.status.is_ok() {
        return;
    }
    let _ = sessions.update_receipt_handle_matching(
        client_id,
        &request.group,
        &request.topic,
        request.message_id.as_str(),
        request.receipt_handle.as_str(),
        plan.receipt_handle.as_str(),
        request.invisible_duration,
    );
}

pub fn clamp_duration(duration: Duration, minimum: Duration, maximum: Duration) -> Duration {
    duration.max(minimum).min(maximum)
}
