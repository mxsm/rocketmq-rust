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

//! One-page DLQ reads, verified original identities, and full-value export projection.

use super::{
    DashboardQuerySession, GpuiAdminProvider, ProviderFailure,
    messages::{dashboard_metadata, require_direct, require_topic},
    mutation_for_revision, query_for_revision, select_admin,
};
use rocketmq_admin_core::core::{
    dashboard::{DashboardMessage, DashboardMessageQuery},
    message::{DirectConsumeRequest, MessageLookupRequest, MessageRecord},
};
use rocketmq_dashboard_common::{
    DLQ_CSV_MAX_BYTES, DlqAttempt, DlqCsv, DlqCsvError, DlqCsvRow, DlqEntryState, DlqIdentityError, DlqPage, DlqQuery,
    DlqResendTarget, DlqRow, MessageConsumeCode, MessageConsumeReceipt, MessageIdentity, MessagePageExtent,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;

impl GpuiAdminProvider {
    pub async fn query_dlq(self: &Arc<Self>, revision: u64, query: DlqQuery) -> Result<DlqPage, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dlq-query", move |cancellation| async move {
            require_direct(this.snapshot_for_revision(revision)?.scope)?;
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let messages = read_page(session, &query, cancellation).await?;
            let extent = if query.message_id.is_none() && messages.len() == query.page_size as usize {
                MessagePageExtent::MoreMayExist
            } else {
                MessagePageExtent::End
            };
            let mut rows = messages
                .iter()
                .map(|message| {
                    let metadata = dashboard_metadata(message);
                    let target =
                        DlqResendTarget::resolve(&query.consumer_group, metadata.identity.clone(), &message.properties);
                    DlqRow { metadata, target }
                })
                .collect::<Vec<_>>();
            // Duplicate source IDs with different originals are ambiguous selection evidence.
            let mut identities = BTreeMap::new();
            for row in &rows {
                let value = row
                    .target
                    .as_ref()
                    .ok()
                    .map(|target| (target.original().clone(), target.consumer_group().to_string()));
                identities
                    .entry(row.metadata.identity.clone())
                    .and_modify(|previous| {
                        if *previous != value {
                            *previous = None;
                        }
                    })
                    .or_insert(value);
            }
            for row in &mut rows {
                if row.target.is_ok() && identities.get(&row.metadata.identity).is_some_and(Option::is_none) {
                    row.target = Err(DlqIdentityError::Conflict);
                }
            }
            Ok(DlqPage { rows, extent })
        })
        .await
    }

    pub async fn dlq_csv(
        self: &Arc<Self>,
        revision: u64,
        query: DlqQuery,
        expected: Vec<MessageIdentity>,
    ) -> Result<DlqCsv, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dlq-export-read", move |cancellation| async move {
            require_direct(this.snapshot_for_revision(revision)?.scope)?;
            let guard = this.query_session.read().await;
            let messages = read_page(query_for_revision(&guard, revision)?, &query, cancellation).await?;
            let mut actual = messages
                .iter()
                .map(|message| MessageIdentity {
                    topic: message.topic.clone(),
                    message_id: message.message_id.clone(),
                })
                .collect::<Vec<_>>();
            let mut expected = expected;
            actual.sort();
            expected.sort();
            if actual != expected {
                return Err(ProviderFailure::invalid_request(
                    "The DLQ page changed. Refresh it before exporting.",
                ));
            }
            let mut rows = Vec::with_capacity(messages.len());
            let mut bytes = 0usize;
            for message in messages {
                let keys = message.keys.unwrap_or_default();
                let tags = message.tags.unwrap_or_default();
                bytes = bytes
                    .saturating_add(message.body.len())
                    .saturating_add(message.message_id.len())
                    .saturating_add(message.topic.len())
                    .saturating_add(keys.len())
                    .saturating_add(tags.len());
                if bytes > DLQ_CSV_MAX_BYTES {
                    return Err(ProviderFailure::invalid_request(DlqCsvError::TooLarge.summary()));
                }
                let body = String::from_utf8(message.body)
                    .map_err(|_| ProviderFailure::invalid_request(DlqCsvError::NonUtf8.summary()))?;
                rows.push(DlqCsvRow {
                    message_id: message.message_id,
                    topic: message.topic,
                    keys,
                    tags,
                    store_timestamp: message.store_timestamp,
                    queue_id: message.queue_id,
                    queue_offset: message.queue_offset,
                    body,
                });
            }
            DlqCsv::from_rows(rows).map_err(|error| ProviderFailure::invalid_request(error.summary()))
        })
        .await
    }

    pub async fn consume_dlq(
        self: &Arc<Self>,
        revision: u64,
        target: DlqResendTarget,
    ) -> Result<DlqAttempt, ProviderFailure> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dlq-consume", move |cancellation| async move {
            let prepared = async {
                require_direct(this.snapshot_for_revision(revision)?.scope)?;
                let guard = this.query_session.read().await;
                let session = query_for_revision(&guard, revision)?;
                let source = select_admin(
                    cancellation.clone(),
                    session.lookup_message(&MessageLookupRequest {
                        topic: target.source().topic.clone(),
                        message_id: target.source().message_id.clone(),
                    }),
                )
                .await?;
                if source.topic != target.source().topic || source.message_id != target.source().message_id {
                    return Err(ProviderFailure::invalid_request("The DLQ source identity changed."));
                }
                let verified =
                    DlqResendTarget::resolve(target.consumer_group(), target.source().clone(), &source.properties)
                        .map_err(|error| ProviderFailure::invalid_request(error.summary()))?;
                if verified != target {
                    return Err(ProviderFailure::invalid_request("The original DLQ identity changed."));
                }
                let original = select_admin(
                    cancellation.clone(),
                    session.lookup_message(&MessageLookupRequest {
                        topic: target.original().topic.clone(),
                        message_id: target.original().message_id.clone(),
                    }),
                )
                .await?;
                require_topic(&original.topic, &target.original().topic)?;
                Ok(DirectConsumeRequest {
                    topic: original.topic,
                    message_id: original.message_id,
                    consumer_group: target.consumer_group().into(),
                    client_id: None,
                })
            }
            .await;
            let request = match prepared {
                Ok(request) => request,
                Err(_) => return Ok(no_receipt(DlqEntryState::CallFailed)),
            };
            let mut guard = this.mutation_session.lock().await;
            if this
                .ensure_mutation(&mut guard, revision, cancellation.clone())
                .await
                .is_err()
            {
                return Ok(no_receipt(DlqEntryState::CallFailed));
            }
            let session = match mutation_for_revision(&mut guard, revision) {
                Ok(session) => session,
                Err(_) => return Ok(no_receipt(DlqEntryState::CallFailed)),
            };
            Ok(
                match select_admin(cancellation, session.consume_message_directly(&request)).await {
                    Ok(result) => DlqAttempt::from_receipt(MessageConsumeReceipt {
                        code: MessageConsumeCode::from_reported(result.consume_result.as_deref()),
                        reported_success: result.success,
                        remark_returned: result.remark.as_ref().is_some_and(|value| !value.trim().is_empty()),
                    }),
                    Err(error) if error.is_invalid_request() => no_receipt(DlqEntryState::CallFailed),
                    Err(_) => no_receipt(DlqEntryState::Unknown),
                },
            )
        })
        .await
    }
}
fn no_receipt(state: DlqEntryState) -> DlqAttempt {
    DlqAttempt { state, receipt: None }
}

async fn read_page(
    session: &dyn DashboardQuerySession,
    query: &DlqQuery,
    cancellation: CancellationToken,
) -> Result<Vec<DashboardMessage>, ProviderFailure> {
    let messages = if let Some(id) = &query.message_id {
        let record = select_admin(
            cancellation,
            session.lookup_message(&MessageLookupRequest {
                topic: query.topic(),
                message_id: id.clone(),
            }),
        )
        .await?;
        if record.store_timestamp < query.begin || record.store_timestamp > query.end {
            return Ok(Vec::new());
        }
        vec![dashboard_record(record)]
    } else {
        select_admin(
            cancellation,
            // The native query promises an inclusive end. The legacy DLQ capability scans an
            // exclusive end offset; the existing Topic query includes that offset and filters time.
            session.query_messages(&DashboardMessageQuery {
                topic: Some(query.topic()),
                key: None,
                message_id: None,
                begin: Some(query.begin),
                end: Some(query.end),
                page_num: Some(query.page),
                page_size: Some(query.page_size),
            }),
        )
        .await?
        .items
    };
    if messages.len() > query.page_size as usize {
        return Err(ProviderFailure::invalid_data(
            "The DLQ response exceeded the requested page.",
        ));
    }
    for message in &messages {
        require_topic(&message.topic, &query.topic())?;
    }
    Ok(messages)
}
fn dashboard_record(message: MessageRecord) -> DashboardMessage {
    DashboardMessage {
        topic: message.topic,
        message_id: message.message_id,
        keys: message.keys,
        tags: message.tags,
        born_timestamp: message.born_timestamp,
        store_timestamp: message.store_timestamp,
        born_host: message.born_host,
        store_host: message.store_host,
        queue_id: message.queue_id,
        queue_offset: message.queue_offset,
        store_size: message.store_size,
        reconsume_times: message.reconsume_times,
        body_crc: message.body_crc,
        sys_flag: message.sys_flag,
        flag: message.flag,
        prepared_transaction_offset: message.prepared_transaction_offset,
        body: message.body,
        properties: message.properties,
    }
}
