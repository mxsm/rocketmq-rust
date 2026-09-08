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

//! Topic and subscription metadata pagination, wire headers, and snapshot assembly.

use super::*;
use bytes::Bytes;
use rocketmq_protocol::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_protocol::protocol::body::topic_info_wrapper::TopicConfigSerializeWrapper;
use rocketmq_protocol::protocol::header::get_all_subscription_group_request_header::GetAllSubscriptionGroupRequestHeader;
use rocketmq_protocol::protocol::header::get_all_topic_config_request_header::GetAllTopicConfigRequestHeader;
use rocketmq_protocol::protocol::DataVersion;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataPageAction {
    Continue,
    Complete,
    Restart,
}

#[derive(Default)]
struct TopicMetadataAccumulator {
    sequence: i32,
    version: Option<DataVersion>,
    configs: HashMap<CheetahString, TopicConfig>,
}

impl TopicMetadataAccumulator {
    fn merge(&mut self, mut page: TopicConfigSerializeWrapper, total: Option<i32>) -> ClientResult<MetadataPageAction> {
        validate_metadata_total(total, "totalTopicNum")?;
        let page_version = page.data_version().cloned();
        if self.version.is_some() && self.version != page_version {
            self.sequence = 0;
            self.version = page_version;
            self.configs.clear();
            return Ok(MetadataPageAction::Restart);
        }
        if self.version.is_none() {
            self.version = page_version;
        }
        let entries = page.take_topic_config_table().unwrap_or_default();
        let received = i32::try_from(entries.len())
            .map_err(|_| ClientError::response_process_failed("getAllTopicConfig", "page entry count exceeds i32"))?;
        self.sequence = self
            .sequence
            .checked_add(received)
            .ok_or_else(|| ClientError::response_process_failed("getAllTopicConfig", "metadata sequence overflow"))?;
        self.configs.extend(entries);
        metadata_page_action(self.sequence, received, total, "getAllTopicConfig")
    }

    fn finish(self) -> TopicConfigSerializeWrapper {
        TopicConfigSerializeWrapper::new(Some(self.configs), self.version)
    }
}

#[derive(Default)]
struct SubscriptionMetadataAccumulator {
    sequence: i32,
    version: Option<DataVersion>,
    groups: HashMap<CheetahString, SubscriptionGroupConfig>,
    forbidden: HashMap<CheetahString, HashMap<CheetahString, i32>>,
}

impl SubscriptionMetadataAccumulator {
    fn merge(&mut self, page: SubscriptionGroupWrapper, total: Option<i32>) -> ClientResult<MetadataPageAction> {
        validate_metadata_total(total, "totalGroupNum")?;
        let page_version = page.data_version.clone();
        if self.version.as_ref().is_some_and(|version| version != &page_version) {
            self.sequence = 0;
            self.version = Some(page_version);
            self.groups.clear();
            self.forbidden.clear();
            return Ok(MetadataPageAction::Restart);
        }
        if self.version.is_none() {
            self.version = Some(page_version);
        }
        let received = i32::try_from(page.subscription_group_table.len()).map_err(|_| {
            ClientError::response_process_failed("getAllSubscriptionGroup", "page entry count exceeds i32")
        })?;
        self.sequence = self.sequence.checked_add(received).ok_or_else(|| {
            ClientError::response_process_failed("getAllSubscriptionGroup", "metadata sequence overflow")
        })?;
        self.groups.extend(page.subscription_group_table);
        self.forbidden.extend(page.forbidden_table);
        metadata_page_action(self.sequence, received, total, "getAllSubscriptionGroup")
    }

    fn finish(self) -> SubscriptionGroupWrapper {
        SubscriptionGroupWrapper {
            subscription_group_table: self.groups,
            forbidden_table: self.forbidden,
            data_version: self.version.unwrap_or_default(),
        }
    }
}

fn validate_metadata_total(total: Option<i32>, field: &'static str) -> ClientResult<()> {
    if total.is_some_and(|value| value < 0) {
        return Err(ClientError::response_process_failed(
            "metadata pagination",
            format!("{field} cannot be negative"),
        ));
    }
    Ok(())
}

fn metadata_page_action(
    sequence: i32,
    received: i32,
    total: Option<i32>,
    operation: &'static str,
) -> ClientResult<MetadataPageAction> {
    let Some(total) = total else {
        return Ok(MetadataPageAction::Complete);
    };
    if sequence >= total.saturating_sub(1) {
        return Ok(MetadataPageAction::Complete);
    }
    if received == 0 {
        return Err(ClientError::response_process_failed(
            operation,
            "server returned an empty page before the advertised total",
        ));
    }
    Ok(MetadataPageAction::Continue)
}

fn metadata_data_version(version: Option<&DataVersion>) -> ClientResult<Option<CheetahString>> {
    let encoded = match version {
        Some(version) => serde_json::to_string(version)
            .map(CheetahString::from_string)
            .map_err(|error| ClientError::response_process_source("metadata dataVersion", error))?,
        None => CheetahString::new(),
    };
    Ok(Some(encoded))
}

fn topic_metadata_request_header(
    accumulator: &TopicMetadataAccumulator,
    page_size: i32,
) -> ClientResult<GetAllTopicConfigRequestHeader> {
    Ok(GetAllTopicConfigRequestHeader {
        topic_seq: accumulator.sequence,
        data_version: metadata_data_version(accumulator.version.as_ref())?,
        max_topic_num: Some(page_size),
    })
}

fn subscription_metadata_request_header(
    accumulator: &SubscriptionMetadataAccumulator,
    page_size: i32,
) -> ClientResult<GetAllSubscriptionGroupRequestHeader> {
    Ok(GetAllSubscriptionGroupRequestHeader {
        group_seq: accumulator.sequence,
        data_version: metadata_data_version(accumulator.version.as_ref())?,
        max_group_num: Some(page_size),
    })
}

fn metadata_total(response: &RemotingCommand, field: &'static str) -> ClientResult<Option<i32>> {
    response
        .ext_fields()
        .and_then(|fields| fields.get(field))
        .map(|value| {
            value
                .parse::<i32>()
                .map_err(|error| ClientError::response_process_source("metadata pagination response", error))
        })
        .transpose()
}

fn metadata_remaining_timeout(started: Instant, timeout_millis: u64, operation: &'static str) -> ClientResult<u64> {
    let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    timeout_millis
        .checked_sub(elapsed)
        .filter(|remaining| *remaining > 0)
        .ok_or(ClientError::timeout(operation, timeout_millis))
}

#[derive(Clone, Copy)]
enum MetadataKind {
    Topics,
    Subscriptions,
}

impl MetadataKind {
    fn operation(self) -> &'static str {
        match self {
            Self::Topics => "getAllTopicConfig",
            Self::Subscriptions => "getAllSubscriptionGroup",
        }
    }

    fn transport_operation(self) -> &'static str {
        match self {
            Self::Topics => "get_all_topic_config",
            Self::Subscriptions => "get_all_subscription_group_config",
        }
    }

    fn total_field(self) -> &'static str {
        match self {
            Self::Topics => "totalTopicNum",
            Self::Subscriptions => "totalGroupNum",
        }
    }
}

// A version restart consumes the original operation budget, never a fresh timeout.
struct MetadataRead {
    kind: MetadataKind,
    started: Instant,
    timeout_millis: u64,
    page_size: i32,
}

impl MetadataRead {
    fn new(kind: MetadataKind, api: &MQClientAPIImpl, timeout_millis: u64) -> ClientResult<Self> {
        let started = Instant::now();
        let page_size = i32::try_from(api.client_config.max_page_size_in_get_metadata)
            .map_err(|source| ClientError::config_invalid_source("max_page_size_in_get_metadata", true, source))?;
        Ok(Self {
            kind,
            started,
            timeout_millis,
            page_size,
        })
    }

    async fn fetch(
        &self,
        api: &MQClientAPIImpl,
        addr: &CheetahString,
        request: RemotingCommand,
    ) -> ClientResult<(Option<i32>, Bytes)> {
        let remaining = metadata_remaining_timeout(self.started, self.timeout_millis, self.kind.operation())?;
        let outcome = api.remoting_client.invoke_request(Some(addr), request, remaining).await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    self.kind.transport_operation(),
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    self.kind.transport_operation(),
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(ClientError::from_shared(error.into_shared_error())),
        };
        self.decode_response(response)
    }

    fn decode_response(&self, mut response: RemotingCommand) -> ClientResult<(Option<i32>, Bytes)> {
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().map_or(String::new(), ToString::to_string)
            ));
        }
        let total = metadata_total(&response, self.kind.total_field())?;
        let body = response.take_body().ok_or_else(|| {
            ClientError::response_process_failed(self.kind.operation(), "successful response has no body")
        })?;
        Ok((total, body))
    }
}

impl MQClientAPIImpl {
    pub(crate) async fn get_all_topic_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> ClientResult<TopicConfigSerializeWrapper> {
        let read = MetadataRead::new(MetadataKind::Topics, self, timeout_millis)?;
        let mut accumulator = TopicMetadataAccumulator::default();
        loop {
            let request = self.create_request_command(
                RequestCode::GetAllTopicConfig,
                topic_metadata_request_header(&accumulator, read.page_size)?,
            );
            let (total, body) = read.fetch(self, addr, request).await?;
            let page = TopicConfigSerializeWrapper::decode(body.as_ref())?;
            match accumulator.merge(page, total)? {
                MetadataPageAction::Complete => return Ok(accumulator.finish()),
                MetadataPageAction::Continue | MetadataPageAction::Restart => {}
            }
        }
    }

    pub(crate) async fn get_all_subscription_group_config(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> ClientResult<SubscriptionGroupWrapper> {
        let read = MetadataRead::new(MetadataKind::Subscriptions, self, timeout_millis)?;
        let mut accumulator = SubscriptionMetadataAccumulator::default();
        loop {
            let request = self.create_request_command(
                RequestCode::GetAllSubscriptionGroupConfig,
                subscription_metadata_request_header(&accumulator, read.page_size)?,
            );
            let (total, body) = read.fetch(self, addr, request).await?;
            let page = SubscriptionGroupWrapper::decode(body.as_ref())?;
            match accumulator.merge(page, total)? {
                MetadataPageAction::Complete => return Ok(accumulator.finish()),
                MetadataPageAction::Continue | MetadataPageAction::Restart => {}
            }
        }
    }
}

#[cfg(test)]
mod metadata_pagination_tests {
    use super::*;

    #[test]
    fn metadata_response_validation_preserves_legacy_bodies_and_rejects_incomplete_pages() {
        for kind in [MetadataKind::Topics, MetadataKind::Subscriptions] {
            let read = MetadataRead {
                kind,
                started: Instant::now(),
                timeout_millis: 1_000,
                page_size: 37,
            };
            let response = RemotingCommand::create_success_response_command().set_body("{}");
            let (total, body) = read.decode_response(response).unwrap();
            assert_eq!(total, None);
            assert_eq!(body.as_ref(), b"{}");

            assert!(read
                .decode_response(RemotingCommand::create_success_response_command())
                .is_err());
            let mut response = RemotingCommand::create_success_response_command().set_body("{}");
            response.add_ext_field(kind.total_field(), "invalid");
            assert!(read.decode_response(response).is_err());
            let response =
                RemotingCommand::create_response_command_with_code(ResponseCode::NoPermission).set_body("{}");
            assert!(read.decode_response(response).is_err());
        }
    }

    #[test]
    fn metadata_version_restarts_share_the_original_timeout() {
        let started = Instant::now() - Duration::from_secs(2);
        for kind in [MetadataKind::Topics, MetadataKind::Subscriptions] {
            let error = metadata_remaining_timeout(started, 1_000, kind.operation()).unwrap_err();
            assert!(error.is(&rocketmq_error::CORE_OPERATION_TIMED_OUT));
        }
    }

    fn topic_page(version: &DataVersion, names: &[&str]) -> TopicConfigSerializeWrapper {
        TopicConfigSerializeWrapper::new(
            Some(
                names
                    .iter()
                    .map(|name| (CheetahString::from_slice(name), TopicConfig::default()))
                    .collect(),
            ),
            Some(version.clone()),
        )
    }

    fn group_page(version: &DataVersion, names: &[&str]) -> SubscriptionGroupWrapper {
        SubscriptionGroupWrapper {
            subscription_group_table: names
                .iter()
                .map(|name| (CheetahString::from_slice(name), SubscriptionGroupConfig::default()))
                .collect(),
            forbidden_table: HashMap::new(),
            data_version: version.clone(),
        }
    }

    #[test]
    fn configured_page_size_enters_topic_and_group_headers() {
        let topic = topic_metadata_request_header(&TopicMetadataAccumulator::default(), 37).expect("topic header");
        let group = subscription_metadata_request_header(&SubscriptionMetadataAccumulator::default(), 37)
            .expect("group header");

        assert_eq!(topic.topic_seq, 0);
        assert_eq!(topic.max_topic_num, Some(37));
        assert_eq!(group.group_seq, 0);
        assert_eq!(group.max_group_num, Some(37));
    }

    #[test]
    fn topic_pages_are_assembled_and_old_server_fallback_is_complete() {
        let version = DataVersion::with_values(1, 2, 3);
        let mut paged = TopicMetadataAccumulator::default();
        assert_eq!(
            paged
                .merge(topic_page(&version, &["a", "b"]), Some(4))
                .expect("first page"),
            MetadataPageAction::Continue
        );
        assert_eq!(
            paged
                .merge(topic_page(&version, &["c", "d"]), Some(4))
                .expect("second page"),
            MetadataPageAction::Complete
        );
        assert_eq!(paged.finish().topic_config_table().map(HashMap::len), Some(4));

        let mut legacy = TopicMetadataAccumulator::default();
        assert_eq!(
            legacy
                .merge(topic_page(&version, &["all"]), None)
                .expect("legacy response"),
            MetadataPageAction::Complete
        );
    }

    #[test]
    fn version_change_restarts_group_pagination_without_mixing_snapshots() {
        let first = DataVersion::with_values(1, 2, 3);
        let second = DataVersion::with_values(2, 3, 4);
        let mut groups = SubscriptionMetadataAccumulator::default();
        assert_eq!(
            groups
                .merge(group_page(&first, &["a", "b"]), Some(5))
                .expect("first page"),
            MetadataPageAction::Continue
        );
        assert_eq!(
            groups
                .merge(group_page(&second, &["stale"]), Some(5))
                .expect("version transition"),
            MetadataPageAction::Restart
        );
        assert_eq!(groups.sequence, 0);
        assert!(groups.groups.is_empty());
        assert_eq!(groups.version.as_ref(), Some(&second));
    }
}
