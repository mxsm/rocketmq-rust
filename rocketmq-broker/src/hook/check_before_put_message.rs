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

use std::sync::Arc;

use rocketmq_model::common::message::MessageTrait;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::PutMessageHook;
use rocketmq_store::PutMessagePreflight;
use rocketmq_store::PutMessageResult;

use crate::util::hook_utils::HookUtils;

pub struct CheckBeforePutMessageHook {
    preflight: PutMessagePreflight,
    message_store_config: Arc<MessageStoreConfig>,
}

impl CheckBeforePutMessageHook {
    pub fn new(preflight: PutMessagePreflight, message_store_config: Arc<MessageStoreConfig>) -> Self {
        Self {
            preflight,
            message_store_config,
        }
    }
}

impl PutMessageHook for CheckBeforePutMessageHook {
    fn hook_name(&self) -> &'static str {
        "checkBeforePutMessage"
    }

    fn execute_before_put_message(&self, msg: &mut dyn MessageTrait) -> Option<PutMessageResult> {
        HookUtils::check_before_put_message(&self.preflight, &self.message_store_config, msg)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use cheetah_string::CheetahString;
    use dashmap::DashMap;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_model::common::message::MessageTrait;
    use rocketmq_store::BrokerReadStore;
    use rocketmq_store::LocalFileMessageStore;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_store::MicroBatchPolicy;
    use rocketmq_store::PutMessageHook;
    use rocketmq_store::PutMessagePreflight;
    use rocketmq_store::PutMessageStatus;
    use rocketmq_store::StoreRuntimeConfig;

    use super::CheckBeforePutMessageHook;

    fn live_preflight() -> PutMessagePreflight {
        let root = tempfile::tempdir().expect("temporary store root");
        let store = LocalFileMessageStore::new(
            Arc::new(MessageStoreConfig {
                store_path_root_dir: root.path().to_string_lossy().into_owned().into(),
                timer_wheel_enable: false,
                ..Default::default()
            }),
            MicroBatchPolicy::disabled(1).expect("valid test policy"),
            Arc::new(StoreRuntimeConfig::default()),
            Arc::new(DashMap::<CheetahString, Arc<TopicConfig>>::new()),
            None,
            false,
            crate::test_service_context("check-before-put-message"),
        )
        .expect("create test store")
        .expect("timer configuration should be valid");
        store.put_message_preflight()
    }

    #[test]
    fn check_before_put_message_hook_has_stable_identity() {
        let hook = CheckBeforePutMessageHook::new(live_preflight(), Arc::new(MessageStoreConfig::default()));

        assert_eq!(hook.hook_name(), "checkBeforePutMessage");
    }

    #[test]
    fn check_before_put_message_hook_delegates_rejection_and_acceptance() {
        let hook = CheckBeforePutMessageHook::new(live_preflight(), Arc::new(MessageStoreConfig::default()));
        let mut invalid = MessageExtBrokerInner::default();
        invalid.set_topic(CheetahString::from_string("x".repeat(128)));
        invalid.set_body(Bytes::from_static(b"body"));

        let rejection = hook
            .execute_before_put_message(&mut invalid)
            .expect("oversized topic should be rejected");
        assert_eq!(rejection.put_message_status(), PutMessageStatus::MessageIllegal);

        let mut valid = MessageExtBrokerInner::default();
        valid.set_topic(CheetahString::from_static_str("orders"));
        valid.set_body(Bytes::from_static(b"body"));
        assert!(hook.execute_before_put_message(&mut valid).is_none());
    }
}
