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

//! CLI-owned static-topic mapping file I/O.

use std::path::Path;

use cheetah_string::CheetahString;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::FileUtils::file_to_string;
use rocketmq_common::FileUtils::string_to_file;
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper;
use rocketmq_remoting::protocol::static_topic::topic_remapping_detail_wrapper::TopicRemappingDetailWrapper;

pub fn read_mapping(path: impl AsRef<Path>) -> Option<TopicRemappingDetailWrapper> {
    file_to_string(path)
        .ok()
        .and_then(|contents| serde_json::from_str(&contents).ok())
}

pub fn write_mapping(wrapper: &TopicRemappingDetailWrapper, after: bool) -> RocketMQResult<CheetahString> {
    let temp_dir =
        EnvUtils::get_property("java.io.tmpdir").ok_or(RocketMQError::ConfigMissing { key: "java.io.tmpdir" })?;
    write_mapping_to_dir(wrapper, after, &temp_dir)
}

fn write_mapping_to_dir(
    wrapper: &TopicRemappingDetailWrapper,
    after: bool,
    temp_dir: impl AsRef<Path>,
) -> RocketMQResult<CheetahString> {
    let suffix = if after {
        topic_remapping_detail_wrapper::SUFFIX_AFTER
    } else {
        topic_remapping_detail_wrapper::SUFFIX_BEFORE
    };
    let file_name = temp_dir
        .as_ref()
        .join(format!("{}-{}{}", wrapper.topic(), wrapper.get_epoch(), suffix));
    string_to_file(&wrapper.serialize_json()?, &file_name)?;
    Ok(file_name.to_string_lossy().into_owned().into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn mapping_file_name_and_json_match_legacy_contract() {
        let wrapper = TopicRemappingDetailWrapper::new(
            "TopicA".into(),
            topic_remapping_detail_wrapper::TYPE_REMAPPING.into(),
            42,
            HashMap::new(),
            HashSet::new(),
            HashSet::new(),
        );
        let directory = std::env::temp_dir().join(format!("rocketmq-admin-static-topic-file-{}", std::process::id()));

        let before = write_mapping_to_dir(&wrapper, false, &directory).unwrap();
        let after = write_mapping_to_dir(&wrapper, true, &directory).unwrap();

        assert!(before.ends_with("TopicA-42.before"));
        assert!(after.ends_with("TopicA-42.after"));
        let decoded = read_mapping(before.as_str()).unwrap();
        assert_eq!(decoded.topic(), "TopicA");
        assert_eq!(decoded.get_epoch(), 42);
    }

    #[test]
    fn invalid_mapping_file_preserves_silent_compatibility() {
        assert!(read_mapping("this-file-does-not-exist.json").is_none());
    }
}
