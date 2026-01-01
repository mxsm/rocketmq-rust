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

use std::cmp::Ordering;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::base::access_channel::AccessChannel;
use crate::trace::trace_bean::TraceBean;
use crate::trace::trace_type::TraceType;

#[derive(Debug, Default)]
pub struct TraceContext {
    pub trace_type: Option<TraceType>,
    pub time_stamp: u64,
    pub region_id: CheetahString,
    pub region_name: CheetahString,
    pub group_name: CheetahString,
    pub cost_time: i32,
    pub is_success: bool,
    pub request_id: CheetahString,
    pub context_code: i32,
    pub access_channel: Option<AccessChannel>,
    pub trace_beans: Option<Vec<TraceBean>>,
}

impl TraceContext {
    pub fn new() -> Self {
        TraceContext {
            trace_type: None,
            time_stamp: get_current_millis(),
            region_id: CheetahString::new(),
            region_name: CheetahString::new(),
            group_name: CheetahString::new(),
            cost_time: 0,
            is_success: true,
            request_id: CheetahString::from_string(MessageClientIDSetter::create_uniq_id()),
            context_code: 0,
            access_channel: None,
            trace_beans: None,
        }
    }
}

impl PartialEq for TraceContext {
    fn eq(&self, other: &Self) -> bool {
        self.time_stamp == other.time_stamp
    }
}

impl Eq for TraceContext {}

impl PartialOrd for TraceContext {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TraceContext {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time_stamp.cmp(&other.time_stamp)
    }
}

impl std::fmt::Display for TraceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut sb = format!(
            "TraceContext{{{:?}_{}_{}_{}_",
            self.trace_type, self.group_name, self.region_id, self.is_success
        );
        if let Some(trace_beans) = &self.trace_beans {
            for bean in trace_beans {
                sb.push_str(&format!("{}_{}_", bean.msg_id, bean.topic));
            }
        }
        sb.push('}');
        write!(f, "{sb}")
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
    use rocketmq_common::TimeUtils::get_current_millis;

    use super::*;

    #[test]
    fn trace_context_default_values() {
        let trace_context = TraceContext::default();
        assert!(trace_context.trace_type.is_none());
        assert_eq!(trace_context.time_stamp, 0);
        assert_eq!(trace_context.region_id, CheetahString::default());
        assert_eq!(trace_context.region_name, CheetahString::default());
        assert_eq!(trace_context.group_name, CheetahString::default());
        assert_eq!(trace_context.cost_time, 0);
        assert!(!trace_context.is_success);
        assert_eq!(trace_context.request_id, CheetahString::default());
        assert_eq!(trace_context.context_code, 0);
        assert!(trace_context.access_channel.is_none());
        assert!(trace_context.trace_beans.is_none());
    }

    #[test]
    fn trace_context_with_values() {
        let trace_context = TraceContext {
            trace_type: Some(TraceType::Pub),
            time_stamp: get_current_millis(),
            region_id: CheetahString::from("region_id"),
            region_name: CheetahString::from("region_name"),
            group_name: CheetahString::from("group_name"),
            cost_time: 100,
            is_success: false,
            request_id: CheetahString::from_string(MessageClientIDSetter::create_uniq_id()),
            context_code: 1,
            access_channel: Some(AccessChannel::Local),
            trace_beans: Some(vec![TraceBean::default()]),
        };
        assert_eq!(trace_context.trace_type, Some(TraceType::Pub));
        assert!(trace_context.time_stamp > 0);
        assert_eq!(trace_context.region_id, CheetahString::from("region_id"));
        assert_eq!(trace_context.region_name, CheetahString::from("region_name"));
        assert_eq!(trace_context.group_name, CheetahString::from("group_name"));
        assert_eq!(trace_context.cost_time, 100);
        assert!(!trace_context.is_success);
        assert!(!trace_context.request_id.is_empty());
        assert_eq!(trace_context.context_code, 1);
        assert_eq!(trace_context.access_channel, Some(AccessChannel::Local));
        assert!(trace_context.trace_beans.is_some());
    }

    #[test]
    fn trace_context_equality() {
        let trace_context1 = TraceContext {
            time_stamp: 12345,
            ..Default::default()
        };
        let trace_context2 = TraceContext {
            time_stamp: 12345,
            ..Default::default()
        };
        assert_eq!(trace_context1, trace_context2);
    }

    #[test]
    fn trace_context_inequality() {
        let trace_context1 = TraceContext {
            time_stamp: 12345,
            ..Default::default()
        };
        let trace_context2 = TraceContext {
            time_stamp: 67890,
            ..Default::default()
        };
        assert_ne!(trace_context1, trace_context2);
    }

    #[test]
    fn trace_context_ordering() {
        let trace_context1 = TraceContext {
            time_stamp: 12345,
            ..Default::default()
        };
        let trace_context2 = TraceContext {
            time_stamp: 67890,
            ..Default::default()
        };
        assert!(trace_context1 < trace_context2);
    }

    #[test]
    fn trace_context_display() {
        let trace_context = TraceContext {
            trace_type: Some(TraceType::Pub),
            group_name: CheetahString::from("group"),
            region_id: CheetahString::from("region"),
            is_success: true,
            trace_beans: Some(vec![TraceBean {
                msg_id: CheetahString::from("msg_id"),
                topic: CheetahString::from("topic"),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let display = format!("{}", trace_context);
        assert!(display.contains("TraceContext{Some(Pub)_group_region_true_"));
        assert!(display.contains("msg_id_topic_"));
    }
}
