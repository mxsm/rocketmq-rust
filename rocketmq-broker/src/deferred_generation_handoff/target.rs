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

use cheetah_string::CheetahString;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum DeferredGeneration {
    #[default]
    Legacy,
    New,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum DeferredGenerationTarget {
    Pop {
        topic: CheetahString,
        consumer_group: CheetahString,
        queue_id: i32,
    },
    Notification {
        topic: CheetahString,
        consumer_group: CheetahString,
        queue_id: i32,
    },
    Pull {
        topic: CheetahString,
        queue_id: i32,
    },
    PopLite {
        client_id: CheetahString,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeferredGenerationTransitionKind {
    LegacyTarget,
    AbandonedReplay,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeferredGenerationTransitionCandidate {
    pub(crate) target: DeferredGenerationTarget,
    pub(crate) kind: DeferredGenerationTransitionKind,
}

impl DeferredGenerationTarget {
    #[must_use]
    pub(crate) const fn pop(topic: CheetahString, consumer_group: CheetahString, queue_id: i32) -> Self {
        Self::Pop {
            topic,
            consumer_group,
            queue_id,
        }
    }

    #[must_use]
    pub(crate) const fn notification(topic: CheetahString, consumer_group: CheetahString, queue_id: i32) -> Self {
        Self::Notification {
            topic,
            consumer_group,
            queue_id,
        }
    }

    #[must_use]
    pub(crate) const fn pull(topic: CheetahString, queue_id: i32) -> Self {
        Self::Pull { topic, queue_id }
    }

    #[must_use]
    pub(crate) const fn pop_lite(client_id: CheetahString) -> Self {
        Self::PopLite { client_id }
    }

    pub(super) const fn is_pop_lite(&self) -> bool {
        matches!(self, Self::PopLite { .. })
    }

    pub(super) fn stable_name(&self) -> String {
        match self {
            Self::Pop {
                topic,
                consumer_group,
                queue_id,
            } => format!("pop:{topic}:{consumer_group}:{queue_id}"),
            Self::Notification {
                topic,
                consumer_group,
                queue_id,
            } => format!("notification:{topic}:{consumer_group}:{queue_id}"),
            Self::Pull { topic, queue_id } => format!("pull:{topic}:{queue_id}"),
            Self::PopLite { client_id } => format!("pop-lite:{client_id}"),
        }
    }
}
