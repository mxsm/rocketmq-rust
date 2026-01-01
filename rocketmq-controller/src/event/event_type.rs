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

use std::fmt;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(i16)]
#[allow(clippy::enum_variant_names)]
pub enum EventType {
    AlterSyncStateSet = 1,
    ApplyBrokerId = 2,
    ElectMaster = 3,
    ReadEvent = 4,
    CleanBrokerData = 5,
    UpdateBrokerAddress = 6,
}

impl EventType {
    pub fn from_id(id: i16) -> Option<Self> {
        match id {
            1 => Some(Self::AlterSyncStateSet),
            2 => Some(Self::ApplyBrokerId),
            3 => Some(Self::ElectMaster),
            4 => Some(Self::ReadEvent),
            5 => Some(Self::CleanBrokerData),
            6 => Some(Self::UpdateBrokerAddress),
            _ => None,
        }
    }

    #[inline]
    pub fn id(self) -> i16 {
        self as i16
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::AlterSyncStateSet => "AlterSyncStateSet",
            Self::ApplyBrokerId => "ApplyBrokerId",
            Self::ElectMaster => "ElectMaster",
            Self::ReadEvent => "ReadEvent",
            Self::CleanBrokerData => "CleanBrokerData",
            Self::UpdateBrokerAddress => "UpdateBrokerAddress",
        }
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_type_from_id() {
        assert_eq!(EventType::from_id(1), Some(EventType::AlterSyncStateSet));
        assert_eq!(EventType::from_id(2), Some(EventType::ApplyBrokerId));
        assert_eq!(EventType::from_id(3), Some(EventType::ElectMaster));
        assert_eq!(EventType::from_id(4), Some(EventType::ReadEvent));
        assert_eq!(EventType::from_id(5), Some(EventType::CleanBrokerData));
        assert_eq!(EventType::from_id(6), Some(EventType::UpdateBrokerAddress));
        assert_eq!(EventType::from_id(0), None);
        assert_eq!(EventType::from_id(7), None);
    }

    #[test]
    fn event_type_id() {
        assert_eq!(EventType::AlterSyncStateSet.id(), 1);
        assert_eq!(EventType::ApplyBrokerId.id(), 2);
        assert_eq!(EventType::ElectMaster.id(), 3);
        assert_eq!(EventType::ReadEvent.id(), 4);
        assert_eq!(EventType::CleanBrokerData.id(), 5);
        assert_eq!(EventType::UpdateBrokerAddress.id(), 6);
    }

    #[test]
    fn event_type_name() {
        assert_eq!(EventType::AlterSyncStateSet.name(), "AlterSyncStateSet");
        assert_eq!(EventType::ApplyBrokerId.name(), "ApplyBrokerId");
        assert_eq!(EventType::ElectMaster.name(), "ElectMaster");
        assert_eq!(EventType::ReadEvent.name(), "ReadEvent");
        assert_eq!(EventType::CleanBrokerData.name(), "CleanBrokerData");
        assert_eq!(EventType::UpdateBrokerAddress.name(), "UpdateBrokerAddress");
    }

    #[test]
    fn event_type_display() {
        assert_eq!(format!("{}", EventType::AlterSyncStateSet), "AlterSyncStateSet");
        assert_eq!(format!("{}", EventType::ApplyBrokerId), "ApplyBrokerId");
        assert_eq!(format!("{}", EventType::ElectMaster), "ElectMaster");
        assert_eq!(format!("{}", EventType::ReadEvent), "ReadEvent");
        assert_eq!(format!("{}", EventType::CleanBrokerData), "CleanBrokerData");
        assert_eq!(format!("{}", EventType::UpdateBrokerAddress), "UpdateBrokerAddress");
    }
}
