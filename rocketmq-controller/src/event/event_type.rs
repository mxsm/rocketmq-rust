//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

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
