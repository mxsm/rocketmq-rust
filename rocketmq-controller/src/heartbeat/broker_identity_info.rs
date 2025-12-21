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

use cheetah_string::CheetahString;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BrokerIdentityInfo {
    pub cluster_name: CheetahString,
    pub broker_name: CheetahString,
    pub broker_id: Option<u64>,
}

impl BrokerIdentityInfo {
    pub fn new(
        cluster_name: impl Into<CheetahString>,
        broker_name: impl Into<CheetahString>,
        broker_id: Option<u64>,
    ) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            broker_id,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cluster_name.trim().is_empty()
            && self.broker_name.trim().is_empty()
            && self.broker_id.is_none()
    }
}

impl fmt::Display for BrokerIdentityInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BrokerIdentityInfo{{clusterName='{}', brokerName='{}', brokerId={:?}}}",
            self.cluster_name, self.broker_name, self.broker_id
        )
    }
}
