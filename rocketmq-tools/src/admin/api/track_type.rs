/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum TrackType {
    CONSUMED,
    ConsumedButFiltered,
    PULL,
    NotConsumeYet,
    NotOnline,
    ConsumeBroadcasting,
    UNKNOWN,
}

impl std::fmt::Display for TrackType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackType::CONSUMED => write!(f, "CONSUMED"),
            TrackType::ConsumedButFiltered => write!(f, "CONSUMED_BUT_FILTERED"),
            TrackType::PULL => write!(f, "PULL"),
            TrackType::NotConsumeYet => write!(f, "NOT_CONSUME_YET"),
            TrackType::NotOnline => write!(f, "NOT_ONLINE"),
            TrackType::ConsumeBroadcasting => write!(f, "CONSUME_BROADCASTING"),
            TrackType::UNKNOWN => write!(f, "UNKNOWN"),
        }
    }
}
