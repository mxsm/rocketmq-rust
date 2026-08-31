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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::body::broker_stats_item::BrokerStatsItem;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Represents broker statistics over different time periods (minute, hour, day)
pub struct BrokerStatsData {
    /// Statistics for the last minute
    stats_minute: BrokerStatsItem,
    /// Statistics for the last hour
    stats_hour: BrokerStatsItem,
    /// Statistics for the last day
    stats_day: BrokerStatsItem,
}

impl BrokerStatsData {
    pub fn new(stats_minute: BrokerStatsItem, stats_hour: BrokerStatsItem, stats_day: BrokerStatsItem) -> Self {
        Self {
            stats_minute,
            stats_hour,
            stats_day,
        }
    }

    pub fn get_stats_minute(&self) -> &BrokerStatsItem {
        &self.stats_minute
    }

    pub fn set_stats_minute(&mut self, stats_minute: BrokerStatsItem) {
        self.stats_minute = stats_minute;
    }

    pub fn get_stats_hour(&self) -> &BrokerStatsItem {
        &self.stats_hour
    }

    pub fn set_stats_hour(&mut self, stats_hour: BrokerStatsItem) {
        self.stats_hour = stats_hour;
    }

    pub fn get_stats_day(&self) -> &BrokerStatsItem {
        &self.stats_day
    }

    pub fn set_stats_day(&mut self, stats_day: BrokerStatsItem) {
        self.stats_day = stats_day;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_and_serde_preserve_broker_stats_periods() {
        let stats_minute = BrokerStatsItem::new(100, 12.5, 8.3);
        let stats_hour = BrokerStatsItem::new(500, 15.0, 9.0);
        let stats_day = BrokerStatsItem::new(1000, 20.0, 10.0);
        let mut broker_stats = BrokerStatsData::new(stats_minute, stats_hour, stats_day);

        assert_eq!(broker_stats.get_stats_minute().get_sum(), 100);
        assert_eq!(broker_stats.get_stats_hour().get_sum(), 500);
        assert_eq!(broker_stats.get_stats_day().get_sum(), 1000);

        broker_stats.set_stats_minute(BrokerStatsItem::new(200, 25.0, 12.0));
        broker_stats.set_stats_hour(BrokerStatsItem::new(600, 18.0, 10.0));
        broker_stats.set_stats_day(BrokerStatsItem::new(1200, 22.0, 11.0));

        let json = serde_json::to_string(&broker_stats).unwrap();
        assert_eq!(
            json,
            r#"{"statsMinute":{"sum":200,"tps":25.0,"avgpt":12.0},"statsHour":{"sum":600,"tps":18.0,"avgpt":10.0},"statsDay":{"sum":1200,"tps":22.0,"avgpt":11.0}}"#
        );

        let decoded: BrokerStatsData = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.get_stats_minute().get_sum(), 200);
        assert_eq!(decoded.get_stats_hour().get_tps(), 18.0);
        assert_eq!(decoded.get_stats_day().get_avgpt(), 11.0);
    }
}
