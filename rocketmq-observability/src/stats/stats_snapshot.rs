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

pub struct StatsSnapshot {
    sum: u64,
    tps: f64,
    times: u64,
    avgpt: f64,
}

impl Default for StatsSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsSnapshot {
    pub fn new() -> Self {
        StatsSnapshot {
            sum: 0,
            tps: 0.0,
            times: 0,
            avgpt: 0.0,
        }
    }

    // Getter and setter for sum
    pub fn get_sum(&self) -> u64 {
        self.sum
    }

    pub fn set_sum(&mut self, sum: u64) {
        self.sum = sum;
    }

    // Getter and setter for tps
    pub fn get_tps(&self) -> f64 {
        self.tps
    }

    pub fn set_tps(&mut self, tps: f64) {
        self.tps = tps;
    }

    // Getter and setter for avgpt
    pub fn get_avgpt(&self) -> f64 {
        self.avgpt
    }

    pub fn set_avgpt(&mut self, avgpt: f64) {
        self.avgpt = avgpt;
    }

    // Getter and setter for times
    pub fn get_times(&self) -> u64 {
        self.times
    }

    pub fn set_times(&mut self, times: u64) {
        self.times = times;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_snapshot_initializes_correctly() {
        let snapshot = StatsSnapshot::new();
        assert_eq!(snapshot.get_sum(), 0);
        assert_eq!(snapshot.get_tps(), 0.0);
        assert_eq!(snapshot.get_times(), 0);
        assert_eq!(snapshot.get_avgpt(), 0.0);
    }

    #[test]
    fn stats_snapshot_updates_sum_correctly() {
        let mut snapshot = StatsSnapshot::new();
        snapshot.set_sum(100);
        assert_eq!(snapshot.get_sum(), 100);
    }

    #[test]
    fn stats_snapshot_updates_tps_correctly() {
        let mut snapshot = StatsSnapshot::new();
        snapshot.set_tps(1.5);
        assert_eq!(snapshot.get_tps(), 1.5);
    }

    #[test]
    fn stats_snapshot_updates_avgpt_correctly() {
        let mut snapshot = StatsSnapshot::new();
        snapshot.set_avgpt(2.5);
        assert_eq!(snapshot.get_avgpt(), 2.5);
    }

    #[test]
    fn stats_snapshot_updates_times_correctly() {
        let mut snapshot = StatsSnapshot::new();
        snapshot.set_times(200);
        assert_eq!(snapshot.get_times(), 200);
    }
}
