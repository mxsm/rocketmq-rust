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

pub struct StatsSnapshot {
    sum: u64,
    tps: f64,
    times: u64,
    avgpt: f64,
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
