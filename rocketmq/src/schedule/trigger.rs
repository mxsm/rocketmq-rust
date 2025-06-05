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

use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use chrono::DateTime;
use chrono::Utc;
use cron::Schedule;

use crate::schedule::SchedulerError;

/// Trigger trait for determining when tasks should run
pub trait Trigger: Send + Sync {
    /// Get the next execution time after the given time
    fn next_execution_time(&self, after: SystemTime) -> Option<SystemTime>;

    /// Check if this trigger will fire again
    fn has_next(&self, after: SystemTime) -> bool;

    /// Get trigger description
    fn description(&self) -> String;
}

/// Cron-based trigger
#[derive(Debug, Clone)]
pub struct CronTrigger {
    schedule: Schedule,
    expression: String,
}

impl CronTrigger {
    pub fn new(expression: impl Into<String>) -> Result<Self, SchedulerError> {
        let expression = expression.into();
        let schedule = Schedule::from_str(&expression)
            .map_err(|e| SchedulerError::TriggerError(format!("Invalid cron expression: {e}")))?;

        Ok(Self {
            schedule,
            expression,
        })
    }

    /// Create a trigger that fires every minute
    pub fn every_minute() -> Result<Self, SchedulerError> {
        Self::new("0 * * * * *")
    }

    /// Create a trigger that fires every hour at minute 0
    pub fn hourly() -> Result<Self, SchedulerError> {
        Self::new("0 0 * * * *")
    }

    /// Create a trigger that fires daily at midnight
    pub fn daily() -> Result<Self, SchedulerError> {
        Self::new("0 0 0 * * *")
    }

    /// Create a trigger that fires weekly on Sunday at midnight
    pub fn weekly() -> Result<Self, SchedulerError> {
        Self::new("0 0 0 * * SUN")
    }

    /// Create a trigger that fires monthly on the 1st at midnight
    pub fn monthly() -> Result<Self, SchedulerError> {
        Self::new("0 0 0 1 * *")
    }
}

impl Trigger for CronTrigger {
    fn next_execution_time(&self, after: SystemTime) -> Option<SystemTime> {
        let after_datetime = system_time_to_datetime(after);
        self.schedule
            .after(&after_datetime)
            .next()
            .map(datetime_to_system_time)
    }

    fn has_next(&self, after: SystemTime) -> bool {
        self.next_execution_time(after).is_some()
    }

    fn description(&self) -> String {
        format!("Cron: {}", self.expression)
    }
}

/// Interval-based trigger
#[derive(Debug, Clone)]
pub struct IntervalTrigger {
    interval: Duration,
    start_time: Option<SystemTime>,
    end_time: Option<SystemTime>,
    repeat_count: Option<u32>,
    executed_count: u32,
}

impl IntervalTrigger {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            start_time: None,
            end_time: None,
            repeat_count: None,
            executed_count: 0,
        }
    }

    pub fn with_start_time(mut self, start_time: SystemTime) -> Self {
        self.start_time = Some(start_time);
        self
    }

    pub fn with_end_time(mut self, end_time: SystemTime) -> Self {
        self.end_time = Some(end_time);
        self
    }

    pub fn with_repeat_count(mut self, count: u32) -> Self {
        self.repeat_count = Some(count);
        self
    }

    pub fn every_seconds(seconds: u64) -> Self {
        Self::new(Duration::from_secs(seconds))
    }

    pub fn every_minutes(minutes: u64) -> Self {
        Self::new(Duration::from_secs(minutes * 60))
    }

    pub fn every_hours(hours: u64) -> Self {
        Self::new(Duration::from_secs(hours * 3600))
    }

    fn increment_executed_count(&mut self) {
        self.executed_count += 1;
    }
}

impl Trigger for IntervalTrigger {
    fn next_execution_time(&self, after: SystemTime) -> Option<SystemTime> {
        // Check repeat count limit
        if let Some(max_count) = self.repeat_count {
            if self.executed_count >= max_count {
                return None;
            }
        }

        let start = self.start_time.unwrap_or(after);

        // If we haven't started yet, return start time
        if after < start {
            return Some(start);
        }

        let next_time = if self.executed_count == 0 {
            start
        } else {
            after + self.interval
        };

        // Check end time limit
        if let Some(end) = self.end_time {
            if next_time > end {
                return None;
            }
        }

        Some(next_time)
    }

    fn has_next(&self, after: SystemTime) -> bool {
        self.next_execution_time(after).is_some()
    }

    fn description(&self) -> String {
        format!("Interval: {:?}", self.interval)
    }
}

/// One-time delay trigger
#[derive(Debug, Clone)]
pub struct DelayTrigger {
    delay: Duration,
    start_time: SystemTime,
    executed: bool,
}

impl DelayTrigger {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            start_time: SystemTime::now(),
            executed: false,
        }
    }

    pub fn after_seconds(seconds: u64) -> Self {
        Self::new(Duration::from_secs(seconds))
    }

    pub fn after_minutes(minutes: u64) -> Self {
        Self::new(Duration::from_secs(minutes * 60))
    }

    pub fn after_hours(hours: u64) -> Self {
        Self::new(Duration::from_secs(hours * 3600))
    }

    pub fn at_time(execution_time: SystemTime) -> Self {
        let now = SystemTime::now();
        let delay = execution_time.duration_since(now).unwrap_or(Duration::ZERO);
        Self {
            delay,
            start_time: now,
            executed: false,
        }
    }
}

impl Trigger for DelayTrigger {
    fn next_execution_time(&self, _after: SystemTime) -> Option<SystemTime> {
        if self.executed {
            None
        } else {
            Some(self.start_time + self.delay)
        }
    }

    fn has_next(&self, _after: SystemTime) -> bool {
        !self.executed
    }

    fn description(&self) -> String {
        format!("Delay: {:?}", self.delay)
    }
}

// Helper functions for time conversion
fn system_time_to_datetime(system_time: SystemTime) -> DateTime<Utc> {
    /*let duration = system_time.duration_since(UNIX_EPOCH).unwrap();
    let naive = NaiveDateTime::from_timestamp(duration.as_secs() as i64, duration.subsec_nanos());
    DateTime::from_utc(naive, Utc)*/
    DateTime::from(system_time)
}

fn datetime_to_system_time(datetime: DateTime<Utc>) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(datetime.timestamp() as u64)
}
