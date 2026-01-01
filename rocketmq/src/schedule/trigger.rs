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

    /// Check if this trigger should fire now (new method for delay support)
    fn should_trigger_now(&self, now: SystemTime) -> bool {
        if let Some(next_time) = self.next_execution_time(now) {
            next_time <= now
        } else {
            false
        }
    }
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

        Ok(Self { schedule, expression })
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
        self.schedule.after(&after_datetime).next().map(datetime_to_system_time)
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
    /// Mark as executed (used internally by scheduler)
    pub fn mark_executed(&mut self) {
        self.executed = true;
    }

    /// Check if already executed
    pub fn is_executed(&self) -> bool {
        self.executed
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

    fn should_trigger_now(&self, _now: SystemTime) -> bool {
        if self.executed {
            return false;
        }

        match self.start_time.elapsed() {
            Ok(elapsed) => elapsed >= self.delay,
            Err(_) => false,
        }
    }
}

/// Interval trigger with initial delay
#[derive(Debug, Clone)]
pub struct DelayedIntervalTrigger {
    interval: Duration,
    initial_delay: Duration,
    start_time: SystemTime,
    last_execution: Option<SystemTime>,
    end_time: Option<SystemTime>,
    repeat_count: Option<u32>,
    executed_count: u32,
}

impl DelayedIntervalTrigger {
    pub fn new(interval: Duration, initial_delay: Duration) -> Self {
        Self {
            interval,
            initial_delay,
            start_time: SystemTime::now(),
            last_execution: None,
            end_time: None,
            repeat_count: None,
            executed_count: 0,
        }
    }

    pub fn every_seconds_with_delay(interval_seconds: u64, delay_seconds: u64) -> Self {
        Self::new(
            Duration::from_secs(interval_seconds),
            Duration::from_secs(delay_seconds),
        )
    }

    pub fn every_minutes_with_delay(interval_minutes: u64, delay_minutes: u64) -> Self {
        Self::new(
            Duration::from_secs(interval_minutes * 60),
            Duration::from_secs(delay_minutes * 60),
        )
    }

    /// Set an end time for the trigger
    pub fn until(mut self, end_time: SystemTime) -> Self {
        self.end_time = Some(end_time);
        self
    }

    /// Set a maximum number of executions
    pub fn repeat(mut self, count: u32) -> Self {
        self.repeat_count = Some(count);
        self
    }

    /// Get the number of times this trigger has been executed
    pub fn execution_count(&self) -> u32 {
        self.executed_count
    }

    /// Mark that an execution has occurred
    pub fn mark_executed(&mut self, execution_time: SystemTime) {
        self.last_execution = Some(execution_time);
        self.executed_count += 1;
    }

    /// Check if this trigger should stop executing
    fn should_stop(&self, now: SystemTime) -> bool {
        // Check if we've reached the end time
        if let Some(end_time) = self.end_time {
            if now >= end_time {
                return true;
            }
        }

        // Check if we've reached the repeat count
        if let Some(repeat_count) = self.repeat_count {
            if self.executed_count >= repeat_count {
                return true;
            }
        }

        false
    }

    /// Calculate the first execution time
    fn first_execution_time(&self) -> SystemTime {
        self.start_time + self.initial_delay
    }
}

impl Trigger for DelayedIntervalTrigger {
    fn next_execution_time(&self, after: SystemTime) -> Option<SystemTime> {
        if self.should_stop(after) {
            return None;
        }

        match self.last_execution {
            None => {
                // First execution
                let first_time = self.first_execution_time();
                if first_time > after {
                    Some(first_time)
                } else {
                    // Calculate next proper interval if first time passed
                    let elapsed_since_first = after.duration_since(first_time).unwrap_or(Duration::ZERO);
                    let intervals_passed = (elapsed_since_first.as_millis() / self.interval.as_millis()) + 1;
                    Some(first_time + Duration::from_millis((intervals_passed * self.interval.as_millis()) as u64))
                }
            }
            Some(last) => {
                // Subsequent executions
                let next_time = last + self.interval;
                if next_time > after && !self.should_stop(next_time) {
                    Some(next_time)
                } else if next_time <= after {
                    // Calculate proper next interval if time passed
                    let elapsed = after.duration_since(last).unwrap_or(Duration::ZERO);
                    let intervals_passed = (elapsed.as_millis() / self.interval.as_millis()) + 1;
                    let calculated_next =
                        last + Duration::from_millis((intervals_passed * self.interval.as_millis()) as u64);

                    if !self.should_stop(calculated_next) {
                        Some(calculated_next)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn has_next(&self, after: SystemTime) -> bool {
        !self.should_stop(after) && self.next_execution_time(after).is_some()
    }

    fn description(&self) -> String {
        let mut desc = format!(
            "DelayedInterval: interval={:?}, initial_delay={:?}, executed_count={}",
            self.interval, self.initial_delay, self.executed_count
        );

        if let Some(end_time) = self.end_time {
            desc.push_str(&format!(", end_time={end_time:?}"));
        }

        if let Some(repeat_count) = self.repeat_count {
            desc.push_str(&format!(", repeat_count={repeat_count}"));
        }

        desc
    }

    fn should_trigger_now(&self, now: SystemTime) -> bool {
        if self.should_stop(now) {
            return false;
        }

        match self.last_execution {
            None => {
                // Check if first execution time has arrived
                let first_time = self.first_execution_time();
                now >= first_time
            }
            Some(last) => {
                // Check if next execution time has arrived
                let next_time = last + self.interval;
                now >= next_time
            }
        }
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
