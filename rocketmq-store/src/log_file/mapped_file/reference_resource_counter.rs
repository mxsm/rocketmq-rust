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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use rocketmq_common::TimeUtils::get_current_millis;

use crate::log_file::mapped_file::reference_resource::ReferenceResource;

pub struct ReferenceResourceCounter {
    ref_count: AtomicI64,
    available: AtomicBool,
    cleanup_over: AtomicBool,
    first_shutdown_timestamp: AtomicU64,
}

impl ReferenceResourceCounter {
    pub fn new() -> Self {
        Self {
            ref_count: AtomicI64::new(1),
            available: AtomicBool::new(true),
            cleanup_over: AtomicBool::new(false),
            first_shutdown_timestamp: AtomicU64::new(0),
        }
    }
}

impl ReferenceResource for ReferenceResourceCounter {
    fn hold(&self) -> bool {
        if self.is_available() {
            if self.ref_count.fetch_add(1, Ordering::AcqRel) > 0 {
                return true;
            } else {
                self.ref_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
        false
    }

    #[inline]
    fn is_available(&self) -> bool {
        self.available.load(Ordering::SeqCst)
    }

    fn shutdown(&self, interval_forcibly: u64) {
        if self
            .available
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            == Ok(true)
        {
            self.first_shutdown_timestamp
                .store(get_current_millis(), Ordering::Release);
            self.release();
        } else if self.get_ref_count() > 0
            && get_current_millis() - self.first_shutdown_timestamp.load(Ordering::Acquire)
                >= interval_forcibly
        {
            self.ref_count
                .store(-1000 - self.get_ref_count(), Ordering::Release);
            self.release();
        }
    }

    fn release(&self) {
        let value = self.ref_count.fetch_sub(1, Ordering::AcqRel) - 1;
        if value > 0 {
            return;
        }

        let cleanup_over = self.cleanup(value);
        self.cleanup_over.store(cleanup_over, Ordering::Release);
    }

    #[inline]
    fn get_ref_count(&self) -> i64 {
        self.ref_count.load(Ordering::Acquire)
    }

    #[inline]
    fn cleanup(&self, _current_ref: i64) -> bool {
        true
    }

    #[inline]
    fn is_cleanup_over(&self) -> bool {
        self.get_ref_count() <= 0 && self.cleanup_over.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn reference_resource_impl_initializes_correctly() {
        let resource = ReferenceResourceCounter::new();
        assert_eq!(resource.get_ref_count(), 1);
        assert!(resource.is_available());
        assert!(!resource.is_cleanup_over());
    }

    #[test]
    fn hold_increases_ref_count_when_available() {
        let resource = ReferenceResourceCounter::new();
        assert!(resource.hold());
        assert_eq!(resource.get_ref_count(), 2);
    }

    #[test]
    fn hold_does_not_increase_ref_count_when_not_available() {
        let resource = ReferenceResourceCounter::new();
        resource.shutdown(0);
        assert!(!resource.hold());
        assert_eq!(resource.get_ref_count(), 0);
    }

    #[test]
    fn shutdown_sets_unavailable_and_releases() {
        let resource = ReferenceResourceCounter::new();
        resource.shutdown(0);
        assert!(!resource.is_available());
        assert_eq!(resource.get_ref_count(), 0);
    }

    #[test]
    fn release_decreases_ref_count() {
        let resource = ReferenceResourceCounter::new();
        resource.hold();
        resource.release();
        assert_eq!(resource.get_ref_count(), 1);
    }

    #[test]
    fn release_triggers_cleanup_when_ref_count_zero() {
        let resource = Arc::new(ReferenceResourceCounter::new());
        let resource_clone = Arc::clone(&resource);
        resource_clone.release();
        assert!(resource.is_cleanup_over());
    }

    #[test]
    fn is_cleanup_over_returns_true_when_cleanup_complete() {
        let resource = ReferenceResourceCounter::new();
        resource.release();
        assert!(resource.is_cleanup_over());
    }
}
