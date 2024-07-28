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

#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(sync_unsafe_cell)]
#![allow(unused_variables)]

use std::borrow::Borrow;
use std::cell::SyncUnsafeCell;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;

pub use crate::common::attribute::topic_attributes as TopicAttributes;
pub use crate::common::message::message_accessor as MessageAccessor;
pub use crate::common::message::message_decoder as MessageDecoder;
use crate::error::Error;
pub use crate::thread_pool::FuturesExecutorService;
pub use crate::thread_pool::FuturesExecutorServiceBuilder;
pub use crate::thread_pool::ScheduledExecutorService;
pub use crate::thread_pool::TokioExecutorService;
pub use crate::utils::cleanup_policy_utils as CleanupPolicyUtils;
pub use crate::utils::crc32_utils as CRC32Utils;
pub use crate::utils::env_utils as EnvUtils;
pub use crate::utils::file_utils as FileUtils;
pub use crate::utils::message_utils as MessageUtils;
pub use crate::utils::parse_config_file as ParseConfigFile;
pub use crate::utils::time_utils as TimeUtils;
pub use crate::utils::util_all as UtilAll;

pub mod common;
pub mod error;
pub mod log;
mod thread_pool;
pub mod utils;

pub type Result<T> = std::result::Result<T, Error>;

pub struct WeakCellWrapper<T: ?Sized> {
    inner: Weak<SyncUnsafeCell<T>>,
}

impl<T: ?Sized> Clone for WeakCellWrapper<T> {
    fn clone(&self) -> Self {
        WeakCellWrapper {
            inner: self.inner.clone(),
        }
    }
}

impl<T> WeakCellWrapper<T> {
    pub fn upgrade(&self) -> Option<ArcCellWrapper<T>> {
        self.inner
            .upgrade()
            .map(|value| ArcCellWrapper { inner: value })
    }
}

#[derive(Default)]
pub struct ArcCellWrapper<T: ?Sized> {
    inner: Arc<SyncUnsafeCell<T>>,
}

impl<T> ArcCellWrapper<T> {
    #[allow(clippy::mut_from_ref)]
    pub fn mut_from_ref(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }

    pub fn downgrade(this: &Self) -> WeakCellWrapper<T> {
        WeakCellWrapper {
            inner: Arc::downgrade(&this.inner),
        }
    }
}

impl<T> ArcCellWrapper<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(SyncUnsafeCell::new(value)),
        }
    }
}

impl<T: ?Sized> Clone for ArcCellWrapper<T> {
    fn clone(&self) -> Self {
        ArcCellWrapper {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> AsRef<T> for ArcCellWrapper<T> {
    fn as_ref(&self) -> &T {
        unsafe { &*self.inner.get() }
    }
}

impl<T> AsMut<T> for ArcCellWrapper<T> {
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

impl<T> Deref for ArcCellWrapper<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for ArcCellWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

pub struct SyncUnsafeCellWrapper<T: ?Sized> {
    inner: SyncUnsafeCell<T>,
}

impl<T> SyncUnsafeCellWrapper<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: SyncUnsafeCell::new(value),
        }
    }
}

impl<T> SyncUnsafeCellWrapper<T> {
    #[allow(clippy::mut_from_ref)]
    pub fn mut_from_ref(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

impl<T> AsRef<T> for SyncUnsafeCellWrapper<T> {
    fn as_ref(&self) -> &T {
        unsafe { &*self.inner.get() }
    }
}

impl<T> AsMut<T> for SyncUnsafeCellWrapper<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self.inner.get_mut()
    }
}

impl<T> Deref for SyncUnsafeCellWrapper<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for SyncUnsafeCellWrapper<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

#[cfg(test)]
mod arc_cell_wrapper_tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn new_creates_arc_cell_wrapper_with_provided_value() {
        let wrapper = ArcCellWrapper::new(10);
        assert_eq!(*wrapper.as_ref(), 10);
    }

    #[test]
    fn clone_creates_a_new_instance_with_same_value() {
        let wrapper = ArcCellWrapper::new(20);
        let cloned_wrapper = wrapper.clone();
        assert_eq!(*cloned_wrapper.as_ref(), 20);
    }

    #[test]
    fn as_ref_returns_immutable_reference_to_value() {
        let wrapper = ArcCellWrapper::new(30);
        assert_eq!(*wrapper.as_ref(), 30);
    }

    #[test]
    fn as_mut_returns_mutable_reference_to_value() {
        let mut wrapper = ArcCellWrapper::new(40);
        *wrapper.as_mut() = 50;
        assert_eq!(*wrapper.as_ref(), 50);
    }

    #[test]
    fn deref_returns_reference_to_inner_value() {
        let wrapper = ArcCellWrapper::new(60);
        assert_eq!(*wrapper, 60);
    }

    #[test]
    fn deref_mut_allows_modification_of_inner_value() {
        let mut wrapper = ArcCellWrapper::new(70);
        *wrapper = 80;
        assert_eq!(*wrapper, 80);
    }

    #[test]
    fn multiple_clones_share_the_same_underlying_data() {
        let wrapper = ArcCellWrapper::new(Arc::new(90));
        let cloned_wrapper1 = wrapper.clone();
        let cloned_wrapper2 = wrapper.clone();

        assert_eq!(Arc::strong_count(wrapper.as_ref()), 1);
        assert_eq!(Arc::strong_count(cloned_wrapper1.as_ref()), 1);
        assert_eq!(Arc::strong_count(cloned_wrapper2.as_ref()), 1);
    }
}
