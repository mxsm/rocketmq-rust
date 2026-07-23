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

#![allow(dead_code)]
#![allow(
    deprecated,
    reason = "this module implements the deprecated compatibility facade until its removal gate"
)]

use core::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;

use parking_lot::RwLock;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

/// A weak version of `ArcMut` that doesn't prevent the inner value from being dropped.
///
/// # Safety
/// The legacy compatibility accessors bypass the backing `RwLock` guards. Callers must ensure
/// that no conflicting references or data races occur when using this type across threads.
#[deprecated(
    since = "1.0.0",
    note = "use std::sync::Weak with an explicit lock or immutable state"
)]
pub struct WeakArcMut<T: ?Sized> {
    inner: Weak<RwLock<T>>,
}

// Implementation of PartialEq for WeakArcMut<T>
impl<T: PartialEq + ?Sized> PartialEq for WeakArcMut<T> {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(a), Some(b)) = (self.inner.upgrade(), other.inner.upgrade()) {
            // SAFETY: the legacy ArcMut contract requires callers to prevent concurrent mutation
            // while equality reads the shared values.
            unsafe { *a.data_ptr() == *b.data_ptr() }
        } else {
            false
        }
    }
}

// Implementation of Eq for WeakArcMut<T>
impl<T: Eq + ?Sized> Eq for WeakArcMut<T> {}

// Implementation of Hash for WeakArcMut<T>

impl<T: Hash + ?Sized> Hash for WeakArcMut<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(arc) = self.inner.upgrade() {
            // SAFETY: the legacy ArcMut contract requires callers to prevent concurrent mutation
            // while hashing the shared value.
            unsafe { (*arc.data_ptr()).hash(state) }
        }
    }
}

impl<T: ?Sized> Clone for WeakArcMut<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ?Sized> WeakArcMut<T> {
    #[inline]
    pub fn upgrade(&self) -> Option<ArcMut<T>> {
        self.inner.upgrade().map(|inner| ArcMut { inner })
    }
}

/// A mutable reference-counted pointer with interior mutability.
///
/// # Safety
/// The legacy compatibility accessors bypass the backing `RwLock` guards. Callers must ensure
/// that no conflicting references or data races occur when using this type across threads.
#[derive(Default)]
#[deprecated(
    since = "1.0.0",
    note = "use std::sync::Arc with RwLock, Mutex, atomics, or an exclusive owner"
)]
pub struct ArcMut<T: ?Sized> {
    inner: Arc<RwLock<T>>,
}

// Implementation of PartialEq for ArcMut<T>
impl<T: PartialEq + ?Sized> PartialEq for ArcMut<T> {
    fn eq(&self, other: &Self) -> bool {
        // SAFETY: the legacy ArcMut contract requires callers to prevent concurrent mutation
        // while equality reads the shared values.
        unsafe { *self.inner.data_ptr() == *other.inner.data_ptr() }
    }
}

impl<T: Eq + ?Sized> Eq for ArcMut<T> {}

impl<T: Hash> Hash for ArcMut<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Compute the hash of the inner value
        // SAFETY: the legacy ArcMut contract requires callers to prevent concurrent mutation
        // while hashing the shared value.
        unsafe { (*self.inner.data_ptr()).hash(state) }
    }
}

impl<T> ArcMut<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(value)),
        }
    }

    /// Returns a mutable reference to the inner value
    ///
    /// # Safety
    /// This is safe as long as no other mutable references exist
    /// and the caller ensures proper synchronization.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn mut_from_ref(&self) -> &mut T {
        // SAFETY: this method preserves the legacy compatibility contract documented above; the
        // caller must provide exclusive access for the returned reference's entire lifetime.
        unsafe { &mut *self.inner.data_ptr() }
    }

    #[inline]
    pub fn downgrade(this: &Self) -> WeakArcMut<T> {
        WeakArcMut {
            inner: Arc::downgrade(&this.inner),
        }
    }

    #[inline]
    /// Returns the stable backing cell.
    ///
    /// New code should use its `read` and `write` guards instead of the legacy reference escapes.
    pub fn get_inner(&self) -> &Arc<RwLock<T>> {
        &self.inner
    }

    pub fn try_unwrap(self) -> Result<T, Self> {
        match Arc::try_unwrap(self.inner) {
            Ok(cell) => Ok(cell.into_inner()),
            Err(inner) => Err(Self { inner }),
        }
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    pub fn weak_count(&self) -> usize {
        Arc::weak_count(&self.inner)
    }
}

impl<T: ?Sized> Clone for ArcMut<T> {
    #[inline]
    fn clone(&self) -> Self {
        ArcMut {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: ?Sized> AsRef<T> for ArcMut<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        // SAFETY: the legacy compatibility contract requires callers to prevent concurrent
        // mutable access for the returned reference's entire lifetime.
        unsafe { &*self.inner.data_ptr() }
    }
}

impl<T: ?Sized> AsMut<T> for ArcMut<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        // SAFETY: the legacy compatibility contract permits cloned owners, so the caller must
        // still ensure this mutable reference is exclusive across all clones.
        unsafe { &mut *self.inner.data_ptr() }
    }
}

impl<T: ?Sized> Deref for ArcMut<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T: ?Sized> DerefMut for ArcMut<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

/// Legacy shared-mutation wrapper backed by a stable `RwLock` allocation.
///
/// # Safety
/// `mut_from_ref` and `AsRef` bypass lock guards for compatibility. Callers must ensure that no
/// conflicting references or data races occur.
#[deprecated(since = "1.0.0", note = "use parking_lot::RwLock or an exclusive owner")]
pub struct SyncUnsafeCellWrapper<T: ?Sized> {
    inner: RwLock<T>,
}

impl<T> SyncUnsafeCellWrapper<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: RwLock::new(value),
        }
    }
}

impl<T> SyncUnsafeCellWrapper<T> {
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub fn mut_from_ref(&self) -> &mut T {
        // SAFETY: this method preserves the legacy compatibility contract documented above; the
        // caller must provide exclusive access for the returned reference's entire lifetime.
        unsafe { &mut *self.inner.data_ptr() }
    }
}

impl<T> AsRef<T> for SyncUnsafeCellWrapper<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        // SAFETY: the legacy compatibility contract requires callers to prevent concurrent
        // mutable access for the returned reference's entire lifetime.
        unsafe { &*self.inner.data_ptr() }
    }
}

impl<T> AsMut<T> for SyncUnsafeCellWrapper<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }
}

impl<T> Deref for SyncUnsafeCellWrapper<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> DerefMut for SyncUnsafeCellWrapper<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl<T: ?Sized + Debug> std::fmt::Debug for ArcMut<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T> Serialize for ArcMut<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // SAFETY: serialization requires callers to prevent concurrent mutation while the
        // serializer observes the shared value.
        let inner_ref = unsafe { &*self.inner.data_ptr() };
        inner_ref.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for ArcMut<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner = T::deserialize(deserializer)?;
        Ok(ArcMut::new(inner))
    }
}

#[cfg(test)]
mod arc_cell_wrapper_tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn new_creates_arc_cell_wrapper_with_provided_value() {
        let wrapper = ArcMut::new(10);
        assert_eq!(*wrapper.as_ref(), 10);
    }

    #[test]
    fn clone_creates_a_new_instance_with_same_value() {
        let wrapper = ArcMut::new(20);
        let cloned_wrapper = wrapper.clone();
        assert_eq!(*cloned_wrapper.as_ref(), 20);
    }

    #[test]
    fn as_ref_returns_immutable_reference_to_value() {
        let wrapper = ArcMut::new(30);
        assert_eq!(*wrapper.as_ref(), 30);
    }

    #[test]
    fn as_mut_returns_mutable_reference_to_value() {
        let mut wrapper = ArcMut::new(40);
        *wrapper.as_mut() = 50;
        assert_eq!(*wrapper.as_ref(), 50);
    }

    #[test]
    fn deref_returns_reference_to_inner_value() {
        let wrapper = ArcMut::new(60);
        assert_eq!(*wrapper, 60);
    }

    #[test]
    fn deref_mut_allows_modification_of_inner_value() {
        let mut wrapper = ArcMut::new(70);
        *wrapper = 80;
        assert_eq!(*wrapper, 80);
    }

    #[test]
    fn multiple_clones_share_the_same_underlying_data() {
        let wrapper = ArcMut::new(Arc::new(90));
        let cloned_wrapper1 = wrapper.clone();
        let cloned_wrapper2 = wrapper.clone();

        assert_eq!(Arc::strong_count(wrapper.as_ref()), 1);
        assert_eq!(Arc::strong_count(cloned_wrapper1.as_ref()), 1);
        assert_eq!(Arc::strong_count(cloned_wrapper2.as_ref()), 1);
    }
}
