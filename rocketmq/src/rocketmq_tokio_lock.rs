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

use std::time::Duration;

pub struct RocketMQTokioRwLock<T: ?Sized> {
    lock: tokio::sync::RwLock<T>,
}

impl<T> Default for RocketMQTokioRwLock<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: ?Sized> RocketMQTokioRwLock<T> {
    /// Creates a new `RocketMQTokioRwLock` instance containing the given data.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected by the read-write lock.
    ///
    /// # Returns
    ///
    /// A new `RocketMQTokioRwLock` instance.
    pub fn new(data: T) -> Self
    where
        T: Sized,
    {
        Self {
            lock: tokio::sync::RwLock::new(data),
        }
    }

    /// Creates a new `RocketMQTokioRwLock` instance from an existing `tokio::sync::RwLock`.
    ///
    /// # Arguments
    ///
    /// * `lock` - An existing `tokio::sync::RwLock` to be used.
    ///
    /// # Returns
    ///
    /// A new `RocketMQTokioRwLock` instance.
    pub fn new_rw_lock(lock: tokio::sync::RwLock<T>) -> Self
    where
        T: Sized,
    {
        Self { lock }
    }

    /// Acquires a read lock asynchronously, blocking the current task until it is able to do so.
    ///
    /// # Returns
    ///
    /// A `RwLockReadGuard` that releases the read lock when dropped.
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, T> {
        self.lock.read().await
    }

    /// Acquires a write lock asynchronously, blocking the current task until it is able to do so.
    ///
    /// # Returns
    ///
    /// A `RwLockWriteGuard` that releases the write lock when dropped.
    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, T> {
        self.lock.write().await
    }

    /// Attempts to acquire a read lock asynchronously without blocking.
    ///
    /// # Returns
    ///
    /// An `Option` containing a `RwLockReadGuard` if the read lock was successfully acquired, or
    /// `None` if the lock is already held.
    pub async fn try_read(&self) -> Option<tokio::sync::RwLockReadGuard<'_, T>> {
        self.lock.try_read().ok()
    }

    /// Attempts to acquire a write lock asynchronously without blocking.
    ///
    /// # Returns
    ///
    /// An `Option` containing a `RwLockWriteGuard` if the write lock was successfully acquired, or
    /// `None` if the lock is already held.
    pub async fn try_write(&self) -> Option<tokio::sync::RwLockWriteGuard<'_, T>> {
        self.lock.try_write().ok()
    }

    /// Attempts to acquire a read lock asynchronously, blocking for up to the specified timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum duration to wait for the read lock.
    ///
    /// # Returns
    ///
    /// An `Option` containing a `RwLockReadGuard` if the read lock was successfully acquired within
    /// the timeout, or `None` if the timeout expired.
    pub async fn try_read_timeout(&self, timeout: Duration) -> Option<tokio::sync::RwLockReadGuard<'_, T>> {
        (tokio::time::timeout(timeout, self.lock.read()).await).ok()
    }

    /// Attempts to acquire a write lock asynchronously, blocking for up to the specified timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum duration to wait for the write lock.
    ///
    /// # Returns
    ///
    /// An `Option` containing a `RwLockWriteGuard` if the write lock was successfully acquired
    /// within the timeout, or `None` if the timeout expired.
    pub async fn try_write_timeout(&self, timeout: Duration) -> Option<tokio::sync::RwLockWriteGuard<'_, T>> {
        (tokio::time::timeout(timeout, self.lock.write()).await).ok()
    }
}

pub struct RocketMQTokioMutex<T: ?Sized> {
    lock: tokio::sync::Mutex<T>,
}

impl<T: ?Sized> RocketMQTokioMutex<T> {
    /// Creates a new `RocketMQTokioMutex` instance containing the given data.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to be protected by the mutex.
    ///
    /// # Returns
    ///
    /// A new `RocketMQTokioMutex` instance.
    pub fn new(data: T) -> Self
    where
        T: Sized,
    {
        Self {
            lock: tokio::sync::Mutex::new(data),
        }
    }

    /// Acquires the lock asynchronously, blocking the current task until it is able to do so.
    ///
    /// # Returns
    ///
    /// A `MutexGuard` that releases the lock when dropped.
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, T> {
        self.lock.lock().await
    }

    /// Attempts to acquire the lock asynchronously without blocking.
    ///
    /// # Returns
    ///
    /// An `Option` containing a `MutexGuard` if the lock was successfully acquired, or `None` if
    /// the lock is already held.
    pub async fn try_lock(&self) -> Option<tokio::sync::MutexGuard<'_, T>> {
        self.lock.try_lock().ok()
    }

    /// Attempts to acquire the lock asynchronously, blocking for up to the specified timeout.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum duration to wait for the lock.
    ///
    /// # Returns
    ///
    /// An `Option` containing a `MutexGuard` if the lock was successfully acquired within the
    /// timeout, or `None` if the timeout expired.
    pub async fn try_lock_timeout(&self, timeout: Duration) -> Option<tokio::sync::MutexGuard<'_, T>> {
        (tokio::time::timeout(timeout, self.lock.lock()).await).ok()
    }
}

impl<T> Default for RocketMQTokioMutex<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::RwLock;

    use super::*;

    #[tokio::test]
    async fn new_creates_instance() {
        let lock = RocketMQTokioRwLock::new(5);
        assert_eq!(*lock.read().await, 5);
    }

    #[tokio::test]
    async fn new_rw_lock_creates_instance() {
        let rw_lock = RwLock::new(5);
        let lock = RocketMQTokioRwLock::new_rw_lock(rw_lock);
        assert_eq!(*lock.read().await, 5);
    }

    #[tokio::test]
    async fn read_locks_and_reads() {
        let lock = RocketMQTokioRwLock::new(5);
        let read_guard = lock.read().await;
        assert_eq!(*read_guard, 5);
    }

    #[tokio::test]
    async fn write_locks_and_writes() {
        let lock = RocketMQTokioRwLock::new(5);
        {
            let mut write_guard = lock.write().await;
            *write_guard = 10;
        }
        assert_eq!(*lock.read().await, 10);
    }

    #[tokio::test]
    async fn try_read_locks_and_reads() {
        let lock = RocketMQTokioRwLock::new(5);
        let read_guard = lock.try_read().await;
        assert!(read_guard.is_some());
        assert_eq!(*read_guard.unwrap(), 5);
    }

    #[tokio::test]
    async fn try_write_locks_and_writes() {
        let lock = RocketMQTokioRwLock::new(5);
        {
            let write_guard = lock.try_write().await;
            assert!(write_guard.is_some());
            *write_guard.unwrap() = 10;
        }
        assert_eq!(*lock.read().await, 10);
    }

    #[tokio::test]
    async fn try_read_timeout_succeeds_within_timeout() {
        let lock = RocketMQTokioRwLock::new(5);
        let read_guard = lock.try_read_timeout(Duration::from_millis(100)).await;
        assert!(read_guard.is_some());
        assert_eq!(*read_guard.unwrap(), 5);
    }

    #[tokio::test]
    async fn try_read_timeout_fails_after_timeout() {
        let lock = Arc::new(RocketMQTokioRwLock::new(5));
        let arc = lock.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _read_guard = lock.write().await;
            tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(_read_guard);
        });
        rx.await.unwrap();
        let read_guard = arc.try_read_timeout(Duration::from_millis(2)).await;
        assert!(read_guard.is_none());
    }

    #[tokio::test]
    async fn try_write_timeout_succeeds_within_timeout() {
        let lock = RocketMQTokioRwLock::new(5);
        let write_guard = lock.try_write_timeout(Duration::from_millis(100)).await;
        assert!(write_guard.is_some());
        *write_guard.unwrap() = 10;
        assert_eq!(*lock.read().await, 10);
    }

    #[tokio::test]
    async fn try_write_timeout_fails_after_timeout() {
        let lock = Arc::new(RocketMQTokioRwLock::new(5));
        let arc = lock.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let write_guard = lock.read().await;
            tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(write_guard);
        });
        rx.await.unwrap();
        let write_guard = arc.try_write_timeout(Duration::from_millis(2)).await;
        assert!(write_guard.is_none());
    }

    #[tokio::test]
    async fn new_creates_mutex_instance() {
        let mutex = RocketMQTokioMutex::new(5);
        let guard = mutex.lock().await;
        assert_eq!(*guard, 5);
    }

    #[tokio::test]
    async fn lock_acquires_lock_and_allows_mutation() {
        let mutex = RocketMQTokioMutex::new(5);
        {
            let mut guard = mutex.lock().await;
            *guard = 10;
        }
        let guard = mutex.lock().await;
        assert_eq!(*guard, 10);
    }

    #[tokio::test]
    async fn try_lock_acquires_lock_if_available() {
        let mutex = RocketMQTokioMutex::new(5);
        let guard = mutex.try_lock().await;
        assert!(guard.is_some());
        assert_eq!(*guard.unwrap(), 5);
    }

    #[tokio::test]
    async fn try_lock_returns_none_if_unavailable() {
        let mutex = Arc::new(RocketMQTokioMutex::new(5));
        let arc = mutex.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _guard = arc.lock().await;
            tx.send(()).unwrap();
            // Hold the lock until the test completes
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rx.await.unwrap();
        let guard = mutex.try_lock().await;
        assert!(guard.is_none());
    }

    #[tokio::test]
    async fn try_lock_timeout_succeeds_within_timeout() {
        let mutex = RocketMQTokioMutex::new(5);
        let guard = mutex.try_lock_timeout(Duration::from_millis(100)).await;
        assert!(guard.is_some());
        assert_eq!(*guard.unwrap(), 5);
    }

    #[tokio::test]
    async fn try_lock_timeout_fails_after_timeout() {
        let mutex = Arc::new(RocketMQTokioMutex::new(5));
        let arc = mutex.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _guard = arc.lock().await;
            tx.send(()).unwrap();
            // Hold the lock for longer than the timeout
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rx.await.unwrap();
        let guard = mutex.try_lock_timeout(Duration::from_millis(2)).await;
        assert!(guard.is_none());
    }
}
