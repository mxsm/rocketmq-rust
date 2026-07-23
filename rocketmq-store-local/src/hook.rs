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

use std::sync::Arc;

use parking_lot::RwLock;

/// Runtime-neutral ordered ownership for store hook adapters.
pub struct HookRegistry<T: ?Sized> {
    hooks: Arc<RwLock<Vec<Arc<T>>>>,
}

impl<T: ?Sized> Clone for HookRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            hooks: Arc::clone(&self.hooks),
        }
    }
}

impl<T: ?Sized> Default for HookRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ?Sized> HookRegistry<T> {
    pub fn new() -> Self {
        Self {
            hooks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn push(&self, hook: Arc<T>) {
        self.hooks.write().push(hook);
    }

    pub fn snapshot(&self) -> Vec<Arc<T>> {
        self.hooks.read().clone()
    }

    pub fn len(&self) -> usize {
        self.hooks.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait TestHook: Send + Sync {
        fn value(&self) -> u32;
    }

    struct Hook(u32);

    impl TestHook for Hook {
        fn value(&self) -> u32 {
            self.0
        }
    }

    #[test]
    fn preserves_registration_order_in_snapshots() {
        let hooks: HookRegistry<dyn TestHook> = HookRegistry::new();
        hooks.push(Arc::new(Hook(1)));
        hooks.push(Arc::new(Hook(2)));

        let snapshot = hooks.snapshot();
        assert_eq!(snapshot.iter().map(|hook| hook.value()).collect::<Vec<_>>(), [1, 2]);

        let cloned = hooks.clone();
        cloned.push(Arc::new(Hook(3)));
        assert_eq!(
            hooks.snapshot().iter().map(|hook| hook.value()).collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }
}
