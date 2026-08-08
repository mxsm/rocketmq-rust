// Copyright 2026 The RocketMQ Rust Authors
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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use arc_swap::ArcSwap;

use crate::runtime::RPCHook;

pub(crate) struct HookSnapshot {
    hooks: Box<[Arc<dyn RPCHook>]>,
    generation: u64,
}

impl HookSnapshot {
    pub(crate) fn hooks(&self) -> &[Arc<dyn RPCHook>] {
        &self.hooks
    }

    #[cfg(test)]
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }
}

pub(crate) struct HookRegistry {
    current: ArcSwap<HookSnapshot>,
    hook_count: AtomicUsize,
    update: Mutex<()>,
}

impl HookRegistry {
    pub(crate) fn new(hooks: Vec<Arc<dyn RPCHook>>) -> Self {
        let hook_count = hooks.len();
        Self {
            current: ArcSwap::from_pointee(HookSnapshot {
                hooks: hooks.into_boxed_slice(),
                generation: 1,
            }),
            hook_count: AtomicUsize::new(hook_count),
            update: Mutex::new(()),
        }
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> Option<Arc<HookSnapshot>> {
        if self.hook_count.load(Ordering::Acquire) == 0 {
            None
        } else {
            Some(self.current.load_full())
        }
    }

    pub(crate) fn register(&self, hook: Arc<dyn RPCHook>) {
        let _update = self.update.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = self.current.load_full();
        let mut hooks = previous.hooks.to_vec();
        hooks.push(hook);
        let next = Arc::new(HookSnapshot {
            hooks: hooks.into_boxed_slice(),
            generation: previous.generation.wrapping_add(1).max(1),
        });
        let hook_count = next.hooks.len();
        self.current.store(next);
        self.hook_count.store(hook_count, Ordering::Release);
    }

    pub(crate) fn clear(&self) {
        let _update = self.update.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = self.current.load_full();
        self.current.store(Arc::new(HookSnapshot {
            hooks: Box::new([]),
            generation: previous.generation.wrapping_add(1).max(1),
        }));
        self.hook_count.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use rocketmq_error::RocketMQResult;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;

    use super::*;

    struct NoopHook;

    impl RPCHook for NoopHook {
        fn do_before_request(&self, _remote_addr: SocketAddr, _request: &mut RemotingCommand) -> RocketMQResult<()> {
            Ok(())
        }

        fn do_after_response(
            &self,
            _remote_addr: SocketAddr,
            _request: &RemotingCommand,
            _response: &mut RemotingCommand,
        ) -> RocketMQResult<()> {
            Ok(())
        }
    }

    #[test]
    fn retained_snapshot_keeps_one_generation() {
        let registry = HookRegistry::new(vec![Arc::new(NoopHook)]);
        let first = registry.snapshot().expect("initial hook snapshot");
        registry.register(Arc::new(NoopHook));
        let second = registry.snapshot().expect("updated hook snapshot");
        registry.clear();
        let cleared = registry.snapshot();

        assert_eq!(first.hooks().len(), 1);
        assert_eq!(second.hooks().len(), 2);
        assert_ne!(first.generation(), second.generation());
        assert!(cleared.is_none());
    }
}
