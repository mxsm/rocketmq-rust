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

/// Service task types and operations.
pub mod service_task;

use std::pin::Pin;

pub use service_task::ServiceManager;

/// Allocates a box and then builds `make`'s value into it.
///
/// `Box::pin(make())` builds the value first, on the stack, and copies it into the allocation:
/// for a future that copy is its whole type size, mostly the uninitialized space of states it
/// has not reached. Allocating first gives the value a destination when it is built, so the
/// optimizer can build it in the allocation directly; for a large future that saves copying
/// kilobytes of unused state on every call.
#[inline]
pub fn box_in_place<T>(make: impl FnOnce() -> T) -> Pin<Box<T>> {
    let slot = Box::new_uninit();
    Box::into_pin(Box::write(slot, make()))
}
