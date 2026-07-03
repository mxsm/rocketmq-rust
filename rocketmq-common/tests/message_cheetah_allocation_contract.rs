/*
 * Copyright 2023 The RocketMQ Rust Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::hint::black_box;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::broker_message::BrokerMessage;
use rocketmq_common::common::message::message_envelope::MessageEnvelope;
use rocketmq_common::common::message::MessageTrait;

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: The allocator delegates all memory operations to the standard
// allocator and only records allocation counts for this integration test crate.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The call is delegated unchanged to the standard allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout were provided by the global allocation API.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn allocations_for<R>(operation: impl FnOnce() -> R) -> usize {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    let result = operation();
    black_box(&result);
    ALLOCATIONS.load(Ordering::Relaxed)
}

fn broker_message_with_properties() -> BrokerMessage {
    let mut envelope = MessageEnvelope::default();
    envelope.put_property(
        CheetahString::from_static_str("KEYS"),
        CheetahString::from_static_str("order-1001"),
    );
    envelope.put_property(
        CheetahString::from_static_str("TAGS"),
        CheetahString::from_static_str("paid"),
    );

    BrokerMessage::from_envelope(envelope)
}

#[test]
fn broker_message_property_lookup_allocation_baseline_is_bounded() {
    let broker_message = broker_message_with_properties();

    let existing_key_allocations = allocations_for(|| broker_message.property(black_box("KEYS")));
    let missing_key_allocations = allocations_for(|| broker_message.property(black_box("NOT_EXISTS")));

    assert_eq!(
        existing_key_allocations, 0,
        "existing key lookup allocated {existing_key_allocations} times"
    );
    assert_eq!(
        missing_key_allocations, 0,
        "missing key lookup allocated {missing_key_allocations} times"
    );
}
