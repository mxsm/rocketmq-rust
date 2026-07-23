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

use loom::sync::Arc;
use loom::sync::RwLock;
use loom::thread;

#[test]
fn guarded_replacement_serializes_concurrent_writers() {
    loom::model(|| {
        let state = Arc::new(RwLock::new(0_u8));
        let first = Arc::clone(&state);
        let second = Arc::clone(&state);

        let first_writer = thread::spawn(move || {
            *first.write().expect("first writer lock") += 1;
        });
        let second_writer = thread::spawn(move || {
            *second.write().expect("second writer lock") += 1;
        });

        first_writer.join().expect("first writer");
        second_writer.join().expect("second writer");
        assert_eq!(*state.read().expect("final reader lock"), 2);
    });
}

#[test]
fn replacement_releases_worker_owners_before_root_unwrap() {
    loom::model(|| {
        let owner = Arc::new(RwLock::new(7_u8));
        let worker_owner = Arc::clone(&owner);

        let reader = thread::spawn(move || {
            assert_eq!(*worker_owner.read().expect("reader lock"), 7);
        });

        reader.join().expect("reader");
        let state = Arc::try_unwrap(owner).expect("worker released its strong owner");
        assert_eq!(state.into_inner().expect("owned state"), 7);
    });
}
