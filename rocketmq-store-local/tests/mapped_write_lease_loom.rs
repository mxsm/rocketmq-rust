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

use loom::sync::atomic::AtomicUsize;
use loom::sync::atomic::Ordering;
use loom::sync::Arc;
use loom::sync::Mutex;
use loom::thread;

#[test]
fn position_publication_never_exposes_partially_copied_bytes() {
    loom::model(|| {
        let bytes = Arc::new(Mutex::new([0_u8; 2]));
        let published = Arc::new(AtomicUsize::new(0));

        let writer_bytes = Arc::clone(&bytes);
        let writer_published = Arc::clone(&published);
        let writer = thread::spawn(move || {
            let mut bytes = writer_bytes.lock().expect("writer lock");
            bytes.copy_from_slice(b"ok");
            writer_published.store(2, Ordering::Release);
        });

        let reader_bytes = Arc::clone(&bytes);
        let reader_published = Arc::clone(&published);
        let reader = thread::spawn(move || {
            if reader_published.load(Ordering::Acquire) == 2 {
                assert_eq!(*reader_bytes.lock().expect("reader lock"), *b"ok");
            }
        });

        writer.join().expect("writer");
        reader.join().expect("reader");
    });
}
