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

use std::ops::Deref;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangeError {
    Overflow,
    OutOfBounds,
}

fn checked_range(mapping_len: usize, offset: usize, len: usize) -> Result<Range<usize>, RangeError> {
    let end = offset.checked_add(len).ok_or(RangeError::Overflow)?;
    if end > mapping_len {
        return Err(RangeError::OutOfBounds);
    }
    Ok(offset..end)
}

struct GenerationOwner {
    bytes: Box<[u8]>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for GenerationOwner {
    fn drop(&mut self) {
        self.events.lock().expect("event lock").push("generation");
    }
}

struct OperationLease {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Drop for OperationLease {
    fn drop(&mut self) {
        self.events.lock().expect("event lock").push("operation");
    }
}

struct AliasInner {
    generation: Option<Arc<GenerationOwner>>,
    operation: Option<OperationLease>,
}

impl Drop for AliasInner {
    fn drop(&mut self) {
        drop(self.generation.take());
        drop(self.operation.take());
    }
}

#[derive(Clone)]
struct ReadAlias {
    inner: Arc<AliasInner>,
    range: Range<usize>,
}

impl ReadAlias {
    fn try_new(
        generation: Arc<GenerationOwner>,
        operation: OperationLease,
        offset: usize,
        len: usize,
    ) -> Result<Self, RangeError> {
        let range = checked_range(generation.bytes.len(), offset, len)?;
        Ok(Self {
            inner: Arc::new(AliasInner {
                generation: Some(generation),
                operation: Some(operation),
            }),
            range,
        })
    }

    fn split_at(&self, mid: usize) -> Result<(Self, Self), RangeError> {
        let absolute_mid = self.range.start.checked_add(mid).ok_or(RangeError::Overflow)?;
        if absolute_mid > self.range.end {
            return Err(RangeError::OutOfBounds);
        }
        Ok((
            Self {
                inner: Arc::clone(&self.inner),
                range: self.range.start..absolute_mid,
            },
            Self {
                inner: Arc::clone(&self.inner),
                range: absolute_mid..self.range.end,
            },
        ))
    }
}

impl Deref for ReadAlias {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let generation = self
            .inner
            .generation
            .as_ref()
            .expect("generation remains present until final alias drop");
        &generation.bytes[self.range.clone()]
    }
}

#[test]
fn detached_slot_keeps_old_generation_readable_until_final_alias() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let generation = Arc::new(GenerationOwner {
        bytes: Box::from(&b"generation-owned"[..]),
        events: Arc::clone(&events),
    });
    let mut slot = Some(Arc::clone(&generation));
    let alias = ReadAlias::try_new(
        Arc::clone(&generation),
        OperationLease {
            events: Arc::clone(&events),
        },
        0,
        generation.bytes.len(),
    )
    .expect("complete checked range");
    drop(generation);

    let detached = slot.take().expect("one detach winner");
    assert!(slot.take().is_none());
    drop(detached);
    assert_eq!(&*alias, b"generation-owned");
    assert!(events.lock().expect("event lock").is_empty());

    let clone = alias.clone();
    let (left, right) = alias.split_at(10).expect("checked split");
    drop(alias);
    drop(clone);
    drop(left);
    assert_eq!(&*right, b"-owned");
    assert!(events.lock().expect("event lock").is_empty());
    drop(right);

    assert_eq!(&*events.lock().expect("event lock"), &["generation", "operation"]);
}

#[test]
fn checked_alias_ranges_reject_overflow_and_out_of_bounds() {
    assert_eq!(checked_range(8, usize::MAX, 1), Err(RangeError::Overflow));
    assert_eq!(checked_range(8, 8, 1), Err(RangeError::OutOfBounds));
    assert_eq!(checked_range(8, 8, 0), Ok(8..8));
}
