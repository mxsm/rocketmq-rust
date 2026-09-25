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

// Normalize before adding lifecycle/select/timeout wrappers. This bounds
// inline state, not arbitrary user poll frames or recursive user destructors.
//
// Unoptimized builds give every by-value move its own stack slot, so each
// wrapper and forwarding call below this boundary keeps another copy of an
// inline future and submission can take many times the future's size. Such
// builds use Tokio's debug boxing boundary instead.
pub(crate) const MAX_INLINE_SIZE: usize = if cfg!(debug_assertions) { 2 * 1024 } else { 16 * 1024 };
