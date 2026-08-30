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

#[path = "authorized_dispatcher/boundary_identity.rs"]
mod boundary_identity;
#[path = "authorized_dispatcher/delivery_hooks.rs"]
mod delivery_hooks;
#[path = "authorized_dispatcher/harness.rs"]
mod harness;
#[path = "authorized_dispatcher/outcomes_deadlines.rs"]
mod outcomes_deadlines;
