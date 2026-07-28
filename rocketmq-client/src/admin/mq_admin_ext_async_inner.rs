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

/// Legacy concrete marker name retained as a zero-sized compatibility alias.
///
/// The client runtime no longer stores this value or constrains registration
/// through an empty marker trait.
#[deprecated(since = "1.1.0", note = "remove in 2.0.0; admin registration stores group presence")]
pub type MQAdminExtInnerImpl = ();
