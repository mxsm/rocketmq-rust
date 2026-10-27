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

//! Provides GPUI's Unix archive xattr API through the maintained implementation.
//!
//! The legacy implementation refers to `libc::ENOATTR` on Linux, where recent libc releases no
//! longer provide that alias. The upstream implementation uses the platform's supported API.

#[cfg(unix)]
pub use xattr_upstream::*;
